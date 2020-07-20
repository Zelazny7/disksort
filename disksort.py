import csv
import queue
import tempfile
import glob
import contextlib
from typing import Tuple


class QueueItem():
    def __init__(self, priority, payload):
        self.priority = priority
        self.payload = payload
    
    def __lt__(self, other):
        return self.priority <= other.priority
    
    def __iter__(self):
        return iter((self.priority, self.payload))


def yield_100_rows(reader, max_buffer_size=100):
    buffer = []
    for line in reader:
        if len(buffer) >= max_buffer_size:
            yield buffer
            buffer.clear()
        buffer.append(line)
    yield buffer

with open('data/titanic-big.csv', 'r') as fin:
    next(fin) # skip header
    reader = csv.reader(fin)

    parts = []

    # fill the minheap
    q = queue.PriorityQueue()
    with tempfile.TemporaryDirectory() as temp_dir:
        for i, chunk in enumerate(yield_100_rows(reader, 100000)):
            # fill the heap
            for line in chunk:
                q.put((float(line[9]), '|'.join(line) + '\n'))
            
            with open(f'{temp_dir}/part{i:04d}.dat', 'a') as fout:
                while not q.empty():
                    fout.write(q.get()[1])

        ## open parts of each file here
        parts = glob.glob(temp_dir + "/*")

        import os
        if os.path.exists('sorted-output'):
            os.remove('sorted-output')
        # create csv readers for all file parts
        # use contextlib.ExitStack to ensure they are all closed automatically
        # https://stackoverflow.com/a/4617069/919872
        with contextlib.ExitStack() as stack, open('sorted-output', 'a') as fout:
            readers = []
            for part in parts:
                reader = csv.reader(stack.enter_context(open(part)), delimiter="|")
                readers.append(yield_100_rows(reader, 10000))
            
            ## do everything in here
            ## read first 10 rows of each file
            # chunks = [next(reader) for reader in readers]
            chunks = {}
            for i, reader in enumerate(readers):
                try:
                    chunks[i] = next(reader)
                    # load queue with tuples of sort value, and index of reader
                    q.put((float(chunks[i][0][9]), i))
                except:
                    continue

            
            ## take from the top
            output_buffer = []
            while not q.empty():
                if len(output_buffer) >= 10000:
                    fout.writelines(output_buffer)
                    output_buffer.clear()

                
                priority, i = q.get()
                l = chunks[i]
                
                row = l.pop(0)
                # add row to output buffer
                output_buffer.append('|'.join(row) + '\n')
                
                # if list is empty read in another N records
                if len(l) == 0:
                    try:
                        chunks[i] = next(readers[i])
                    except:
                        continue
                
                # add next element from chunk to the queue
                q.put((float(chunks[i][0][9]), i))
            
            fout.writelines(output_buffer)