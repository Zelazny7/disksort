import csv
import queue
import tempfile
import glob
import contextlib
from collections import deque


def iter_chunks(reader, max_buffer_size=100):
    buffer = deque()
    for line in reader:
        if len(buffer) >= max_buffer_size:
            yield buffer
            # buffer.clear()
        buffer.append(line)
    yield buffer

with open('data/titanic-small.csv', 'r') as fin:
    next(fin) # skip header
    reader = csv.reader(fin)

    parts = []

    # fill the minheap
    q = queue.PriorityQueue()
    with tempfile.TemporaryDirectory() as temp_dir:
        for i, chunk in enumerate(iter_chunks(reader, 100000)):
            # fill the min-heap
            for line in chunk:
                q.put((float(line[9]), '|'.join(line) + '\n'))
            
            # empty the min-heap into the output files
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
                readers.append(iter_chunks(reader, 100))
            
            ## read first N rows of each file
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

                # get index of list with smallest item
                _, i = q.get()
                deq: deque = chunks[i]
                row = deq.popleft()
                
                # add row to output buffer
                output_buffer.append('|'.join(row) + '\n')
                
                # if list is empty read in another N records
                if len(deq) == 0:
                    try:
                        chunks[i] = next(readers[i])
                    except:
                        continue
                
                # add next element from chunk to the queue
                q.put((float(chunks[i][0][9]), i))
            
            fout.writelines(output_buffer)