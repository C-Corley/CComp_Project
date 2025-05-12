#!/usr/bin/env python3

import os
import json
import orjson
import pandas as pd
from xopen import xopen
import concurrent.futures
from multiprocessing import Manager, Pool

def combine(args):
    file, lock  = args
    output_dir = "/scratch/ptolemy/users/cc3886/project_cc/combined.ndjson"
    path ="/scratch/ptolemy/users/cc3886/project_cc/Sample2015"


    decoder = json.JSONDecoder()
    buffer = ''
    batch = []

    with xopen(os.path.join(path, file), 'rt', encoding='utf-8') as infile:
        for line in infile:
            buffer += line

            while True:
                buffer = buffer.lstrip()
                if not buffer:
                    break

                try:
                    obj, index = decoder.raw_decode(buffer)
                    batch.append(obj)
                    buffer = buffer[index:]
                except json.JSONDecodeError:
                    break

                if len(batch) >= 250:
                    with lock:
                        with open(output_dir, 'ab') as outfile:
                            for item in batch:
                                outfile.write(orjson.dumps(item))
                                outfile.write(b'\n')
                    batch.clear()

        if batch:
            with lock:
                with open(output_dir, 'ab') as outfile:
                    for item in batch:
                        outfile.write(orjson.dumps(item))
                        outfile.write(b'\n')
            batch.clear()


if __name__ ==  '__main__':

    path = "/scratch/ptolemy/users/cc3886/project_cc/Sample2015"
    yearList = os.listdir(path)




    with Manager() as manager:
        lock = manager.Lock()
        header_written = manager.Value('b', False)

        args = [(year, lock) for year in yearList[:-1]]

        with Pool() as p:
            p.map(combine, args)


