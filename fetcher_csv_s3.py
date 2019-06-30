#!/usr/bin/env python3

import sqlite3, sys, os, time, tarfile, boto3
from threading import Thread
from queue import Queue

def s3get(client, inputBucket, q, resultQ):
    while True:
        (filename, offset, end) = q.get()
        obj = client.get_object(Bucket=inputBucket, Key=filename, Range='bytes={}-{}'.format(offset, end))
        line=obj['Body'].read()
        resultQ.put(line)
        q.task_done()

def resPrint(outFile, resultQ, amount):
    counter = 0 
    while True:
        line=resultQ.get()
        outFile.write(line)
        counter += 1
        if counter % 1000 == 0:
            print("\rProgress: [{}/{}]".format(counter, amount), file=sys.stderr, end='')
        resultQ.task_done()

def deliverFiles(ids, dbFile, inputBucket, outFile):
    i = 0
    missedFiles = 0
    client = boto3.client('s3', endpoint_url = 'https://s3.wasabisys.com')
    
    resultQ = Queue(maxsize=0)
    outThread = Thread(target=resPrint, args=(outFile, resultQ, len(ids)), daemon = True)
    outThread.start()

    q = Queue(maxsize=0)
    num_threads = 32
    for i in range(num_threads):
        worker = Thread(target=s3get, args=(client, inputBucket, q, resultQ), daemon = True)
        worker.start()


    missedIds = 0
    with sqlite3.connect(dbFile) as conn:
        cursor = conn.cursor()
        for sid in ids:
            cursor.execute("SELECT sid,pos,size,filename FROM data JOIN filenames ON filenames.id = data.filename_id WHERE sid = ?", (sid,))
            row = cursor.fetchone()
            if row:
                sid, pos, size, filename = row
                q.put ((filename, pos, pos+size-1))
            else:
                missedIds += 1
    print("\nMissed Ids count: {}".format(missedIds))

    q.join()
    resultQ.join()


def check_ids(ids):
    result = []
    for item in ids.split():
        try:
            result.append(int(item))
        except:
            print("Ignoring {}".format(item), file=sys.stderr)
    return result

    return myDir

if __name__ == '__main__':
    import argparse
    currentDir = os.getcwd()
    parser = argparse.ArgumentParser(description="CSV data downloader", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-f", "--input-file", dest='inputFile', type=argparse.FileType('r'), metavar='FILE',
                        help="input file with space or newline separated IDs. For stdin use -")
    parser.add_argument("-i", "--ids", dest='ids', type=check_ids, metavar='LIST',
                        help="List of IDs, --ids '399 400 5476276'")
    parser.add_argument("--db", dest='dbFile',  
                        metavar='DBFILE', required=True,
                        help="SQLite database file to read index from.")
    parser.add_argument("-r", "--readfrombucket", dest='inputBucket',  
                        metavar='DIR', required=True,
                        help='Directory to search input SDF files')     
    parser.add_argument("-o", "--output-file", dest='outFile', type=argparse.FileType('wb'), 
                        metavar='FILE', required=True,
                        help='Output file name, like result.csv. For stdout use -') 

    args = parser.parse_args()

    ids = []
    if args.inputFile:
        ids.extend( check_ids( args.inputFile.read() ) )
        args.inputFile.close()
    if args.ids:
        ids.extend(args.ids)
    if ids:
        ids.sort()
        startTime = time.time()
        print("Fetching {} molecules to {}".format(len(ids), args.outFile.name), file=sys.stderr)
        deliverFiles(ids = ids, dbFile = args.dbFile, inputBucket =args.inputBucket, outFile = args.outFile)
        print( "\nExecution completed. Work time {:.2f} sec".format(time.time()-startTime), file=sys.stderr )
    else:
        parser.error ('Either --input-file or --ids is required.')
