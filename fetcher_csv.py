#!/usr/bin/env python3

from __future__ import print_function
import sqlite3, sys, os, time, tarfile, lz4.frame

def deliverFiles(ids, dbFile, inputDir, outFile):
    i = 0
    missedFiles = 0
    mtime = time.time()
    with sqlite3.connect(dbFile) as conn:
        try:
            cursor = conn.cursor()
            for sid in ids:
                cursor.execute("SELECT sid,pos,size,filename FROM data JOIN filenames ON filenames.id = data.filename_id WHERE sid = ?", (sid,))
                row = cursor.fetchone()
                if row:
                    sid, pos, size, filename = row

                    if filename.endswith('.lz4'):
                        opener = lz4.frame.open
                    else:
                        opener = open 

                    try:
                        if os.path.basename(f.name) != filename:
                            f.close()
                            f = opener(inputDir + '/' + filename, "rb")
                    except:
                        f = opener(inputDir + '/' + filename, "rb")
                    f.seek(pos)
                    data = f.read(size)
                    #data = data[:size]
                    outFile.write(data)

                else:
                    missedFiles+=1
                i+=1
                if i % 100 == 0:
                        print("\rProgress: [{}/{}]. Missed ids count: {}".format(i, len(ids), missedFiles), file=sys.stderr, end='')
                        sys.stderr.flush()
        finally:
            try:
                f.close()
            except:
                pass

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
    parser.add_argument("-r", "--readfromdir", dest='inputDir',  
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
        deliverFiles(ids = ids, dbFile = args.dbFile, inputDir =args.inputDir, outFile = args.outFile)
        print( "\nExecution completed. Work time {:.2f} sec".format(time.time()-startTime), file=sys.stderr )
    else:
        parser.error ('Either --input-file or --ids is required.')
