#!/usr/bin/env python3

import os, subprocess, sqlite3

dirs=[
    "/home/ubuntu/Desc1"
]

conn = sqlite3.connect("/home/ubuntu/Desc1/index.sqlite")

with conn:
    cur = conn.cursor()
    files = []
    for thisdir in dirs:
        filesList = os.listdir(thisdir)
        filesList = [x for x in filesList if x.endswith('.desc')]
        names={}
        for fromname in filesList:
            filename=thisdir+"/"+fromname
            with open(filename, 'r') as f: 
                f.readline()
                firstid = int(f.readline().split(',')[0][1:-1])
                lastline = subprocess.check_output(['tail', '-1', filename]).decode('ascii')
                lastid = int(lastline.split(',')[0][1:-1])
                if (lastid-firstid>25000) :
                    print("SOMETHING WRONG WITH {}: {}-{}".format(filename, firstid, lastid))
                    #exit(1)

                newname=int(firstid/25000)*25000
                if (newname+25000)<lastid:
                    print("SOMETHING WRONG WITH {}: {}-{}".format(filename, firstid, lastid))
                    #exit(1)
                if newname in names:
                    print('Duplicating name ', newname, filename, names[newname])
                    #compare_csvs(filename, names[newname])
                    #exit(1)
                names[newname] = filename
                onlyname = "{}_{}.desc".format(thisdir[-1], newname)
                newname= os.path.dirname(filename) + "/" + onlyname

                if os.path.isfile(newname):
                    print("file {} exists".format(newname))

                print("moving {} to {}".format(filename, newname))
                print ("update filenames set filename = {} where filename={}".format(onlyname, fromname))
                cur.execute("update filenames set filename = ? where filename=?", (onlyname, fromname))
                os.rename(filename,newname)
    #print(names)


