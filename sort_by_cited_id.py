#!/usr/bin/python2.7
import string, sys, os, itertools

if __name__ == '__main__':    
    f = open(sys.argv[1]).readlines()
    print "done reading"
    f.sort(key=lambda x:x.split('\t')[2])
    print "done sorting"
    outfile = open(sys.argv[1].strip('.txt')+"sorted.txt",'w')
    for line in f:
        outfile.write(line+'\n')
    outfile.close()
    

