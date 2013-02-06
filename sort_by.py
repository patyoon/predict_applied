#!/usr/bin/python2.7
import string, sys, os, itertools

def normalize_whitespace(str):
    import re
    str = str.strip()
    str = re.sub(r'\s+', ' ', str)
    return str

if __name__ == '__main__':    
    if len(sys.argv) !=3:
        sys.exit(1)
    f = open(sys.argv[1]).readlines()
    print "done reading"
    f.sort(key=lambda x:normalize_whitespace(x.split('\t')[int(sys.argv[2])]))
    print "done sorting"
    outfile = open(sys.argv[1].strip('.txt')+"sorted.txt",'w')
    for line in f:
        outfile.write(line+'\n')
    outfile.close()
    

