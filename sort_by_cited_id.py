#!/usr/bin/python2.7
import string, sys, os, itertools

if __name__ == '__main__':
    
    f = open('/home/cail/deduped-context-and-matches.txt').readlines()
    print "done reading"
    f.sort(key=lambda x:x.split('\t')[14])
    print "done sorting"
    outfile = open('contexts-and-matches_sorted_deduped.txt','w')
    for line in f:
        tokens = line.split('\t')
        outfile.write(tokens[14]+'\t'+"".join(tokens[4:7])+'\n')
    outfile.close()
    
    

