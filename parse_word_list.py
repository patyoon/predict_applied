#!/usr/bin/python2.6                                                          
import os, sys
from optparse import OptionParser

class WordListParser:

    def __init__(self, input_file):
        self.input_file = open(input_file, 'r')

    def yield_tuple(self):
        i = 0
        for line in self.input_file:
            if i == 0:
                i+=1
                continue
            tokens = line.split("\t")
            tokens = map(lambda x:x.strip(), tokens)
            yield tokens

if __name__ == "__main__":

    usage = "usage: %prog [options] [in_file] [out_file]"
    parser = OptionParser(usage)

    (options, args) = parser.parse_args()

    if len(args)!=1:
        print "Incorrect number of argument"
        sys.exit(1)

    in_file = open(sys.argv[1])
    i = 1
    for line in in_file:
        tokens = line.split('\t')
        map(lambda x:x.strip("\n"), tokens)
        print "-----------".join(tokens)
