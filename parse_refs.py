#!/usr/bin/python2.6
import re

class Parser:
    
    def __init__(self, filename):
        self.infile = open(filename)
        self.bpidMap = {}
    def yield_tuple(self):
        for line in self.infile:
            tokens = line.split('\t')
            tokens = map(lambda x:x.strip(), tokens)
            sentences = "".join(tokens[4:7])
            if tokens[14] in self.bpidMap:
                self.bpidMap[tokens[14]] +=1
            else:
                self.bpidMap[tokens[14]] = 0
                yield ('\N', tokens[0], tokens[8], tokens[9], '\N', tokens[11], tokens[12], tokens[10], sentences[0:255])

