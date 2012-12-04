#!/usr/bin/python2.6
from optparse import OptionParser
import sys, os, re
from subprocess import Popen, PIPE

if __name__ == "__main__":

    usage = """"usage: %prog [options] [term] [input_path] [output_path]

Extracts 100 random citances from dataset.
"""

    parser = OptionParser(usage)

    parser.add_option("-n", "--number", dest="num", action = "store", default=100, help="Number of random contexts to extract")

    (options, args) = parser.parse_args()

    num =  options.num 

    if len(args)!=3:
        print "Incorrect number of argument"
        sys.exit(1)
        
    term = args[0]
    in_file = args[1]
    output_file = open(args[2], 'w')

    p1 = Popen(['grep', '-e', term, in_file], stdout=PIPE)
    p2 = Popen(['uniq'], stdin=PIPE, stdout=PIPE)
    p3 = Popen(['sort', '--random-sort'], stdin=PIPE, stdout=PIPE)
    p4 = Popen(['head', '-n', str(num)], stdin=PIPE)
    final_output = p4.stdout.read()
    p4.wait()
    lines = final_output.split("\n")
    i = 1
    for line in lines:
        output_file.write(str(i) + ' '+ line+'\n')
    output_file.close()
