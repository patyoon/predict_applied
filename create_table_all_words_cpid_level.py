#!/usr/bin/python2.7

import string, sys, os
from optparse import OptionParser
from collections import Counter
from MySQLdb import connect, escape_string

JOURNAL_TABLE_NAME = 'journal'

if __name__ == '__main__':

    usage = ("usage: %prog [options] [input_text_file_name][table_name]"
             )
    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))

    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    if len(args) != 3:
        print ("need four arguments :  [input_text_file_name][table_name]"
               "[feature_type]")
        sys.exit(1)

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()
    #assert args[3] in ['jnl', ' 
    if args[2] == 'title':
        idx = 5
    else:
        idx = 6

    create_statement = "CREATE TABLE IF NOT EXISTS " + args[1]+" (cpid char(255), word char(50), count INT) "
    print create_statement
    cursor.execute(create_statement)
        
    i = 0
    j = 0
    counter = Counter()
    with open(args[0], 'rt') as f:
        for line in f:
            if line == '\n':
                continue
            line = line.translate(TR) # Strip punctuation
            i+=1
            if i % 100000 == 0:
                print "processed ", i, "th line ", cpid
            #print line.split('\t')
            cpid, sentences = (line.split('\t')[0].strip(), line.split('\t')[idx].strip())
            #cpid = get_cpid(cursor, cpid)
            counter = Counter()
            for word in sentences.split():
                word = word.lower()
                if len(word) > 50:
                    print "length greater than 50 %s" %word
                    continue
                if word.isalpha():
                    counter[word] += 1
            if len(counter) > 0:
                for x in map(lambda (x,y,): ("'"+str(cpid)+"'", "'"+str(x)+"'", 
                                             str(y)), counter.items()):
                    values = ', '.join(x)
                    string = 'INSERT INTO ' + args[1] + ' VALUES('+values+')'
                    cursor.execute(string)
                    j += cursor.rowcount
                    if j % 1000000 == 0:
                        print "wrote ", j, "th record ", cpid
    print "num entry created: ", j
