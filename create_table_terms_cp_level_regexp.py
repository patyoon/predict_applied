#!/usr/bin/python2.7
 
import string, sys, os, re
from optparse import OptionParser
from collections import Counter
from MySQLdb import connect, escape_string

if __name__ == '__main__':

    usage = ("usage: %prog [options] [input_text_file_name][table_name] [term_table_name]"
             " "
             "'cited_paper_terms_count_sample_4m'"
             )

    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    if len(args) != 3:
        print "need three arguments :  [input_text_file_name][table_name] [term_table_name]"
        sys.exit(1)

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()
    
    cursor.execute ("SELECT DISTINCT term FROM "+ args[2])

    if cursor.rowcount == 0:
        print "No terms retrived"
        sys.exit(1)

    termList = map(lambda x: x[0].strip(), cursor.fetchall())
    print "number of terms used ", len(termList)
    print termList
    termREs = dict((term[0], re.compile(r'\b(%s)\b' % 
                                        term.replace('*', '\w*'), re.I)) 
                   for term in termList)
    
    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))


    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

    create_statement = "CREATE TABLE IF NOT EXISTS " + args[1]+" (cited_id BIGINT, term char(50), count INT) "
    print create_statement
    cursor.execute(create_statement)
    
    curr_cited_id = 0
    i = 0
    j = 0
    counter = Counter()
    with open(args[0], 'rt') as f:
        for line in f:
            line = line.translate(TR) # Strip punctuation
            i+=1
            if i % 100000 == 0:
                print "processed ", i, "th line"
                #print i
            cited_id, sentences = line.split('\t')
            if float(cited_id) != curr_cited_id:
                if len(counter) > 0:
                    for x in map(lambda (x,y,): (str(curr_cited_id), "'"+escape_string(str(x))+"'", 
                                                 str(y)), counter.items()):
                        values = ', '.join(x)
                        string = 'INSERT INTO ' + args[1] + ' VALUES('+values+')'
                        #print string
                        cursor.execute(string)
                        j += cursor.rowcount
                        if j % 1000000 == 0:
                            print "wrote ", j, "th record"
                curr_cited_id = float(cited_id)
                counter = Counter()

            for term in termList:
                num_matches = len(termREs[term[0]].findall(sentences))
                if num_matches > 0 and cited_id.isdigit():
                    #counter uses term_id not original term here.
                    counter[term] += num_matches
    print "num entry created: ", j
