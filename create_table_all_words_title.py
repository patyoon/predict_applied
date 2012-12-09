#!/usr/bin/python2.7

import string, sys, os
from optparse import OptionParser
from collections import Counter
from MySQLdb import connect, escape_string

JOURNAL_TABLE_NAME = 'journal'

def normalize_whitespace(str):
    import re
    str = str.strip()
    str = re.sub(r'\s+', ' ', str)
    return str

def get_jid(cursor, jnl):
    cursor.execute('SELECT jid FROM '
                   + JOURNAL_TABLE_NAME +
                   ' where jnl="'+jnl+'"')
    sql_result = cursor.fetchall()
    print len(sql_result), jnl
    assert len(sql_result) == 1
    return sql_result[0][0]

if __name__ == '__main__':

    usage = ("usage: %prog [options] [input_text_file_name][table_name]"
             " "
             "'cited_paper_terms_count_sample_4m'"
             )
    # STOP WORD is not used
    # STOP_WORDS = set([
    #         'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 
    #         'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
    #         ])

    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))

    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    if len(args) != 2:
        print "need two arguments :  [input_text_file_name][table_name]"
        sys.exit(1)

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

    create_statement = "CREATE TABLE IF NOT EXISTS " + args[1]+" (jnl char(255), word char(50), count INT) "
    print create_statement
    cursor.execute(create_statement)
    
    curr_jnl = ""
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
                print "processed ", i, "th line ", curr_jnl
            #print line.split('\t')
            jnl, sentences = (normalize_whitespace(line.split('\t')[2].lower().strip()),
                              line.split('\t')[5].strip())
            #jnl = get_jnl(cursor, jnl)
            if jnl != curr_jnl:
                if len(counter) > 0:
                    for x in map(lambda (x,y,): ("'"+curr_jnl
                                                 +"'", "'"+str(x)+"'", 
                                                 str(y)), counter.items()):
                        values = ', '.join(x)
                        string = 'INSERT INTO ' + args[1] + ' VALUES('+values+')'
                        cursor.execute(string)
                        j += cursor.rowcount
                        if j % 1000000 == 0:
                            print "wrote ", j, "th record ", curr_jnl
                curr_jnl = jnl
                counter = Counter()
            for word in sentences.split():
                word = word.lower()
                if len(word) > 50:
                    print "length greater than 50 %s" %word
                    continue
                if word.isalpha():
                    counter[word] += 1
    print "num entry created: ", j
