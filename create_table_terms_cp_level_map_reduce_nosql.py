#!/usr/bin/python2.7
import multiprocessing
import string, sys, os
from optparse import OptionParser
from collections import Counter, defaultdict
import multiprocessing
from multiprocessing_mapreduce import SimpleMapReduce
from MySQLdb import connect
import re

def file_to_words(filename):
    """Read a citance and return a sequence of (word, occurances) values.
    """
    
    term_list_file = open('term_list')

    termList = map(lambda x:x.strip().split('\t'), term_list_file.readlines())

    # Build a dictionary of term and compiled RE
    termREs = dict((term[0], re.compile(r'\b(%s)\b' % 
                                             term[0].replace('*', '\w*'), re.I)) 
                        for term in termList)

    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
    output = []

    cited_id_map = defaultdict(Counter)

    i = 0
    with open(filename, 'rt') as f:
        for line in f:
            i+=1
            if i%100000 == 0:
                print multiprocessing.current_process().name, " reached ", i
            line = line.translate(TR) # Strip punctuation
            counter = Counter()
            tokens = line.split('\t')
            sentences = "".join(tokens[4:7])
            cited_id = tokens[14]
            for term in termList:
                num_matches = len(termREs[term[0]].findall(sentences))
                if num_matches > 0 and cited_id.isdigit():
                    #counter uses term_id not original term here.
                    counter[term[1]] += num_matches
            #print multiprocessing.current_process().name, " ", counter
            cited_id_map[cited_id] += counter
            #output.append((cited_id, counter,))

    return cited_id_map.items()


def count_words(item):
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """
    cited_id, counters = item
    return (cited_id, reduce(lambda x, y: x+y, counters))

def divide_files(infilename, outfilename, num_division, filesize):
    
    f = open(infilename)
    chunk_size = filesize / num_division
    i = 0
    j = 0
    file_list = list()
    while j < num_division:
        outfile_name = outfilename + '_' + str(j)
        outfile = open(outfile_name, 'w')
        while (j == num_division- 1 and i < filesize) or i < (j+1) * chunk_size:
            outfile.write(f.readline())
            i+=1
        print 'Processed file: ', outfile_name
        file_list.append(outfile_name)
        j+=1
    return file_list

if __name__ == '__main__':

    usage = ("[input_text_file_name][table_name][file_size][term_table_name]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

    if len(args) != 4:
        print "need four arguments : [input_text_file_name][table_name][file_size][term_table_name]"
        sys.exit(1)
    #write term list file
    #For now, use term2
    if not os.path.exists('term_list'):
        cursor.execute ("SELECT DISTINCT word, Id FROM "+ term_table_name)
        if cursor.rowcount == 0:
            print "No terms retrived"
            sys.exit(1)
        
        #term list has tuple of (term, term_id)
        termList = cursor.fetchall()
        outfile = open('term_list', 'w')
        for (term, term_id) in termList:
            outfile.write(term+"\t"+str(term_id)+"\n")
        print "Number of terms used: %d" % len(termList)
        outfile.close()
    print "term_list already exists"

    #keep 8 mappers
    mapper = SimpleMapReduce(file_to_words, count_words, 8)
    outfile_name = os.path.basename(args[0])
    print outfile_name
    if not os.path.exists(outfile_name+'_0'):
        #inputs = divide_files('/home/mpatek/all_files_output/contexts-and-matches.txt', 8, 13571478)
        inputs = divide_files(args[0], outfile_name, 8, args[2])
    else:
        inputs = map(lambda x:outfile_name+'_'+str(x), range(8))
    print inputs

    word_counts = mapper(inputs)

    table_name = args[1]

    create_statement = "CREATE TABLE IF NOT EXISTS " + table_name+" (cited_id BIGINT, term_id int, counts INT) "
    print create_statement
    cursor.execute(create_statement)
    
    i = 0
    print "writing to database"
    for cited_id, counter in word_counts:
        for x in map(lambda (x,y,): (cited_id, str(x), str(y)), counter.items()):
            values = ', '.join(x)
            #print values
            string = 'INSERT INTO ' + table_name + ' VALUES('+values+')'
            #print string
            cursor.execute(string)
            i += cursor.rowcount

    print "num entry created: ", i
