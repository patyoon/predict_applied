#!/usr/bin/python2.7
import multiprocessing
import string, sys, os
from optparse import OptionParser
from collections import Counter, defaultdict
import multiprocessing
from multiprocessing_mapreduce import SimpleMapReduce
from MySQLdb import connect

def file_to_words(filename):
    """Read a citance and return a sequence of (word, occurances) values.
    """

    print multiprocessing.current_process().name, " reading"


    #use stop words
    STOP_WORDS = set([
            'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 
            'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
            ])
    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))
    output = []

    
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

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
            for word in sentences.split():
                word = word.lower()
                if cited_id.isdigit() and word.isalpha() and word not in STOP_WORDS:
                    counter[word] += 1
            cited_id_map[cited_id] += counter
    return cited_id_map.items()

def count_words(item):
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """

    cited_id, counters = item
    print multiprocessing.current_process().name, " reducing ", cited_id
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

    usage = ("usage: %prog [options] [input_text_file_name][table_name]"
             " "
             "'cited_paper_terms_count_sample_4m'"
             )
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    if len(args) != 3:
        print "need three arguments :  [input_text_file_name][table_name][file_size]"
        sys.exit(1)

    mapper = SimpleMapReduce(file_to_words, count_words, 8)
    outfile_name = os.path.basename(args[0])
    print outfile_name
    if not os.path.exists(outfile_name+'_0'):
        #inputs = divide_files('/home/mpatek/all_files_output/contexts-and-matches.txt', 8, 13571478)
        print "dividing files into 8 pieces"
        inputs = divide_files(args[0], outfile_name, 8, args[2])
    else:
        inputs = map(lambda x:outfile_name+'_'+str(x), range(8))
    print inputs

    word_counts = mapper(inputs)
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

    create_statement = "CREATE TABLE IF NOT EXISTS " + args[1]+" (cited_id BIGINT, word char(50), counts INT) "
    print create_statement
    cursor.execute(create_statement)
    
    i = 0
    print "writing to database"
    for cited_id, counter in word_counts:
        for x in map(lambda (x,y,): (cited_id, "'"+str(x)+"'", str(y)), counter.items()):
            values = ', '.join(x)
            #print values
            string = 'INSERT INTO ' + args[1] + ' VALUES('+values+')'
            #print string
            cursor.execute(string)
            i += cursor.rowcount

    print "num entry created: ", i
