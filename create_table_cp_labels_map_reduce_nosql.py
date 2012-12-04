#!/usr/bin/python2.7
import multiprocessing
import string, sys, os
from optparse import OptionParser
from collections import Counter, defaultdict
import multiprocessing
from multiprocessing_mapreduce import SimpleMapReduce
from MySQLdb import connect

def file_to_labels(filename):
    """Read a citance and return a sequence of (label, occurances) values.
    """

    print multiprocessing.current_process().name, " reading"

    word2labels = {}
    # with open('label_list','rt') as wf:
    #     for line in wf:
    #         st = line.split('\t')
    #         word = st[0]
    #         label_id = st[1]
    #         print word, "\t", label_id
    #         word2labels[word] = label_id

    word_paper_pairs = {}

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

    cursor.execute ("SELECT DISTINCT term, term_id FROM terms")
    pairs = cursor.fetchall()
    word2labels = dict(pairs)

    # use stop words
    STOP_WORDS = set([
            'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 
            'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
            ])
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
            citing_id = tokens[7]
            cited_id = tokens[14]
            for word in sentences.split():
                word = word.lower()
                if cited_id.isdigit() and word.isalpha() and word not in STOP_WORDS and word in word2labels and (cited_id, citing_id, word) not in word_paper_pairs:
                    word_paper_pairs[(cited_id,citing_id,word)] = True
                    label_id = word2labels[word]
                    counter[label_id] += 1

            #print multiprocessing.current_process().name, " ", counter
            #output.append((cited_id, counter,))
            cited_id_map[cited_id] += counter
    return cited_id_map.items()

def count_labels(item):
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

    usage = ("")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    # conn = connect(host = 'localhost', user = 'root',
    #                db = 'shepard', passwd = 'shepard')

    # cursor = conn.cursor()

    # #write word list file                                                              
    # #For now, use term                                                                
    # if not os.path.exists('label_list'):
    #     cursor.execute ("SELECT DISTINCT term_id, term FROM terms")
    #     if cursor.rowcount == 0:
    #         print "No labels retrieved"
    #         sys.exit(1)

    #     #label list has tuple of (term, term_id)                                         
    #     termList = cursor.fetchall()
    #     outfile = open('label_list', 'w')
    #     for (term_id, term) in termList:
    #         outfile.write(term+"\t"+str(term_id)+"\n")
    #     print "Number of terms used: %d" % len(termList)
    #     outfile.close()
    # else:
    #     print "term_list already exists"

    # Begin the Map-Reduce
    mapper = SimpleMapReduce(file_to_labels, count_labels, 8)
    outfile_name = os.path.basename(args[0])
    print outfile_name
    if not os.path.exists(outfile_name+'_0'):
        #inputs = divide_files('/home/mpatek/all_files_output/contexts-and-matches.txt', 8, 13571478)
        print "dividing files into 8 pieces"
        inputs = divide_files(args[0], outfile_name, 8, 4000000)
    else:
        inputs = map(lambda x:outfile_name+'_'+str(x), range(8))
    print inputs

    label_counts = mapper(inputs)
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')

    cursor = conn.cursor()

    create_statement = "CREATE TABLE IF NOT EXISTS cited_paper_labels_4m (cited_id BIGINT, word char(50), counts INT) "
    print create_statement
    cursor.execute(create_statement)
    
    i = 0
    print "writing to database"
    for cited_id, counter in label_counts:
        for x in map(lambda (x,y,): (cited_id, "'"+str(x)+"'", str(y)), counter.items()):
            values = ', '.join(x)
            #print values
            string = 'INSERT INTO cited_paper_labels_4m VALUES('+values+')'
            #print string
            cursor.execute(string)
            i += cursor.rowcount

    print "num entry created: ", i
