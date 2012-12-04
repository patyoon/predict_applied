#!/usr/bin/python2.6
import multiprocessing
import string, sys
from MySQLdb import connect

from multiprocessing_mapreduce import SimpleMapReduce

def file_to_words(citance_id):
    """Read a citance and return a sequence of (word, occurances) values.
    """
    #use stop words
    STOP_WORDS = set([
            'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in', 
            'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
            ])
    TR = string.maketrans(string.punctuation, ' ' * len(string.punctuation))

    print multiprocessing.current_process().name, 'reading', citance_id
    output = []
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd = 'shepard')
    cursor = conn.cursor()
    cursor.execute('SELECT count(*) from citances')
    print multiprocessing.current_process().name, "count ", cursor.fetchall()
    print multiprocessing.current_process().name, "statement ", 'SELECT cited_id, sentences from citances where citance_id='+ str(citance_id)
    cursor.execute('SELECT cited_id, sentences from citances where citance_id='+ str(citance_id))
    try:
        if cursor.rowcount == 0:
            print error
            sys.exit(1)
        line = cursor.fetchall()[0][1]
        cited_id = str(cursor.fetchall()[0][0])
        print "success"
        line = line.translate(TR) # Strip punctuation
        counter = Counter()
        for word in line.split():
            word = word.lower()
            if word.isalpha() and word not in STOP_WORDS:
                counter[word] += 1
        conn.close()
        return (cited_id, counter)
    except IndexError as e:
        conn.close()
        print cursor.fetchall(), " ", citance_id, " Index error"


def count_words(item):
    """Convert the partitioned data for a word to a
    tuple containing the word and the number of occurances.
    """
    cited_id, counters = item
    return (cited_id, reduce(lambda x, y: x+y, counters))

if __name__ == '__main__':
    
    citance_ids = map(lambda x:x+1, range(13571478))
    
    mapper = SimpleMapReduce(file_to_words, count_words, 1)
    word_counts = mapper(citance_ids)
    
    create_statement = "CREATE TABLE IF NOT EXISTS  cited_paper_all_count (cited_id BIGINT, word char(50), counts INT) "
    print create_statement
    cursor.execute(create_statement)
    
    i = 0
    for cited_id, counter in word_counts:
        values = ', '.join(map(lambda (x,y): (cited_id, x,y), counter.items()))
        #print values
        cursor.execute('INSERT INTO cited_paper_all_count (cited_id, word, counts) VALUES '+values)
        i += cursor.rowcount

    print i
