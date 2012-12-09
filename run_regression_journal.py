from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys
import pickle, os
from math import sqrt
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from numpy import zeros, array, matrix
from run_cos_sim import get_index_term_dicts, get_cited_id_index_dict

JOURNAL_LEVEL_COUNT_TABLE = 'journal_MED_jid_word_count'
PAPER_LEVEL_COUNT_TABLE = 'cpid_MED_jid_word_count'
CORPUS_LEVEL_JNL_COUNT_TABLE = 'all_word_abstract_MED_word_count'

def get_index_word_dicts(cursor):
    if os.path.exists("word_index_dict.pkl"):
        word_index_dict = pickle.load(open("word_index_dict.pkl", "rb"))
        index_word_dict = pickle.load(open("index_word_dict.pkl", "rb"))
    else:     
        cursor.execute('select word from '+ CORPUS_LEVEL_COUNT_TABLE +
                       ' where count > 50 order by count desc')
        words = map (lambda x : x[0].strip().lower(), cursor.fetchall())
        num_words = len(words)
        i = 0
        word_index_dict = {}
        index_word_dict = {}
        for word in words:
            word_index_dict[word] = i
            index_word_dict[i] = word
            i+=1
        pickle.dump(index_word_dict, open("index_word_dict.pkl", "wb"))  
        pickle.dump(word_index_dict, open("words_index_dict.pkl", "wb"))
        print len(index_word_dict)
        print len(word_index_dict)
    return (index_word_dict, word_index_dict)

def get_cpid_index_dict(cursor):
    if os.path.exists("cpid_index_dict.pkl"):            
        cpid_index_dict = pickle.load(open("cpid_index_dict.pkl", "rb"))   
    else:
        #count number of distinct cpid in cited_paper_words_count table
        cursor.execute('SELECT distinct cpid FROM ' + 
                       PAPER_LEVEL_COUNT_TABLE)
        cpids = map (lambda x : x[0], cursor.fetchall())
        i = 0
        cpid_index_dict = {}
        for cpid in cpids:
            cpid_index_dict[cpid] = i
            i+=1
        pickle.dump(cpid_index_dict, open("cpid_index_dict.pkl", "wb"))
        print len(cpid_index_dict)
    return cpid_index_dict

def get_journal_index_dict(cursor):
    if os.path.exists("journal_index_dict.pkl"):            
        journal_index_dict = pickle.load(open("journal_index_dict.pkl", "rb"))   
    else:
        #count number of distinct journal in cited_paper_words_count table
        cursor.execute('SELECT distinct jnl FROM ' + 
                       JOURNAL_LEVEL_COUNT_TABLE)
        journals = map (lambda x : x[0], cursor.fetchall())
        i = 0
        journal_index_dict = {}
        for journal in journals:
            journal_index_dict[journal] = i
            i+=1
        pickle.dump(journal_index_dict, open("journal_index_dict.pkl", "wb"))
        print "journal", i
    return journal_index_dict

def get_journal_sparse_matrix(cursor):
        #lil sparse matrix in scipy package:
        #http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.lil_matrix.html
        #matrix is (num_journal) * num_words+1 size (+1 for the last column)
    if os.path.exists("X_word_by_journal.pkl"):            
        journal_index_dict = pickle.load(open("X_word_by_journal.pkl", "rb"))   
    else:
        (index_word_dict, word_index_dict) = get_index_word_dicts(cursor)
        journal_index_dict = get_journal_index_dict(cursor)
        X = lil_matrix((len(word_index_dict), len(journal_index_dict),))    
        print X.get_shape()
        #dict for tracking sparse matrix index and actual cited id
        i = 0
        print "Reading journal and words..."
        for word in word_index_dict:
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT jnl, sum(count) from '+ JOURNAL_LEVEL_COUNT_TABLE 
                           +' where word=\''+escape_string(word)+'\' group by jnl')
            journal_count_dict = dict(cursor.fetchall())
            i+=1
            #t = map(lambda (x,y): (journal_index_dict[x], int(y)), journal_count_dict.items())
            #f = filter(lambda (x,y): x > len(journal_index_dict), t)
            #print f
            #print word_index_dict[word.lower()]
            X[[1],[3,4]] = [1,3]
            X[[2],[3,4]] = [3,5]
            X[[1],[4]] = 3
            print "fefe", X[[1:2],[3:4]]
            
            #print map(lambda x: journal_index_dict[x], 
            #          journal_count_dict.keys())
            X[[word_index_dict[word.lower()]], [[map(lambda x: journal_index_dict[x], 
                                                    journal_count_dict.keys())]]] = (
                map(lambda x: int(x), journal_count_dict.values())) 
            # for x in journal_count_dict:
            #     try:
            #         X[[word_index_dict[word.lower()]], [journal_index_dict[x]]] = int(journal_count_dict[x])
            #     except IndexError as e:
            #         print word_index_dict[word.lower()], journal_index_dict[x]
            #         print(e)
            #         sys.exit(1)
        pickle.dump(X, open("X_word_by_journal.pkl", "wb"))
        print "finished inserting count into sparse matrix"
        #row standardize X as distribution
        return X

def get_cpid_sparse_matrix(cursor):
        #lil sparse matrix in scipy package:
        #http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.lil_matrix.html
        #matrix is (num_cpid) * num_words+1 size (+1 for the last column)
    if os.path.exists("X_word_by_cpid.pkl"):            
        cpid_index_dict = pickle.load(open("X_word_by_cpid.pkl", "rb"))   
    else:
        (index_word_dict, word_index_dict) = get_index_word_dicts(cursor)
        cpid_index_dict = get_cpid_index_dict(cursor)
        X = lil_matrix(len(cpid_index_dict), len(word_index_dict), )    
        #dict for tracking sparse matrix index and actual cited id
        print X.get_shape()
        i = 0
        print "Reading journal and words..."
        for word in word_index_dict:
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT cpid, sum(count) from '+ PAPER_LEVEL_COUNT_TABLE 
                           +' where word=\''+escape_string(word)+'\' group by cpid')
            cpid_count_dict = dict(cursor.fetchall())
            #print cpid_count_dict
            i+=1
            X[[cpid_count_dict.keys()],[word_index_dict[word.lower()]]] = (
                cpid_count_dict.values())
        pickle.dump(X, open("X_word_by_cpid.pkl", "wb"))
        print "finished inserting count into sparse matrix"
        #row standardize X as distribution
        return X

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name]")

    parser = OptionParser(usage)
    
    (options, args) = parser.parse_args()
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()
    X1 = get_cpid_sparse_matrix(cursor)
    X2 = get_journal_sparse_matrix(cursor)
    X3 =
