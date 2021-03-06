from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys
import pickle, os
from math import sqrt
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from numpy import zeros, array, matrix

JNL_LEVEL_ABSTRACT_COUNT_TABLE = 'journal_abstract_MED_jid_word_count'
JNL_LEVEL_TITLE_COUNT_TABLE = 'journal_title_MED_jid_word_count'
CPID_LEVEL_ABSTRACT_COUNT_TABLE = 'cpid_abstract_MED_jid_word_count'
CPID_LEVEL_TITLE_COUNT_TABLE = 'cpid_title_MED_jid_word_count'
CORPUS_LEVEL_ABSTRACT_COUNT_TABLE = 'all_word_abstract_MED_word_count'
CORPUS_LEVEL_TITLE_COUNT_TABLE = 'all_word_title_MED_word_count'

def get_index_word_dicts(cursor, feature_type):
    if os.path.exists("word_index_dict.pkl"):
        word_index_dict = pickle.load(open("word_index_"+feature_type+"_dict.pkl", "rb"))
        index_word_dict = pickle.load(open("index_word_"+feature_type+"dict.pkl", "rb"))
    else:
        cursor.execute('select word from '+ eval("CORPUS_LEVEL_"+feature_type.upper()+"_COUNT_TABLE") +
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
        pickle.dump(index_word_dict, open("index_word_"+feature_type+"_dict.pkl", "wb"))
        pickle.dump(word_index_dict, open("words_index_"+feature_type+"_dict.pkl", "wb"))
    return (index_word_dict, word_index_dict)

def get_level_index_dict(cursor, feature_type, level_type):
    if os.path.exists("level_index_"+feature_type+"_"+level_type+"_dict.pkl"):
        level_index_dict = pickle.load(open("level_index_"+feature_type+"_"+level_type+"_dict.pkl", "rb"))
    else:
        #count number of distinct level in cited_paper_words_count table
        cursor.execute('SELECT distinct '+level_type+' FROM MED_cpid_refjnl_rlev_ct where rlev!=0')
        levels = map (lambda x : x[0], cursor.fetchall())
        i = 0
        level_index_dict = {}
        for level in levels:
            level_index_dict[level] = i
            i+=1
        pickle.dump(level_index_dict, open("level_index_"+feature_type+"_"+level_type+"_dict.pkl", "wb"))
        print len(level_index_dict)
    return level_index_dict

def get_sparse_matrix(cursor, feature_type, level_type):
        #lil sparse matrix in scipy package:
        #http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.lil_matrix.html
        #matrix is (num_level) * num_words+1 size (+1 for the last column)
    if os.path.exists("X_word_by_"+level_type+"_"+feature_type+".pkl"):
        X = pickle.load(open("X_word_by_"+level_type+"_"+feature_type+".pkl", "rb"))
    else:
        (index_word_dict, word_index_dict) = get_index_word_dicts(cursor, feature_type)
        level_index_dict = get_level_index_dict(cursor, feature_type, level_type)
        X = lil_matrix((len(level_index_dict),len(word_index_dict),))
        print X.get_shape()
        #dict for tracking sparse matrix index and actual cited id
        i = 0
        print "Reading level and words..."

        for level in level_index_dict:
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT word, count from '+ eval(level_type.upper() + '_LEVEL_'+feature_type.upper()+'_COUNT_TABLE')
                           +' where '+level_type+'=\'' + level +'\'')
            word_count_dict = dict(cursor.fetchall())
            vec = map(lambda x : word_count_dict[x] if x in word_count_dict else 0, word_index_dict.keys())
            i+=1
            X[[level_index_dict[level]], [word_count_dict.keys()]] = word_count_dict.values()

        pickle.dump(X, open("X_word_by_"+level_type+"_"+feature_type+".pkl", "wb"))
        print "finished inserting count into sparse matrix"
        #row standardize X as distribution
        return X

def get_label_vector(cursor, feature_type, level_type):
    if os.path.exists("Y_"+level_type+".pkl"):
        Y = pickle.load(open("Y_"+level_type+".pkl", "rb"))
    else:
        level_index_dict = get_level_index_dict(cursor, feature_type, level_type)
        Y = lil_matrix((len(level_index_dict),1,))
        i = 0
        for level in level_index_dict:
            cursor.execute('SELECT distinct rlev from MED_cpid_refjnl_rlev_ct where '
                           +level_type+'=\''+level+'\'')
            rlevl = cursor.fetchall()
            print rlevl
            print level
            assert len(rlevl) == 1
            Y[[level_index_dict[level]],[0]] = rlevl[0][0] 
        Y = pickle.dump(Y, open("Y_" + level_type + ".pkl", "wb"))
    return Y

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name]")

    parser = OptionParser(usage)

    (options, args) = parser.parse_args()
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')

    cursor = conn.cursor()
    X1 = get_sparse_matrix(cursor, 'abstract', 'jnl')
    X2 = get_sparse_matrix(cursor, 'title', 'jnl')
    #X3 = get_sparse_matrix(cursor, 'title', 'cpid')
    #X4 = get_sparse_matrix(cursor, 'abstract', 'cpid')
    Y1 = get_label_vector(cursor, 'title', 'jnl')
    Y2 = get_label_vector(cursor, 'title', 'cpid')
