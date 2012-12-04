#!/usr/bin/python2.7
from MySQLdb import connect, escape_string
from collections import Counter
from scipy import corrcoef
import pickle
from optparse import OptionParser
import sys, os
from word_group_cos_sim import get_label_citedID_count_matrix, get_label_term_dict

DEFAULT_DB = 'shepard'
DEFAULT_USER = 'root'
DEFAULT_PASSWD = 'shepard'
DEFAULT_HOST = 'localhost'

TERMS_TABLE = 'terms_teufel'
COUNTS_TABLE = 'cited_paper_term_count'

TERM_CITED_ID_TEUFEL = '/home/yeyoon/sentiment/script/yeyoon/X_term_by_cited_id_teufel.pkl'

TERM_CITED_ID_INDEX = '/home/yeyoon/sentiment/script/yeyoon/cited_id_index.pkl'

ROW_IND = 0
COL_IND = 1

def compute_correlation_matrix(word_group_name):
    # Get X_label table
    #X_label = pickle.load(open('X_term_by_cited_id_teufel.pkl','rb'))
    conn = connect(db=DEFAULT_DB, user=DEFAULT_USER, passwd=DEFAULT_PASSWD, host=DEFAULT_HOST)
    cursor = conn.cursor()    

    X_label = get_label_citedID_count_matrix(word_group_name)
    print "Got X_label vector"

    cited_id_index_dict = get_cited_id_index_dict(cursor, word_group_name)
 
    cited_count_07_list = []
    if os.path.exists("regression_Y_"+word_group_name+".pkl"):
        Y = pickle.load(open("regression_Y_"+word_group_name+".pkl", "rb"))
    else:
        for cited_id in cited_id_list:
        # retrieve information on cited_paper
            cursor.execute('SELECT ncit07 FROM '
                           + cited_paper_table_name +
                           ' where cited_id='+str(cited_id))
            #parse ncit_07 and ncit count for cited papers in cited_paper_terms_count
            sql_result = cursor.fetchall()
            if len(sql_result) == 3:
                cited_count_07 = sql_result[0][1] if sql_result[0][1] else 0
                cited_count_07_list.append(cited_count_07)
        pickle.dump(good_year_index_list, open("regression_Y_"+word_group_name+".pkl", "wb"))

    # Get the ncit_citedID vector

    label_term_dict = get_label_term_dict(cursor, word_group_name)

    ncit_vec = cited_count_07_list
    
    print "finished getting ncit07"
    
    if not X_label.shape[COL_IND] == len(cited_count_07_list):
        print "Number of citedID's do not match!!!"
        return

    print "create table"
    cursor.execute("CREATE TABLE IF NOT EXISTS ncit_correlation_"+word_group_name+" "+
                   "(label varchar(50), corr float(4,7))")

    for label_index in xrange(X_label.shape[0]):
        corr = corrcoef(ncit_vec, X_label[[label_index],:].todense())[0][1]
        label = label_term_dict.keys().index(label_index)
        cursor.execute("INSERT INTO ncit_correlation_"+word_group_name+" "+
                       "VALUES (" + label + ", " + str(corr) + ")" )

if __name__=='__main__':
    usage = ("usage: %prog [options] [word_group_name]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    if len(args) != 1:
        print "need one argument :  [word_group_name]"
        sys.exit(1)
    
    compute_correlation_matrix(args[0])

