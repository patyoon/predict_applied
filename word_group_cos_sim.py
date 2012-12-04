#!/usr/bin/python
"""
This computes the cosine similarity between the lda topic groups and
Teufel's categories at citance level. (**edit: Uses Small's categories 
instead of Teufel's)
"""
import sys, pickle, os
from MySQLdb import connect, escape_string
from run_cos_sim import get_index_term_dicts
from optparse import OptionParser
from collections import defaultdict
from scipy.sparse import lil_matrix, linalg


# return label_term_dict
def get_label_term_dict(cursor, word_group_name):
    if os.path.exists("label_term_dict_"+word_group_name+".pkl"):
        label_term_dict = pickle.load(open("label_term_dict_"+word_group_name+".pkl", "rb"))
    else:
        cursor.execute('SELECT label, term from terms_'+word_group_name)
        label_term_list = map(lambda x:(x[0], x[1]), cursor.fetchall())
        label_term_dict = defaultdict(list)
        for (label, term) in label_term_list:
            label_term_dict[label.strip().lower()].append(term.strip().lower())
        pickle.dump(label_term_dict, open("label_term_dict_"+word_group_name+".pkl", "wb"))
    return label_term_dict

def get_label_citedID_count_matrix(word_group_name):
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    print "get_label_citedID_count_matrix"
    if not os.path.exists("X_term_by_cited_id_"+word_group_name+".pkl"):
        print "X_term_by_cited_id_"+word_group_name+".pkl not found" 
        sys.exit(1)
    X = pickle.load(open("X_term_by_cited_id_" + word_group_name + ".pkl", 'rb'))       
    print "X_term_by_cited_id loaded"
    (index_term_dict, term_index_dict) = get_index_term_dicts(cursor, word_group_name)
    label_term_dict = get_label_term_dict(cursor, word_group_name)
    X_label = lil_matrix((len(label_term_dict.keys()), X.get_shape()[1]))
    for label in label_term_dict:
        term_index = map(lambda x: term_index_dict[x], label_term_dict[label])
        label_index = label_term_dict.keys().index(label)
        X_label[[label_index],:] = X[[label_index],[term_index]].sum(0)
    return X_label

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name1][word_group_name2]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    if len(args) != 2:
        print "need two arguments :  [word_group_name1] [word_group_name2]"
        sys.exit(1)
    
    X_label_by_cited_id_1 = get_label_citedID_count_matrix(args[0])
    X_label_by_cited_id_2 = get_label_citedID_count_matrix(args[1])
    
    label_list = pickle.load(open("label_term_dict_"+word_group_name+".pkl", "rb")).keys()
    cursor.execute('CREATE TABLE IF NOT EXISTS cos_sim_word_groups_result (label1 varchar(50), label2 varchar(50), sim float)')
    for i in xrange(X_label_by_cited_id_1.get_shape()[0]):
        for j in xrange(i+1, X_label_by_cited_id_2.get_shape()[0]):
            vec1 = X_label_by_cited_id_1[[i],:]
            vec2 = X_label_by_cited_id_2[[j],:]
            dot_prod = vec1 * vec2
            cos_sim = dot_prod/linalg.norm(vec1,1)/linalg.norm(vec2,1)
            if value > 0: 
                cursor.execute("INSERT INTO cos_sim_word_groups_result VALUES ('" + escape_string(label_list[i]) +"', '"+escape_string(label_list[j])+"', "+ str(cos_sim)+")")
            else:
                cursor.execute("INSERT INTO cos_sim_word_groups_result (label1, label2) VALUES ('" + escape_string(label_list[i]) +"', '"+escape_string(label_list[j])+"')")
