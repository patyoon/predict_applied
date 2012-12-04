#!/usr/bin/python2.7
#old module 
#import logistic_regression
from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys
import  pickle 
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from numpy import zeros, array, matrix
from word_group_cos_sim import get_label_term_dict
from itertools import product, combinations

if __name__ == "__main__":

    usage = ("usage: %prog [options] [word_group_name]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    word_group_name = args[0]
    
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()
    
    label_term_dict = get_label_term_dict(cursor, word_group_name)
    term_set = set([item for sublist in label_term_dict.values() for item in sublist])

    tot_avg_in_label = 0
    tot_avg_not_in_label = 0

    out_file = open("label_cos_sim"+word_group_name, 'w')
    
    for label in label_term_dict:
        print "label: ", label
        in_label = 0
        not_in_label = 0
        i = 0
        for pair in combinations(label_term_dict[label], 2):
            if pair[0] > pair[1]:
                pair = (pair[1], pair[0],)
            cursor.execute('SELECT sim from cos_sim_result where term1=\''+escape_string(pair[0])
                           +'\' AND term2=\''+ escape_string(pair[1])+'\'')
            res = cursor.fetchone()
            if not res:
                print pair
                continue
            if res[0]:
                i +=1
                in_label += res[0]
        avg_in_label = float(in_label)/i
        print "label: ", label, " in label average ",avg_in_label 
        rest_term = term_set - set(label_term_dict[label])
        i = 0
        for pair in product(rest_term, label_term_dict[label]):
            if pair[0] > pair[1]:
                pair = (pair[1], pair[0],)
            cursor.execute('SELECT sim from cos_sim_result where term1=\''+escape_string(pair[0])
                           +'\' AND term2=\''+ escape_string(pair[1])+'\'')
            res = cursor.fetchone()[0]
            if res:
                i +=1
                not_in_label += res
        avg_not_in_label = float(not_in_label)/i
        print "label: ", label, " not in label average ",avg_not_in_label
        out_file.write(label+"\t"+str(avg_in_label)+"\t"+str(avg_not_in_label)+"\n")
        tot_avg_in_label +=avg_in_label
        tot_avg_not_in_label +=avg_not_in_label
    print "tot_avg_in_label", tot_avg_in_label, " tot_avg_not_in_label", tot_avg_not_in_label
    out_file.close()


            
