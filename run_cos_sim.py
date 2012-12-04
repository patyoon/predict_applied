#!/usr/bin/python2.7
#old module 
#import logistic_regression
from MySQLdb import connect, escape_string
import numpy as np
from optparse import OptionParser
import  pickle, os, sys
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from numpy import zeros, array, matrix, linalg


#gives you (index_term_dict,term_index_dict)
def get_index_term_dicts(cursor, word_group_name):
    if os.path.exists("term_index_dict"+word_group_name+".pkl"):
        term_index_dict = pickle.load(open("term_index_dict"+word_group_name+".pkl", "rb"))
        index_term_dict = pickle.load(open("index_term_dict_"+word_group_name+".pkl", "rb"))
    else:     
        cursor.execute('SELECT distinct term FROM terms_'+ word_group_name)
        #number of target terms 
        # +1 : arbitrary...
        terms = map (lambda x : x[0].strip().lower(), cursor.fetchall())
        num_terms = len(terms)

        i = 0
        term_index_dict = {}
        index_term_dict = {}
        for term in terms:
            term_index_dict[term] = i
            index_term_dict[i] = term
            i+=1
        #for now, we use sample table cited_paper_terms_count_sample
        # not the one for all citances cited_paper_terms_count
        pickle.dump(index_term_dict, open("index_term_dict_"+word_group_name+".pkl", "wb"))  
        pickle.dump(term_index_dict, open("terms_index_"+word_group_name+".pkl", "wb"))
    return (index_term_dict, term_index_dict)

def get_cited_id_index_dict(cursor, word_group_name, cited_id_list=None):
    if cited_id_list:
        i = 0
        cited_id_index_dict = {}
        for cited_id in cited_id_list:
            cited_id_index_dict[cited_id] = i
            i+=1
        return cited_id_index_dict
    if os.path.exists("cited_id_index_dict"+word_group_name+".pkl"):            
        cited_id_index_dict = pickle.load(open("cited_id_index_"+word_group_name+".pkl", "rb"))        
    else:
        #count number of distinct cited_id in cited_paper_terms_count table
        cursor.execute('SELECT distinct cited_id FROM cited_paper_level_' + word_group_name)
        cited_ids = map (lambda x : x[0], cursor.fetchall())
        i = 0
        cited_id_index_dict = {}
        for cited_id in cited_ids:
            cited_id_index_dict[cited_id] = i
            i+=1
        pickle.dump(cited_id_index_dict, open("cited_id_index_"+word_group_name+".pkl", "wb"))
    return cited_id_index_dict

def get_lil_term_by_cited_id(cursor, cited_id_index_dict, term_index_dict, word_group_name):
        #lil sparse matrix in scipy package:
        #http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.lil_matrix.html
        #matrix is (num_cited_id) * num_terms+1 size (+1 for the last column)
    X = lil_matrix((len(term_index_dict),len(cited_id_index_dict),))
    
        #dict for tracking sparse matrix index and actual cited id
    i = 0
    print "Read cited_id and terms"
    for term in term_index_dict:
            #print 'SELECT cited_id, sum(count) from '+count_table_name+' where term=\''+escape_string(str(term.lower()))+'\' group by cited_id'
        try:
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT cited_id, sum(count) from cited_paper_level_'+word_group_name+' where word=\''+escape_string(str(term.lower()))+'\' group by cited_id')
            cited_id_count_dict = dict(cursor.fetchall())
            i+=1
            X[[term_index_dict[term.lower()]],[cited_id_count_dict.keys()]] = cited_id_count_dict.values()
        except:
            pickle.dump(X, open("X_tem_by_cited_id_"+word_group_name+".pkl", "wb"))
    print "finished inserting count into sparse matrix"
        #row standardize X as distribution
    return X


def run_cos_sim(word_group_name):
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()
        
    print "Fetching terms from DB" 
    
    (index_term_dict, term_index_dict) = get_index_term_dicts(cursor, word_group_name)
    print "got index_term_dicts"
    num_terms = len(term_index_dict.keys())
    cited_id_index_dict = get_cited_id_index_dict(cursor, word_group_name)
    print "got cited_id_index_dict"
    if not os.path.exists("X_term_by_cited_id_"+word_group_name+".pkl"):    
        num_cited_id = len(cited_id_index_dict.keys())
        print "Got distinct cited_ids. number of cited papers : %d" % (num_cited_id)
        X = get_lil_term_by_cited_id(cursor, cited_id_index_dict,
                                     term_index_dict, word_group_name)
        X = X.tocsr()
        X_trans = X.transpose(copy=True)
        X_prod = X * X_trans
        pickle.dump(X, open("X_term_by_cited_id_"+word_group_name+".pkl", "wb"))
        pickle.dump(X_prod, open("X_prod_"+word_group_name+".pkl", "wb"))
    else:
        print "reading pickles"
        X_prod = pickle.load(open("X_prod_"+word_group_name+".pkl", "rb"))
        X = pickle.load(open("X_term_by_cited_id_" + word_group_name + ".pkl", 'rb'))
    print "got X_term_by_cited_id"
    cursor.execute('CREATE TABLE IF NOT EXISTS cos_sim_result_l2_'+word_group_name+' (term1 varchar(50), term2 varchar(50), sim float)')
    cursor.execute('CREATE TABLE IF NOT EXISTS cos_sim_result_l1_'+word_group_name+' (term1 varchar(50), term2 varchar(50), sim float)')
    for i in xrange(num_terms):
        for j in xrange(i+1, num_terms):
            value = (linalg.norm(X[[i],:].todense(),1)) * linalg.norm(X[[j],:].todense(),1)
            value2 = (linalg.norm(X[[i],:].todense(),2)) * linalg.norm(X[[j],:].todense(),2)
            cos_sim1 = X_prod[[i],[j]].item()/value
            cos_sim2 = X_prod[[i],[j]].item()/value2
            if value > 0:
                cursor.execute("INSERT INTO cos_sim_result_l1_"+word_group_name+" VALUES ('" + escape_string(index_term_dict[i])
                                   +"', '"+escape_string(index_term_dict[j])+"', "+ str(cos_sim1)+")")
            else:
                cursor.execute("INSERT INTO cos_sim_result_l1_"+word_group_name+ " (term1, term2) VALUES ('" + escape_string(index_term_dict[i]) +"', '"+escape_string(index_term_dict[j])+"')")
            if value2 > 0:
                cursor.execute("INSERT INTO cos_sim_result_l2_"+word_group_name+" VALUES ('" + escape_string(index_term_dict[i])
                               +"', '"+escape_string(index_term_dict[j])+"', "+ str(cos_sim2)+")")
            else:
                cursor.execute("INSERT INTO cos_sim_result_l2_"+word_group_name+" (term1, term2) VALUES ('" + escape_string(index_term_dict[i]) +"', '"+escape_string(index_term_dict[j])+"')")

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name]")

    parser = OptionParser(usage)
    
    parser.add_option("-t", "--time", dest="pubtime", action = "store_true", 
                      default=False, help="Use publication date as feature")  
    parser.add_option("-a", "--avgfile", dest="avgtime", action = "store",
                      default=None, help="Use pre-saved file for average cited count per year. (Use name before _07 or_tot)")

    (options, args) = parser.parse_args()

    if len(args) != 1:
        print "need one argument [word_group_name]"
        sys.exit(1)

    run_cos_sim(args[0])
