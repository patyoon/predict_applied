#!/usr/bin/python2.7
#old module 
#import logistic_regression
from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys
import pickle, os
from math import sqrt
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from numpy import zeros, array, matrix
from run_cos_sim import get_index_term_dicts, get_cited_id_index_dict

def run_log_reg(word_group_name, cited_paper_table_name, pub_year_cutoff):
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    print "Fetching terms from DB" 

    if os.path.exists("regression_X_"+word_group_name+".pkl"):
        X  = pickle.load(open("regression_X_"+word_group_name+".pkl", "rb"))        
        Y = pickle.load(open("regression_Y_"+word_group_name+".pkl", "rb"))
        X_year = pickle.load(X_year, open("regression_X_year_"+word_group_name+".pkl", "rb"))
    else:
        (index_term_dict,term_index_dict) =  get_index_term_dicts(cursor, word_group_name)
        num_terms = len(term_index_dict.keys())
        if not os.path.exists("reg_cited_id_index_dict_"+word_group_name+".pkl"):
            cursor.execute("SELECT distinct cited_paper_level_"+word_group_name
                           +".cited_id FROM cited_paper_level_"+word_group_name+
                           " INNER JOIN cited_papers ON cited_paper_level_"+
                           word_group_name+".cited_id=cited_papers.cited_id"+
                           " WHERE cited_papers.year IS NOT NULL AND "+
                           "cited_papers.ncit07 IS NOT NULL and cited_papers.ncit IS NOT NULL")
        #get cited_id only with valid pubyear, ncit, ncit07
            print "finished running long join query"
            cited_id_index_dict = get_cited_id_index_dict(cursor, word_group_name, 
                                                          map(lambda x:x[0], cursor.fetchall()))
            pickle.dump(cited_id_index_dict, open("reg_cited_id_index_dict_"+word_group_name+".pkl", "wb"))
        else:
            cited_id_index_dict = pickle.load(open("reg_cited_id_index_dict_"+word_group_name+".pkl", "rb"))        

        num_cited_id = len(cited_id_index_dict.keys())
        print "number of cited papers : %d" % (num_cited_id)

        #matrix is (num_cited_id) * num_terms+1 size (+1 for the last column)
        X = lil_matrix((num_cited_id, num_terms+1,))
        print X.get_shape()
        i = 0
        cited_count_07_list = list()
        adj_cited_count_tot_list = list()
        pub_year_list = list()
        good_year_index_list = list()
        cited_id_list = cited_id_index_dict.keys()

        for cited_id in cited_id_list:
        # retrieve information on cited_paper
            cursor.execute('SELECT year, ncit07, ncit_tot_adj1 FROM '
                           + cited_paper_table_name +
                           ' where cited_id='+str(cited_id))
            #parse ncit_07 and ncit count for cited papers in cited_paper_terms_count
            sql_result = cursor.fetchall()
            if len(sql_result) == 3:
                cited_count_07 = sql_result[0][1] if sql_result[0][1] else 0
                cited_count_tot = sql_result[0][2]if sql_result[0][2] else 0
                pub_year = sql_result[0][0]if sql_result[0][0] else 0
                if pub_year >= pub_year_cutoff:
                    good_year_index_list.append(cited_id_list.index(pub_year))
                cited_count_07_list.append(cited_count_07)
                adj_cited_count_tot_list.append(cited_count_tot)
            #fill in sqrt'ed age to the last column
                X[[cited_id_index_dict[cited_id]],[num_terms]] = sqrt(2007-pub_year)

        print "finished inserting years"

        print "Read cited_id and terms"
        for cited_id in cited_id_list:
            if i%100000 == 0:
                print i, "th sparse insert"
            cursor.execute('SELECT word, count from cited_paper_level_'+word_group_name+' where cited_id='+str(cited_id))
            term_count_dict = dict(cursor.fetchall())
            #vec = map(lambda x : term_count_dict[x] if x in term_count_dict else 0, term_index_dict.keys())
            i+=1
            X[[cited_id_index_dict[cited_id]],term_count_dict.keys()] = term_count_dict.values()
        print "finished inserting count into sparse matrix"
        #row standardize X as distribution

        pickle.dump(good_year_index_list, open("good_year_index_list_"+pub_year_cutoff+".pkl", "wb"))

        ccd = spdiags(1./X.sum(1).T, 0, X.shape[0], X.shape[0], format='lil')
        X = ccd * X
        pickle.dump(X, open("regression_X_"+word_group_name+".pkl", "wb"))
        Y = csr_matrix(cited_count_07_list)
        pickle.dump(Y, open("regression_Y_"+word_group_name+".pkl", "wb"))
        X_ncit = matrix(adj_cited_count_tot_list)
        pickle.dump(X_year, open("regression_X_ncit_"+word_group_name+".pkl", "wb"))
    
    #only take cited paper published after year pub_year_cutoff
    X = X[good_year_index_list, :] 
    Y = Y[good_year_index_list, :]
    X_year = X_year[good_year_index_list, :]

    #(TODO)

    run_and_plot_reg(X,Y, X_year)

def run_and_plot_reg(X,Y, X_year):
    
    print "finished converting count into distribution"

    """1) Using term distribution and adjusted age, can we build a model on predicting number of citations in 2007
    """

    clf = SGDRegressor()
    
    scores = cross_val_score(clf, X, Y, cv=10)
    print "Accuracy of reg2 : %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() / 2)
    pickle.dump(clf, open("regressor_1"+word_group_name+".pkl", "wb"))
    pickle.dump(scores, open("regression_scores_1"+scores+".pkl", "wb"))

    """
    2) Using term distribution and number of citation(year adjusted) before year 2007
    can we build a model on predicting number of citations in 2007?
    """

    #insert into X
    X[:,num_terms] = X_year.transpose()

    clf = SGDRegressor()
    
    scores = cross_val_score(clf, X, Y, cv=10)
    print "Accuracy of reg2 : %0.2f (+/- %0.2f)" % (scores.mean(), scores.std() / 2)    
    pickle.dump(clf, open("regressor_2"+word_group_name+".pkl", "wb"))
    pickle.dump(scores, open("regression_scores_2"+scores+".pkl", "wb"))


if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name] [cited_paper_table_name] [pub_year_cutoff]"
             " Run logistic regression with logistic_regression method."
             "'cited_paper_terms_count_sample_4m'"
                 )

    parser = OptionParser(usage)
    
    parser.add_option("-t", "--time", dest="pubtime", action = "store_true", 
                      default=False, help="Use publication date as feature")  
    parser.add_option("-a", "--avgfile", dest="avgtime", action = "store",
                      default=None, help="Use pre-saved file for average cited count per year. (Use name before _07 or_tot)")

    (options, args) = parser.parse_args()

    if len(args) != 3:
        print "need two arguments [word_group_name] [cited_paper_table_name][pub_year_cutoff]"
        sys.exit(1)

    run_log_reg(args[0], args[1], args[2])
