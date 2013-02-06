from MySQLdb import escape_string
from optparse import OptionParser
import sys
import pickle, os
from math import sqrt
from scipy.sparse import coo_matrix, csr_matrix, lil_matrix
from numpy import zeros, array, matrix
import numpy as np
import datetime, random
from collections import Counter

import heapq, operator

#sql table names in shepard db
JNL_LEVEL_ABSTRACT_COUNT_TABLE = 'journal_abstract_MED_jid_word_count'
JNL_LEVEL_TITLE_COUNT_TABLE = 'journal_title_MED_jid_word_count'
CPID_LEVEL_ABSTRACT_COUNT_TABLE = 'cpid_abstract_MED_jid_word_count'
CPID_LEVEL_TITLE_COUNT_TABLE = 'cpid_title_MED_jid_word_count'
CORPUS_LEVEL_ABSTRACT_COUNT_TABLE = 'all_word_abstract_MED_word_count'
CORPUS_LEVEL_TITLE_COUNT_TABLE = 'all_word_title_MED_word_count'
CPID_JNL_LEV_TABLE = 'MED_cpid_disc_jnl_lev_sec_title_abstr'

def get_index_word_dicts(cursor, feature_type, threshold=100):
    if os.path.exists("word_index_%s_%d_dict.pkl" % (feature_type, threshold)):
        word_index_dict = pickle.load(open("word_index_%s_%d_dict.pkl"
                                           % (feature_type, threshold), "rb"))
        index_word_dict = pickle.load(open("index_word_%s_%d_dict.pkl"
                                           % (feature_type, threshold), "rb"))
    else:
        cursor.execute('select word from %s where count >= %d order by count desc'
                       % (eval("CORPUS_LEVEL_%s_COUNT_TABLE" % feature_type.upper()),
                          threshold))
        words = map (lambda x : x[0].strip().lower(), cursor.fetchall())
        num_words = len(words)
        i = 0
        word_index_dict = {}
        index_word_dict = {}
        for word in words:
            word_index_dict[word] = i
            index_word_dict[i] = word
            i+=1
        pickle.dump(index_word_dict, open("index_word_%s_%d_dict.pkl"
                                          % (feature_type, threshold), "wb"))
        pickle.dump(word_index_dict, open("words_index_%s_%d_dict.pkl"
                                          % (feature_type, threshold), "wb"))
    return (index_word_dict, word_index_dict)

def get_level_index_dict(cursor, level_type, get_zero_levels = False):
    if os.path.exists("level_index_%s_dict.pkl" % level_type):
        level_index_dict = pickle.load(open("level_index_%s_dict.pkl" % level_type, "rb"))
    else:
        #count number of distinct level in cited_paper_words_count table
        if get_zero_levels:
            cursor.execute('SELECT distinct %s FROM %s where and %s is not null'
                           % (level_type, CPID_JNL_LEV_TABLE, level_type))
        else:
            cursor.execute('SELECT distinct %s FROM %s where lev!=0 and %s is not null'
                           % (level_type, CPID_JNL_LEV_TABLE, level_type))
        levels = map (lambda x : x[0], cursor.fetchall())
        i = 0
        level_index_dict = {}
        for level in levels:
            level_index_dict[level] = i
            i+=1
        pickle.dump(level_index_dict, open("level_index_%s_dict.pkl" % level_type, "wb"))
        print len(level_index_dict)
    return level_index_dict

def get_combined_sparse_matrix(cursor, level_type, binary = False, threshold = 100):
    #coo matrix is used for constructing sparse matrix of large data
    if os.path.exists("X_word_combined_by_%s_%d%s.pkl"
                      %(level_type, threshold,
                        ("_%s" % binary if binary else ""))):
        (X, non_empty_sample)  = pickle.load(
            open("X_word_combined_by_%s_%d%s.pkl"
                %(level_type, threshold,
                  ("_%s" % binary if binary else "")), "rb"))
    else:
        (title_index_word_dict,
         title_word_index_dict) = get_index_word_dicts(cursor, 'title', threshold)
        (abstract_index_word_dict,
         abstract_word_index_dict) = get_index_word_dicts(cursor, 'abstract', threshold)
        level_index_dict = get_level_index_dict(cursor, level_type)
        #dict for tracking sparse matrix index and actual cited id
        i = 0
        print "Reading level and words..."
        #(patyoon):not sure pre-allocation is faster vs. appending.
        row = []
        col = []
        data = []
        non_empty_sample = []
        print "num samples: ", len(level_index_dict)
        print "num target title words: ", len(title_index_word_dict)
        len_title_word_dict = len(title_index_word_dict)
        print "num target abstract words: ", len(abstract_index_word_dict)
        for level in level_index_dict:
            #print level
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT word, count from '+
                           eval(level_type.upper()+
                                '_LEVEL_TITLE_COUNT_TABLE')
                           +' where '+level_type+'="' + str(level)+'"')
            word_count_dict = dict((title_word_index_dict[key],value) for key, value in
                                   dict(cursor.fetchall()).iteritems()
                                   if key in title_word_index_dict)
            cursor.execute('SELECT word, count from '+
                           eval(level_type.upper()+
                                '_LEVEL_ABSTRACT_COUNT_TABLE')
                           +' where '+level_type+'="' + str(level)+'"')
            word_count_dict.update(dict((abstract_word_index_dict[key] + len_title_word_dict,
                                         value) for key, value in
                                        dict(cursor.fetchall()).iteritems()
                                        if key in abstract_word_index_dict))
            word_count_sum = float(sum(word_count_dict.values()))
            #only take samples with more than two non-zero features
            if len(word_count_dict) >= 2:
                for word_index, count in word_count_dict.iteritems():
                    row.append(i)
                    col.append(word_index)
                    if binary:
                        data.append(1)
                    else:
                        data.append(count/word_count_sum)
                non_empty_sample.append(level)
                i+=1
        X = coo_matrix((data,(row,col)),
                       shape = (len(non_empty_sample),
                                len(abstract_word_index_dict)+len(title_word_index_dict)))
        pickle.dump((X,non_empty_sample,),
                    open("X_word_combined_by_%s_%d%s.pkl"
                         % (level_type, threshold,
                            ("_%s" % binary if binary else "")), "wb"))
        print "finished creating X sparse matrix"
    print "number of active samples: %d" % len(non_empty_sample)
    return (X.tocsr(), non_empty_sample)

def get_sparse_matrix(cursor, feature_type, level_type, binary=False,
                      threshold = 100):
        #coo matrix is used for constructing sparse matrix of large data
    if os.path.exists("X_word_combined_by_%s_%s_%d%s.pkl"
                      %(level_type, feature_type, threshold,
                        ("_%s" % binary if binary else ""))):
        (X, non_empty_sample)  = pickle.load(
            open("X_word_combined_by_%s_%s%d%s.pkl"
                 %(level_type, feature_type, threshold,
                   ("_%s" % binary if binary else "")), "rb"))
    else:
        (index_word_dict, word_index_dict) = get_index_word_dicts(cursor, feature_type)
        level_index_dict = get_level_index_dict(cursor, level_type)
        #dict for tracking sparse matrix index and actual cited id
        i = 0
        print "Reading level and words..."
        #(patyoon):not sure pre-allocation is faster vs. appending.
        row = []
        col = []
        data = []
        non_empty_sample = []
        print "num samples: ", len(level_index_dict)
        print "num target words: ", len(index_word_dict)
        for level in level_index_dict:
            #print level
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT word, count from '+ eval(level_type.upper() +
                                            '_LEVEL_'+feature_type.upper()+'_COUNT_TABLE')
                           +' where '+level_type+'="' + str(level)+'"')
            word_count_dict = dict((word_index_dict[key],value) for key, value in
                                   dict(cursor.fetchall()).iteritems()
                                   if key in word_index_dict)
            word_count_sum = float(sum(word_count_dict.values()))
            #only take samples with more than two non-zero features
            if len(word_count_dict) >= 2:
                for word_index, count in word_count_dict.iteritems():
                    row.append(i)
                    col.append(word_index)
                    if binary:
                        data.append(1)
                    else:
                        data.append(count/word_count_sum)
                non_empty_sample.append(level)
                i+=1
        print "num active samples: ", i
        X = coo_matrix((data,(row,col)),
                       shape = (len(non_empty_sample), len(word_index_dict)))
        pickle.dump((X,non_empty_sample,),
                    open("X_word_combined_by_%s_%s_%d%s.pkl"
                         %(level_type, feature_type, threshold,
                           ("_%s" % binary if binary else "")), "wb"))
        print "finished creating X sparse matrix"
    print "number of active samples: %d" % len(non_empty_sample)
    return (X.tocsr(), non_empty_sample)

def get_label_vector(cursor, non_empty_sample, feature_type, level_type,
                     threshold = 100, binary = False):
    print 'Y length ', len(non_empty_sample)
    if os.path.exists("Y_%s_%s_%d%s.pkl"
                      %(level_type, feature_type, threshold,
                        ("_%s" % binary if binary else ""))):
        Y = pickle.load(open("Y_%s_%s_%d%s.pkl"
                             %(level_type, feature_type, threshold,
                               ("_%s" % binary if binary else "")), "rb"))
    else:
        Y = lil_matrix((len(non_empty_sample),1,))
        i = 0
        for level in non_empty_sample:
            cursor.execute('SELECT distinct lev from '+ CPID_JNL_LEV_TABLE
                           + ' where '
                           +str(level_type)+'="'+str(level)+'"')
            rlevl = filter(lambda x: x[0]!=0, cursor.fetchall())
            if len(rlevl) != 1:
                print "has two rlev : ", rlevl, level
            Y[[i],[0]] = rlevl[0][0]
            i+=1
        pickle.dump(Y, open("Y_%s_%s_%d%s.pkl"
                            %(level_type, feature_type, threshold,
                              ("_%s" % binary if binary else "")), "wb"))
    return Y

def plot_feature_importance(feature_importance, feature_names):
    # make importances relative to max importance
    feature_importance = 100.0 * (feature_importance / feature_importance.max())
    sorted_idx = np.argsort(feature_importance)
    pos = np.arange(sorted_idx.shape[0]) + .5
    pl.subplot(1, 2, 2)
    pl.barh(pos, feature_importance[sorted_idx], align='center')
    pl.yticks(pos, feature_names[sorted_idx])
    pl.xlabel('Relative Importance')
    pl.title('Variable Importance')
    pl.show()

def get_class_dist(Y):
    dist = [0]*4
    for entry in Y:
        dist[int(entry)-1] += 1
    sum_dist = sum(dist)
    for i in xrange(len(dist)):
        dist[i] = float(dist[i])/sum_dist
    return np.array(dist)
