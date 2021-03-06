from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys
import pickle, os
from math import sqrt
from scipy.sparse import lil_matrix, spdiags, csr_matrix
from numpy import zeros, array, matrix
from sklearn import (cross_validation, svm,
                     metrics, linear_model)
from sklearn.svm.sparse import SVR
from sklearn.naive_bayes import MultinomialNB
from sklearn.utils import shuffle
import pylab as pl
import numpy as np
#from sklearn import ensemble
import datetime, random
from collections import Counter
from scipy.stats import pearsonr

JNL_LEVEL_ABSTRACT_COUNT_TABLE = 'journal_abstract_MED_jid_word_count'
JNL_LEVEL_TITLE_COUNT_TABLE = 'journal_title_MED_jid_word_count'
CPID_LEVEL_ABSTRACT_COUNT_TABLE = 'cpid_abstract_MED_jid_word_count'
CPID_LEVEL_TITLE_COUNT_TABLE = 'cpid_title_MED_jid_word_count'
CORPUS_LEVEL_ABSTRACT_COUNT_TABLE = 'all_word_abstract_MED_word_count'
CORPUS_LEVEL_TITLE_COUNT_TABLE = 'all_word_title_MED_word_count'

def run_cv(clf, X, Y):
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_squared_error)
    return scores

def run_confusion(clf, name, X, Y, Y_dist, sample_names, outfile, thres=0.01):
    random.seed(0)
    n_samples = X.get_shape()[0]
    p = range(n_samples)
    random.shuffle(p)
    X, Y = X[p], Y[p]
    half = int(n_samples / 2)
    #Run classifier
    sample_names = map (lambda x:sample_names[x], p)[half:]
    y_ = clf.fit(X[:half], Y[:half]).predict(X[half:])
    y_prob= clf.predict_proba(X[half:])
    valid_idx= []
    invalid_idx = []
    invalid_name = []
    diff = y_prob-Y_dist
    for i in xrange(len(diff)):
        diff_sum = 0
        for x in diff[i]:
            diff_sum += abs(x)
        if diff_sum > thres:
            valid_idx.append(i)
        else:
            invalid_idx.append(i)
            invalid_name.append(sample_names[i])
            print "too close"
    print len(valid_idx), len(invalid_idx)
    outfile.write("invalid\n")
    for name in invalid_name:
        outfile.write(name+'\n')

    #Compute confusion matrix
    Y_half = Y[half:]
    print "Pearson correlation %f", pearsonr(Y_half[valid_idx], y_[valid_idx])
    cm = metrics.confusion_matrix(Y_half[valid_idx], y_[valid_idx])
    outfile.write(cm)
    print cm
    #Show confusion matrix
    # pl.matshow(cm)
    # pl.title('Confusion matrix')
    # pl.colorbar()
    # pl.show()
    #pl.savefig(name+'png')
    #pl.figure()

#pickle decorator
def pickler(func):
    def inner_pickler(*args, **kwargs):
        print args[2], kwargs
        name = func.__name__+"_"+'_'.join(map(lambda x: "_".join(x), kwargs.items()))
        if os.path.exists(name+".pkl"):
            tup = pickle.load(open(name+".pkl", 'rb'))
            clf = tup[0]
            scores = tup[1]
        else:
            clf = func(*args[:2])
            scores = run_cv(clf, *args[:2])
            clf.fit(*args[:2])
            pickle.dump((clf, scores,), open(name+".pkl", 'wb'))
        run_confusion(clf, name, *args[:5])
        return (scores.mean(), scores.std(),)
    return inner_pickler

@pickler
def DT(X, Y):
    return DecisionTreeClassifier(random_state=0)

@pickler
def multinomial_NB(X, Y):
    return MultinomialNB()

@pickler
def svr(X, Y, kernel, param=None, C=1e3):
    if kernel in ['rbf', 'poly']:
        clf = SVR(kernel, C, param)
    else:
        clf = SVR(kernel, C)
    return clf

@pickler
def logit(X, Y, penalty = 'l1', C=1e5):
    return linear_model.LogisticRegression(penalty = penalty, C=C)

@pickler
def ridge(X, Y, alpha=1):
    clf = linear_model.Ridge(alpha=1.0)
    return clf

@pickler
def sgd_regressor(X,Y):
    clf = linear_model.SGDRegressor()
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_squared_error)
    clf.fit(X,Y)
    return clf

def get_index_word_dicts(cursor, feature_type, threshold=100):
    if os.path.exists("word_index_"+feature_type+"_dict.pkl"):
        word_index_dict = pickle.load(open("word_index_"+feature_type+"_dict.pkl", "rb"))
        index_word_dict = pickle.load(open("index_word_"+feature_type+"dict.pkl", "rb"))
    else:
        cursor.execute('select word from '+ eval("CORPUS_LEVEL_"+feature_type.upper()+"_COUNT_TABLE") +
                       ' where count >= '+str(threshold)+' order by count desc')
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
        cursor.execute('SELECT distinct '+level_type+' FROM MED_cpid_refjnl_rlev_ct where rlev!=0 and ' + level_type+' is not null')
        levels = map (lambda x : x[0], cursor.fetchall())
        i = 0
        level_index_dict = {}
        for level in levels:
            level_index_dict[level] = i
            i+=1
        pickle.dump(level_index_dict, open("level_index_"+feature_type+"_"+level_type+"_dict.pkl", "wb"))
        print len(level_index_dict)
    return level_index_dict

def get_class_dist(Y):
    dist = [0]*4
    for entry in Y:
        dist[entry-1] += 1
    sum_dist = sum(dist)
    for i in xrange(len(dist)):
        dist[i] = float(dist[i])/sum_dist
    return np.array(dist)

def get_sparse_matrix(cursor, feature_type, level_type):
        #lil sparse matrix in scipy package:
        #http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.lil_matrix.html
        #matrix is (num_level) * num_words+1 size (+1 for the last column)
    if os.path.exists("X_word_by_"+level_type+"_"+feature_type+".pkl"):
        (X, non_empty_sample_index, non_empty_sample)  = pickle.load(open("X_word_by_"+level_type+"_"+feature_type+".pkl", "rb"))
    else:
        (index_word_dict, word_index_dict) = get_index_word_dicts(cursor, feature_type)
        level_index_dict = get_level_index_dict(cursor, feature_type, level_type)
        #dict for tracking sparse matrix index and actual cited id
        i = 0
        print "Reading level and words..."
        level_dict = {}
        X = lil_matrix((len(level_index_dict),len(word_index_dict),))
        print X.get_shape()
        non_empty_sample = []
        non_empty_sample_index = []
        for level in level_index_dict:
            #print level
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT word, count from '+ eval(level_type.upper() + '_LEVEL_'+feature_type.upper()+'_COUNT_TABLE')
                           +' where '+level_type+'="' + str(level)+'"')
            word_count_dict = dict((word_index_dict[key],value) for key, value in
                                   dict(cursor.fetchall()).iteritems()
                                   if key in word_index_dict)
            word_count_sum = float(sum(word_count_dict.values()))
            if len(word_count_dict) >= 2:
                X[[level_index_dict[level]],
                  word_count_dict.keys()] =  map(lambda x: x/word_count_sum, word_count_dict.values())
                i+=1
                non_empty_sample_index.append(level_index_dict[level])
                non_empty_sample.append(level)
        X = X[non_empty_sample_index,:]
        pickle.dump((X,non_empty_sample_index,non_empty_sample),
                    open("X_word_by_"+level_type+"_"+feature_type+".pkl", "wb"))
        print "finished inserting count into sparse matrix"
        #row standardize X as distribution
    return (X, non_empty_sample_index)

def get_label_vector(cursor, feature_type, level_type):
    if os.path.exists("Y_"+level_type+".pkl"):
        Y = pickle.load(open("Y_"+level_type+".pkl", "rb"))
    else:
        level_index_dict = get_level_index_dict(cursor, feature_type, level_type)
        Y = lil_matrix((len(level_index_dict),1,))
        i = 0
        for level in level_index_dict:
            cursor.execute('SELECT distinct rlev from MED_cpid_refjnl_rlev_ct where '
                           +str(level_type)+'="'+str(level)+'"')
            rlevl = filter(lambda x: x[0]!=0, cursor.fetchall())
            if len(rlevl) != 1:
                print "two rlevel : ", rlevl, level
            Y[[level_index_dict[level]],[0]] = rlevl[0][0]
        pickle.dump(Y, open("Y_" + level_type + ".pkl", "wb"))
    return Y

def run_all_models(X, Y, Y_dist, level_names, feature_names, outfile, feature_param, level_param):
    try:
        XS, YS = shuffle(X, Y, random_state=13)
        params = {'feature_param':feature_param, 'level_param':level_param}
        #2. Run Cross Validation with SVR
        #2.1 Linear Kernel
        #(mean, std) = svr(XS, YS, kernel='linear')
        #outfile.write( "Linear SVR MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        #2.2 RBF Kernel with gamma 0.1
        #(mean, std) = svr(XS, YS, kernel='rbf', gamma=0.1)
        #outfile.write( "SVR RBF Kernel MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        #2.2 Polynomial Kernel with degree 2
        #(mean, std) = svr(XS, YS, kernel='poly', degree=2)
        #outfile.write( "SVR Poly Kernel MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        outfile.write(feature_param +"\t"+level_param+"\n")
        outfile.write("num samples :"+str(X.get_shape()[0])+
                      "\tnum features :"+str(X.get_shape()[1])+"\n")
        #3 Run Cross Validation with Logit
        (mean, std) = logit(XS, YS, Y_dist, level_names, outfile, "logit", **params)
        outfile.write( ("Logit MSE Score: %0.2f (+/- %0.2f)\n"
               % (mean, std/2)))
        # Run multinomial NB
        #(mean, std) = multinomial_NB(XS, YS, "nb", **params)
        #outfile.write( ("Multinomial NB MSE Score: %0.2f (+/- %0.2f)\n"
        #       % (mean, std/2)))
        # Run SGDRegressor
        #(mean, std) = sgd_regressor(XS,YS, "SGD", **params)
        #outfile.write( ("SGD Regressor MSE Score: %0.2f (+/- %0.2f)\n"
        #       % (mean, std/2)))
        #outfile.write( "Decision Tree MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        # Run gradient boosting
        #(mean, std, feature_importances) = gradient_boosting(XS, YS, feature_names)
        #outfile.write( "Gradient Boosting MSE Score: %0.2f (+/- %0.2f)" % (sqrt(mean), std/2)
        #plot_feature_importance(feature_importances, feature_names)
    except IndexError as e:
        outfile.write( e +'\n')

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

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name]")

    parser = OptionParser(usage)

    (options, args) = parser.parse_args()
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    now = datetime.datetime.now()
    f = open("result-"+now.strftime("%Y-%m-%d-%H:%M"), 'a')
    abstract_word_index =  get_index_word_dicts(cursor, 'abstract')[0]
    title_word_index = get_index_word_dicts(cursor, 'title')[0]
    index_jnl_dict =  {v:k for k, v in get_level_index_dict(cursor, 'title', 'jnl').items()}
    index_cpid_dict=  {v:k for k, v in get_level_index_dict(cursor, 'title', 'cpid').items()}
    (X1, non_empty) = get_sparse_matrix(cursor, 'abstract', 'jnl')

    Y1 = get_label_vector(cursor, 'title', 'jnl').toarray().ravel()
    Y1_dist = get_class_dist(Y1)
    run_all_models(X1, Y1[non_empty], Y1_dist, index_jnl_dict,
                   abstract_word_index, f, 'abstract', 'jnl')
    X1 = None
    (X2, non_empty) = get_sparse_matrix(cursor, 'title', 'jnl')

    run_all_models(X2, Y1[non_empty], Y1_dist,  index_jnl_dict,
                   title_word_index, f, 'title', 'jnl')
    X2 = None
    Y1 = None
    # (X3, non_empty) = get_sparse_matrix(cursor, 'title', 'cpid')
    # Y2 = get_label_vector(cursor, 'title', 'cpid').toarray().ravel()
    # Y2_dist = get_class_dist(Y2)
    # run_all_models(X3, Y2[non_empty], Y2_dist,  index_cpid_dict,
    #                title_word_index, f, 'title', 'cpid')
    # X3 = None
    # (X4, non_empty) = get_sparse_matrix(cursor, 'abstract', 'cpid')
    # run_all_models(X4, Y2[non_empty], Y2_dist,  index_cpid_dict,
    #                abstract_word_index, f, 'abstract', 'cpid')
