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

#pickle decorator
def pickler(func):
    def inner_pickler(*args, **kwargs):
        name = func.__name__+"_"+'_'.join(map(lambda x: "_".join(x), kwargs.items()))
        if os.path.exists(name):
            tup = pickle.load(open(name, 'rb'))
        else:
            tup = func(*args, **kwargs)
            pickle.dump(tup, open(name, 'wb'))
        return tup[1:]
    return inner_pickler

@pickler
def DT(X, Y):
    clf = DecisionTreeClassifier(random_state=0)
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_square_error)
    return (clf, scores.mean(), scores.std())

@pickler    
def multinomial_NB(X, Y):
    clf = MultinomialNB()
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_square_error)
    return (clf, scores.mean(), scores.std())

@pickler
def svr(X, Y, kernel, param=None, C=1e3):
    if kernel in ['rbf', 'poly']:
        clf = SVR(kernel, C, param)
    else:
        clf = SVR(kernel, C)
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_square_error)
    return (clf, scores.mean(), scores.std())

@pickler
def logit(X, Y, penalty = 'l2', C=1e5):
    clf = linear_model.LogisticRegression(penalty = 'l2', C=C)
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_square_error)
    return (clf, scores.mean(), scores.std())

@pickler
def ridge(X, Y, alpha=1):
    clf = linear_model.Ridge(alpha=1.0)
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_square_error)
    return (clf, scores.mean(), scores.std())

@pickler
def gradient_boosting(X,Y):
    params = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 1,
          'learn_rate': 0.01, 'loss': 'ls'}
    clf = ensemble.GradientBoostingClassifier(**params)
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=metrics.mean_square_error)
    clf.fit(X,Y)
    return (clf, scores.mean(), scores.std(), clf.feature_importances_)
    
JNL_LEVEL_ABSTRACT_COUNT_TABLE = 'journal_abstract_MED_jid_word_count'
JNL_LEVEL_TITLE_COUNT_TABLE = 'journal_title_MED_jid_word_count'
CPID_LEVEL_ABSTRACT_COUNT_TABLE = 'cpid_abstract_MED_jid_word_count'
CPID_LEVEL_TITLE_COUNT_TABLE = 'cpid_title_MED_jid_word_count'
CORPUS_LEVEL_ABSTRACT_COUNT_TABLE = 'all_word_abstract_MED_word_count'
CORPUS_LEVEL_TITLE_COUNT_TABLE = 'all_word_title_MED_word_count'

def get_index_word_dicts(cursor, feature_type, threshold=50):
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

def get_sparse_matrix(cursor, feature_type, level_type):
        #lil sparse matrix in scipy package:
        #http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.lil_matrix.html
        #matrix is (num_level) * num_words+1 size (+1 for the last column)
    if os.path.exists("X_word_by_"+level_type+"_"+feature_type+".pkl"):
        X = pickle.load(open("X_word_by_"+level_type+"_"+feature_type+".pkl", "rb"))
    else:
        (index_word_dict, word_index_dict) = get_index_word_dicts(cursor, feature_type)
        level_index_dict = get_level_index_dict(cursor, feature_type, level_type)        
        #print X.get_shape()
        #dict for tracking sparse matrix index and actual cited id
        i = 0
        print "Reading level and words..."
        level_dict = {}
        X = lil_matrix((len(level_index_dict),len(word_index_dict),))
        for level in level_index_dict:
            #print level
            if i%10000 == 0:
                print i, "th insert"
            cursor.execute('SELECT word, count from '+ eval(level_type.upper() + '_LEVEL_'+feature_type.upper()+'_COUNT_TABLE')
                           +' where '+level_type+'="' + str(level) +'" && count >= 50 ')
            word_count_dict = dict((word_index_dict[key],value) for key, value in 
                                   dict(cursor.fetchall()).iteritems() 
                                   if key in word_index_dict)
            if len(word_count_dict) > 0:
                X[[level_index_dict[level]], word_count_dict.keys()] =  word_count_dict.values()
            i+=1
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
                           +level_type+'="'+level+'"')
            rlevl = filter(lambda x: x[0]!=0, cursor.fetchall())
            if len(rlevl) != 1:
                print "two rlevel : ", rlevl, level
            Y[[level_index_dict[level]],[0]] = rlevl[0][0] 
        Y = pickle.dump(Y, open("Y_" + level_type + ".pkl", "wb"))
    return Y

def run_all_models(X, Y, feature_names):
    try:
        print type(X)
        #print X
        #print len(Y[0])
        #print X.get_shape()
        #print Y.get_shape()
        XS, YS = shuffle(X, Y, random_state=13)
        #2. Run Cross Validation with SVR
        #2.1 Linear Kernel
        #(mean, std) = svr(XS, YS, kernel='linear')
        #print "Linear SVR MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        #2.2 RBF Kernel with gamma 0.1
        #(mean, std) = svr(XS, YS, kernel='rbf', gamma=0.1)
        #print "SVR RBF Kernel MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        #2.2 Polynomial Kernel with degree 2
        #(mean, std) = svr(XS, YS, kernel='poly', degree=2)
        #print "SVR Poly Kernel MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        #3 Run Cross Validation with Logit
        (mean, std) = logit(XS, YS)
        print ("Logit without PCA MSE Score: %0.2f (+/- %0.2f)"
               % (mean, std/2))
        # Run multinomial NB
        (mean, std) = multinomial_NB(XS, YS)
        print ("Multinomial NB MSE Score without PCA: %0.2f (+/- %0.2f)"
               % (mean, std/2))
        # Run decision tree
        #(mean, std) = decision_tree(XS, YS)
        #print "Decision Tree MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)
        # Run gradient boosting
        (mean, std, feature_importances) = gradient_boosting(XS[YS!=0], YS[YS!=0])
        print "Gradient Boosting MSE Score: %0.2f (+/- %0.2f)" % (mean, std/2)        
        plot_feature_importance(feature_importances, feature_names)
    except IndexError as e:
        print e
    finally:
        print ""

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

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name]")

    parser = OptionParser(usage)

    (options, args) = parser.parse_args()
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()
    X1 = get_sparse_matrix(cursor, 'abstract', 'jnl')
    X2 = get_sparse_matrix(cursor, 'title', 'jnl')
    Y1 = get_label_vector(cursor, 'title', 'jnl')
    
    #X3 = get_sparse_matrix(cursor, 'title', 'cpid')
    #X4 = get_sparse_matrix(cursor, 'abstract', 'cpid')
    #Y2 = get_label_vector(cursor, 'title', 'cpid')

    #convert to dense matrix
    X1csr = X1.tocsr()
    X2csr = X2.tocsr()
    Y1csr = Y1.toarray().ravel()
    #1.1. Journal level
    run_all_models(X1csr, Y1csr, get_index_word_dicts(cursor, 'abstract')[0])
    #run_all_models(X2d, Y1d, get_index_word_dicts(cursor, 'title')[0])
