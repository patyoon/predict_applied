import random, os, pickle
import pylab as pl
from sklearn import (cross_validation, svm,
                     metrics, linear_model)
from sklearn.svm.sparse import SVR
from sklearn.naive_bayes import MultinomialNB
from sklearn.utils import shuffle
from sklearn.feature_selection import chi2
import logging
from scipy.stats import pearsonr
from run_logit import logger

def run_cv(clf, X, Y, score_func = metrics.mean_squared_error):
    scores = cross_validation.cross_val_score(clf, X, Y, cv=10,
                                              score_func=score_func,
                                              n_jobs=-1) #use all CPUs
    return scores

def run_confusion(clf, X, Y, Y_dist, sample_names, **kwargs):
    #print X, Y
    thres = kwargs['options'].get('confus_thres', 0.01)
    random.seed(0)
    if hasattr(clf, 'get_shape') and callable(getattr(X, 'get_shape')):
        #sparse matrix
        n_samples = X.get_shape()[0]
    else:
        #normal ndarray
        n_samples = X.shape[0]
    p = range(n_samples)
    random.shuffle(p)
    X, Y = X[p], Y[p]
    half = int(n_samples / 2)
    #Run classifier
    sample_names = sample_names[half:]
    y_ = clf.fit(X[:half], Y[:half]).predict(X[half:])
    if hasattr(clf, 'predict_proba') and callable(getattr(clf, 'predict_proba')):
        try:
            y_prob= clf.predict_proba(X[half:])
            valid_idx= []
            invalid_idx = []
            invalid_name = []
            diff = y_prob-Y_dist
            #for computing confusion matrix, we only pick
            for i in xrange(len(diff)):
                diff_sum = 0
                for x in diff[i]:
                    diff_sum += abs(x)
                if diff_sum > thres:
                    valid_idx.append(i)
                else:
                    invalid_idx.append(i)
                    invalid_name.append(sample_names[i])
                    print "too close", y_prob[i], "\nversus\n", Y_dist
            print "valid ", len(valid_idx), " invalid", len(invalid_idx)
            for name in invalid_name:
                logger.info("samples with class distribution: %s" % name)
        except NotImplementedError:
            valid_idx = range(len(y_))
    else:
        valid_idx = range(len(y_))
    #Compute confusion matrix
    Y_half = Y[half:]
    corr = pearsonr(Y_half[valid_idx], y_[valid_idx])
    print "Pearson correlation %f" %corr[0]
    cm = metrics.confusion_matrix(Y_half[valid_idx], y_[valid_idx])
    print cm
    logger.info('confusion matrix : %s' % str(cm))
    return (corr, cm, y_prob, sample_names)

def dict_to_str(kwargs):
    string = ""
    for k,v in kwargs.iteritems():
        if type(v) in [str, int, float]:
            string += ("_%s_%s" % (k,v))
        if hasattr(v, '__call__'):
            string += ("_%s_%s" % (k, v.__name__))
        if type(v) == dict:
            string += dict_to_str(v)
    return string

def build_pickle_name(func, *args, **kwargs):
    names = [func.__name__]
    for var in args:
        if type(var) in [str, int, float]:
            names.append("%s_%s" % (locals()[var], str(var)))
    names.append(dict_to_str(kwargs))
    return "_".join(names)

#pickle decorator
def pickler(func):
    def inner_pickler(*args, **kwargs):
        name = build_pickle_name(func, *args, **kwargs)
        print "name ", name
        logger.info('name: %s' % name)
        if os.path.exists(name+".pkl"):
            tup = pickle.load(open(name+".pkl", 'rb'))
            clf = tup[0]
            scores = tup[1]
            corr = tup[2]
            cm = tup[3]
            y_prob = tup[4]
            sample_names = tup[5]
        else:
            clf = func(**kwargs)
            logger.info('running confusion matrix')
            (corr, cm, y_prob, sample_names) = run_confusion(clf, *args, **kwargs)
            logger.info('running cross validation')
            scores = run_cv(clf, *args[:2])
            if kwargs['options'].get('train', True):
                clf.fit(*args[:2])
            pickle.dump((clf, scores,corr, cm,
                         y_prob, sample_names), open(name+".pkl", 'wb'))
        logger.info('scores. %s' % str(scores))
        return (name, scores, corr, cm, y_prob, sample_names)
    return inner_pickler

@pickler
def DT(X, Y):
    return DecisionTreeClassifier(random_state=0)

@pickler
def multinomial_NB(X, Y):
    return MultinomialNB()

@pickler
#parameter C controls the sparsity: the smaller C the fewer features selected.
def logit(**kwargs):
    penalty = kwargs['options'].get('penalty', 'l2')
    #print penalty
    C = kwargs['options'].get('C', 1e5)
    return linear_model.LogisticRegression(penalty = 'l2', C=C)

@pickler
def ridge(alpha=1):
    clf = linear_model.Ridge(alpha=alpha)
    return clf

@pickler
#sparse_lasso
def lasso(**kwargs):
    alpha = kwargs['options'].get('alpha', 0.1)
    clf = linear_model.Lasso(alpha=alpha, fit_intercept=True, max_iter=10000)
    return clf

@pickler
def sgd_regressor(**kwargs):
    clf = linear_model.SGDRegressor()
    return clf

@pickler
def sgd_classifier(**kwargs):
    clf = linear_model.SGDClassifier()
    return clf

def calc_chi2(X, Y, feature_type, level_type):
    name = "chi2_%s_%s" % (feature_type, level_type)
    if os.path.exists(name+".pkl"):
        return pickle.load(open(name+".pkl", 'rb'))
    else:
        res = chi2(X, Y)
        pickle.dump(res, open(name+".pkl", 'wb'))
        return res
