from MySQLdb import connect
from numpy import zeros
from operator import itemgetter
import pylab as pl
from optparse import OptionParser
from collections import defaultdict
import pickle, random, os
import numpy as np
from  sklearn.linear_model import LinearRegression
from regression_models import run_cv, run_confusion
from sklearn import metrics
from utils import get_class_dist
from scipy.sparse import coo_matrix, csr_matrix, lil_matrix
from collections import Counter
import operator

CPID_JNL_LEV_TABLE = 'MED_cpid_disc_jnl_lev_sec_title_abstr'
CPID_REFJNL_RLEV_TABLE = 'MED_cpid_refjnl_rlev_ct'

#python correlation_results.py regression_100/logit_level_cpid_feature_title.pkl regression_100/logit_level_cpid_feature_abstract.pkl regression_100/X_word_by_cpid_title.pkl regression_100/X_word_by_cpid_abstract.pkl regression_100/Y_cpid_title.pkl regression_100/Y_cpid_abstract.pkl

if __name__ == "__main__":
    usage = ("usage: %prog [options]"
             " [title_model] [abstract_model] [X_csr_title_matrix]")
    parser = OptionParser(usage)

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()
    (options, args) = parser.parse_args()
    #predict research level for them with trained model.

    model = pickle.load(open(args[0], 'rb'))[0]

    #only deals with active samples (more than two non-zero features)
    (X_title, title_active_samples) = pickle.load(open(args[2], 'rb'))
    (X_abstract, abstract_active_samples) = pickle.load(open(args[3], 'rb'))
    sample_intersection = set(title_active_samples) & set(abstract_active_samples)
    title_index = map(lambda x: title_active_samples.index(x), sample_intersection)
    X_title = X_title.tocsr()[title_index]
    Y_title = pickle.load(open(args[4], 'rb')).toarray().ravel()[title_index]
    abstract_index = map(lambda x: title_active_samples.index(x), sample_intersection)
    Y_abstract = pickle.load(open(args[5], 'rb')).toarray().ravel()[abstract_index]
    X_title = X_title.tocsr()[abstract_index]
    k = 0
    if os.path.exists("cited_journal.pkl"):
        (cited_journal_prediction,
         cited_predicted_cpids,
         actual_label) = pickle.load(open('cited_journal.pkl', 'rb'))
    else:
        actual_label = np.zeros((len(active_samples), 1,))
        cited_journal_prediction = np.zeros((len(active_samples), 4,))
        cited_predicted_cpids = []
        #retrieve paper's research levels
        j = 0
        for i in xrange(len(active_samples)):
            cursor.execute('SELECT lev from %s where cpid=%s' % (CPID_JNL_LEV_TABLE,
                                                                 active_samples[i]))
            result = cursor.fetchall()
            actual_label[i, 0] = result[0][0]
            cursor.execute('SELECT rlev from %s where cpid=%s' %(CPID_REFJNL_RLEV_TABLE,
                                                             +active_samples[i]))
            result = cursor.fetchall()
            if len(result) > 0:
                dic = defaultdict(int)
                count_sum = float()
                for rlev in result:
                    if rlev[0] > 0:
                        dic[int(rlev[0])] += 1
                    count_sum += 1
                #only pick samples with non-zero refjnl's level
                rlevl = [dic.get(lev, 0)/count_sum for lev in [1,2,3,4]]
            else:
                k+=1
                rlevl = [0,0,0,0]
            cited_journal_prediction[i, :] = np.array(rlevl)
            cited_predicted_cpids.append(i)
            if j % 100000 == 0:
                print "processed %s th cpid" %j
            j+=1

        pickle.dump((cited_journal_prediction,
                     cited_predicted_cpids,
                     actual_label), open('cited_journal.pkl', 'wb'))
    print "# jnl without refjnl ", k
    n_samples = len(cited_predicted_cpids)
    random.shuffle(cited_predicted_cpids)
    X_title, Y_title = (X_title[cited_predicted_cpids],
                        Y_title[cited_predicted_cpids])
    X_abstract, Y_abstract = (X_abstract[cited_predicted_cpids],
                              Y_abstract[cited_predicted_cpids])
    cited_journal_prediction = cited_journal_prediction[cited_predicted_cpids]
    actual_label = actual_label[cited_predicted_cpids]
    half = int(n_samples / 2)
    if os.path.exists("title_model_prediction.pkl"):
        title_model_prediction = pickle.load(open('title_model_prediction.pkl', 'rb'))
    else:
        title_model_prediction = title_model.fit(X_title[:half],
                                             Y_title[:half]).predict_proba(X_title)
        pickle.dump(title_model_prediction, open("title_model_prediction.pkl",'wb'))
    print "loaded title model prediction"
    if os.path.exists("abstract_model_prediction.pkl"):
        abstract_model_prediction = pickle.load(open('abstract_model_prediction.pkl', 'rb'))
    else:
        abstract_model_prediction = abstract_model.fit(X_abstract[:half],
                                                   Y_abstract[:half]).predict_proba(
                                                       X_abstract)
        pickle.dump(abstract_model_prediction, open("abstract_model_prediction.pkl",'wb'))
    print "loaded abstract cited journal prediction"
    del X_title
    del Y_title
    del X_abstract
    del Y_abstract
    print "loaded cited journal prediction"
    #run regression with these features and measure weight
    print type(abstract_model_prediction), type(title_model_prediction), type(cited_journal_prediction)

    X = np.concatenate((cited_journal_prediction, title_model_prediction,
                        abstract_model_prediction), axis=1)
    Y = np.array(actual_label[active_samples]).ravel()
    del cited_journal_prediction
    del actual_label
    if os.path.exists("combined_model_prediction.pkl"):
        (clf, Y_) = pickle.load(open('combined_model_prediction.pkl', 'rb'))
    else:
        clf = LinearRegression()
        print "num valid samples" , len(cited_predicted_cpids)
        sample_names = np.array(active_samples)[cited_predicted_cpids][half:]
        Y_ = clf.fit(X[:half],Y[:half]).predict(X)
        pickle.dump((clf, Y_,), open('combined_model_prediction.pkl', 'wb'))
    print "coef", clf.coef_
    print clf.score(X,Y)
    print run_cv(clf, X,Y)
    X = csr_matrix(X)
    print X.get_shape()
    print type(X)
    print Counter(Y.tolist())
    print metrics.confusion_matrix(Y[half:],
                                   map(lambda x: max(enumerate(x),
                                                     key=operator.itemgetter(1))[0]+1,
                                       abstract_model_prediction)[half:])
    print metrics.confusion_matrix(Y[half:],
                                   map(lambda x: max(enumerate(x),
                                                     key=operator.itemgetter(1))[0]+1,
                                       title_model_prediction)[half:])
    print metrics.confusion_matrix(Y[half:], map(lambda x: round(x), Y_.tolist())[half:])
