from MySQLdb import connect
from numpy import zeros
from operator import itemgetter
import pylab as pl
from optparse import OptionParser
from collections import defaultdict
import pickle, random, os
import numpy as np
from  sklearn.linear_model import LinearRegression, LogisticRegression
from regression_models import run_cv, run_confusion, logit
from sklearn import metrics
from utils import get_class_dist, get_level_index_dict
from scipy.sparse import coo_matrix, csr_matrix, lil_matrix
from collections import Counter
import operator
from scipy.stats import pearsonr

CPID_JNL_LEV_TABLE = 'MED_cpid_disc_jnl_lev_sec_title_abstr'
CPID_REFJNL_RLEV_TABLE = 'MED_cpid_refjnl_rlev_ct'

#python correlation_results.py regression_100/logit_level_cpid_feature_title.pkl regression_100/logit_level_cpid_feature_abstract.pkl regression_100/X_word_by_cpid_title.pkl regression_100/X_word_by_cpid_abstract.pkl regression_100/Y_cpid_title.pkl regression_100/Y_cpid_abstract.pkl

if __name__ == "__main__":
    usage = ("usage: %prog [options]"
             " [X_input] [abstract_model] [X_csr_title_matrix]")
    parser = OptionParser(usage)

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()
    (options, args) = parser.parse_args()
    #predict research level for them with trained model.

    #model = pickle.load(open(args[0], 'rb'))[0]
    
    (X_combined, active_samples) = pickle.load(open(args[0], 'rb'))
    print len(active_samples)
    Y_combined = pickle.load(open(args[1], 'rb'))
    all_papers = get_level_index_dict(cursor, 'cpid', True)
    inactive_samples = list(set(all_papers.keys()) - set(active_samples))    
    num_active_samples = len(active_samples)
    num_inactive_samples = len(inactive_samples)
    k = 0
    if os.path.exists(args[2]):
        (cited_journal_prediction,
         cited_predicted_cpids,
         actual_label) = pickle.load(open(args[2], 'rb'))
    else:
        actual_label = np.zeros((len(all_papers), 1,))
        cited_journal_prediction = np.zeros((len(all_papers), 4,))
        cited_predicted_cpids = []
        #retrieve paper's research levels
        j = 0
        for i in xrange(len(all_papers)):
            if i < len(active_samples):
                sample = active_samples[i]
            else:
                sample = inactive_samples[i - len(active_samples)]
            cursor.execute('SELECT lev from %s where cpid=%s' % (CPID_JNL_LEV_TABLE,
                                                                 sample))
            result = cursor.fetchall()
            actual_label[i, 0] = result[0][0]
            cursor.execute('SELECT rlev from %s where cpid=%s' %(CPID_REFJNL_RLEV_TABLE,
                                                             + sample))
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
            cited_predicted_cpids.append(sample)
            if j % 100000 == 0:
                print "processed %s th cpid" %j
            j+=1
        pickle.dump((cited_journal_prediction,
                     cited_predicted_cpids,
                     actual_label), open(args[2], 'wb'))
    print "# jnl without refjnl ", k
    
    random.seed(0)
    p = range(num_active_samples)
    random.shuffle(p)
    print len(p)
    print len(Y_combined.toarray())
    print len(cited_predicted_cpids)
    (X_combined, Y_combined) = (X_combined.tocsr()[p],
                                Y_combined.toarray().ravel()[p])
    cited_journal_prediction[:num_active_samples,:] = cited_journal_prediction[p]
    actual_label[:num_active_samples] = actual_label[p]
    cited_predicted_cpids = np.array(cited_predicted_cpids)
    cited_predicted_cpids[:num_active_samples] = cited_predicted_cpids[p]
    
    half = int(num_active_samples / 2)
    if os.path.exists(args[3]):
        model_prediction = pickle.load(open(args[3], 'rb'))
    else:
        model = LogisticRegression(penalty = 'l1', C=1e5)
        prediction = model.fit(X_combined[:half],
                                    Y_combined[:half]).predict_proba(X_combined)
        model_prediction = np.zeros((len(all_papers), 4,))
        print prediction.shape
        print model_prediction.shape
        model_prediction = np.concatenate(
(prediction,  np.array([get_class_dist(Y_combined),]*num_inactive_samples)),axis=0)
        pickle.dump(model_prediction, open(args[3],'wb'))
    if not os.path.exists(args[6]):
        model.fit(X_combined, Y_combined)
        pickle.dump(model, open(args[6], 'wb'))
    
    print "loaded cited journal prediction"
    #run regression with these features and measure weight

    X_full = np.concatenate((model_prediction,
                        cited_journal_prediction), axis=1)
    Y_full = np.array(actual_label).ravel()
    if os.path.exists(args[4]):
        (clf, Y_, Y_proba) = pickle.load(open(args[4], 'rb'))
    else:
        #clf = LinearRegression()
        clf = LogisticRegression()
        X_full = csr_matrix(X_full)        
        Y_ = clf.fit(X_full[:half],Y_full[:half]).predict(X_full)
        Y_proba = clf.predict_proba(X_full)
        pickle.dump((clf, Y_, Y_proba), open(args[4], 'wb'))
    if not os.path.exists(args[5]):
        clf.fit(X_full[:num_active_samples],Y_full[:num_active_samples])
        pickle.dump(clf, open(args[5], 'wb'))
    print "coef", clf.coef_
    print clf.score(X_full, Y_full)
    Y_combined_ = map(lambda x: max(enumerate(x),
                                    key=operator.itemgetter(1))[0]+1,
                                    model_prediction)
    print "Pearson correlation combined %f" %pearsonr(Y_full, Y_combined_)[0]
    print "Pearson correlation full %f" %pearsonr(Y_full, Y_)[0]
    
    # print run_cv(clf, model_prediction, Y_full)
    # print run_cv(clf, X_full, Y_full)
    
    # print metrics.confusion_matrix(Y_full[half:], Y_combined_[half:])
    # print metrics.confusion_matrix(Y_full[half:], map(lambda x: round(x),
    #                                                   Y_.tolist())[half:])
    # print metrics.confusion_matrix(Y_full, Y_combined_)
    # print metrics.confusion_matrix(Y_full, map(lambda x: round(x),
    #                                                   Y_.tolist()))
    print cited_predicted_cpids.shape
    print Y_proba.shape
    with open(args[7], 'w') as f:
        for i in xrange(len(Y_)):
            f.write("%d\t%f\t%f\t%f\t%f\n" %( cited_predicted_cpids[i],
                    Y_proba[i][0], Y_proba[i][1], Y_proba[i][2], Y_proba[i][3]))
    
    # ipython correlation_results.py X_word_combined_by_cpid_100.pkl Y_cpid_combined_100.pkl cited_journal_pred_combined_100.pkl model_pred_combined_100.pkl full_pred_combined_100.pkl

    #ipython correlation_results.py X_wordbined_by_cpid_100_True.pkl Y_cpid_combined_100_True.pkl cited_journal_pred_combined_100_True.pkl combined_pred_combined_100_True.pkl full_pred_combined_100_True.pkl full_pred_combined_100_True_model.pkl combined_pred_combined_100_True_model.pkl Y_proba_pred_combiend_100_True.pkl
