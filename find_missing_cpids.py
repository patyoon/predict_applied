
from optparse import OptionParser
import pickle
from collections import Counter
from MySQLdb import connect

#python find_missing_cpids.py regression_50/X_word_by_jnl_title.pkl regression_100/X_word_by_jnl_title.pkl regression_50/X_word_by_jnl_abstract.pkl regression_100/X_word_by_jnl_abstract.pkl

if __name__ == "__main__":
    usage = ("usage: %prog [options]"
             " [title_model_1] [abstract_model_1]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    JNL_LEVEL_ABSTRACT_COUNT_TABLE = 'journal_abstract_MED_jid_word_count'
    JNL_LEVEL_TITLE_COUNT_TABLE = 'journal_title_MED_jid_word_count'

    title_active_samples_1 = set(pickle.load(open(args[0], 'rb'))[2])
    title_active_samples_2 = set(pickle.load(open(args[1], 'rb'))[1])

    abstract_active_samples_1 = set(pickle.load(open(args[2],'rb'))[2])
    abstract_active_samples_2 = set(pickle.load(open(args[3], 'rb'))[1])
    print len(title_active_samples_2), len(abstract_active_samples_2)
    print len(title_active_samples_1), len(abstract_active_samples_1)
    title_missing_samples = title_active_samples_1 - title_active_samples_2
    abstract_missing_samples = abstract_active_samples_1 - abstract_active_samples_2
    print len(title_missing_samples)
    print len(abstract_missing_samples)
    if os.path.exists('word_counter.pkl'):
        (abstract_dict, title_dict) = pickle.load(open('word_counter.pkl','rb'))
    else:
        title_dict = Counter()
        abstract_dict = Counter()
        for jnl in title_missing_samples:
            cursor.execute('SELECT word, count from ' + JNL_LEVEL_TITLE_COUNT_TABLE +
                           ' where jnl = "%s"' % jnl)
            title_dict += Counter(dict(cursor.fetchall()))
            
        for jnl in abstract_missing_samples:
            cursor.execute('SELECT word, count from ' + JNL_LEVEL_ABSTRACT_COUNT_TABLE +
                           ' where jnl = "%s"' % jnl)
            abstract_dict += Counter(dict(cursor.fetchall()))
        pickle.dump((abstract_dict, title_dict), open('counter_dict.pkl', 'wb'))
    print "title dict: %s" % title_dict
    print "abstract dict: %s" % abstract_dict
    
    title_words_1 = set(pickle.load(open(args[4],'rb')).keys())
    title_words_2 = set(pickle.load(open(args[5], 'rb')).keys())
    abstract_words_1 = set(pickle.load(open(args[6],'rb')).keys())
    abstract_words_2 = set(pickle.load(open(args[7], 'rb')).keys())
    print len(title_words_1), len(title_words_2), len(abstract_words_1), len(abstract_words_2)
    title_missing_words = title_words_1 - title_words_2
    abstract_missing_words = abstract_words_1 - abstract_words_2
    with open('missing words', 'w') as f:
        f.write(str(title_missing_words)+"\n\n\n"+str(abstract_missing_words)+"\n")
        f.write(str(title_dict)+"\n\n\n"+str(abstract_dict)+"\n")

        
