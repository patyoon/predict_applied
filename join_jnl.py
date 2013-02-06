from MySQLdb import connect
from numpy import zeros
from operator import itemgetter
import pylab as pl
from optparse import OptionParser
from collections import defaultdict

CPID_JNL_LEV_TABLE = 'MED_cpid_disc_jnl_lev_sec_title_abstr'
CPID_REFJNL_RLEV_TABLE = 'MED_cpid_refjnl_rlev_ct'

if __name__ == "__main__":
    usage = ("usage: %prog [options]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()
    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    cursor.execute('SELECT distinct cpid, lev from %s where lev != 0' % CPID_JNL_LEV_TABLE)
    cpids = cursor.fetchall()
    mat = zeros((4, 4))
    for cpid, lev in cpids:
        if lev == 0:
            continue
        else:
            cursor.execute('SELECT rlev from %s'+ CPID_REFJNL_RLEV_TABLE +
                           ' where cpid='+str(cpid))
            result = cursor.fetchall()
            if len(result) > 0:
                #print result
                #print "result > 0"
                dic = defaultdict(int)
                for rlev in result:
                    if rlev[0] > 0:
                        dic[int(rlev[0])] +=1
                #print dic
                if len(dic) > 0:
                    rlevl = max(dic.items(), key = itemgetter(1) )
                    mat[int(lev-1), int(rlev[0]-1)] += 1
    print mat
    pl.matshow(mat)
    pl.title('cited refjnl rlev vs jnl lev matrix')
    pl.colorbar()
    pl.savefig('cited refjnl rlev vs jnl lev matrix')
    pl.figure()
