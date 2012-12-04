from MySQLdb import connect
import sys
from collections import defaultdict

# edits_filename must contain entries that are tab-delimited and
# in the order of cited_id ncit07 ncit

if __name__ == "__main__":

    conn = connect(host='localhost',user='root',passwd='shepard',db='shepard')
    cursor = conn.cursor()
    term_table_name = sys.argv[1]
    cursor.execute('select term, term_id from '+term_table_name)
    
    term_dict = defaultdict(list)

    res = cursor.fetchall()

    for item in res:
        term_dict[item[0]].append(item[1])
        print term_dict[item[0]]
    i = 0
    for key, value in term_dict.items():
        for term_id in value:
            cursor.execute('update '+term_table_name+' set uterm_id='+str(i) + ' where term_id=' + str(term_id))
            print 'updated '
        i+=1
    conn.close()
