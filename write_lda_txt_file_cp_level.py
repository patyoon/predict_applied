#!/usr/bin/python2.7
from MySQLdb import connect, escape_string
from optparse import OptionParser

if __name__ == "__main__":
    usage = ("usage: %prog [options] [word_group_name] [out_file_name]"
             " Run logistic regression with logistic_regression method."
             "'cited_paper_terms_count_sample_4m'"
                 )
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()

    out_file = open(args[1], 'w')

    conn = connect(host = 'localhost', user = 'root',
                   db = 'shepard', passwd='shepard')
    cursor = conn.cursor()

    cursor.execute('SELECT distinct cited_id FROM cited_paper_level_'+ args[0])
    cited_ids = map (lambda x : x[0], cursor.fetchall())

    i = 0
    for cited_id in cited_ids:
        i+=1
        if i % 100000 == 0:
            print i, "th processed"
        cursor.execute('SELECT word, count from cited_paper_level_'+ args[0] + ' WHERE cited_id='+str(cited_id))
        query_result = cursor.fetchall()
        lines = map (lambda query_record: " ".join([query_record[0]] * query_record[1]), query_result)
        out_file.write(" ".join(lines)+'\n')      
    conn.close()
    out_file_close()
