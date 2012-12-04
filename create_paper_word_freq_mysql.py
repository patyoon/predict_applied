#!/usr/bin/python2.6

from MySQLdb import connect, escape_strign
from optparse import OptionParser
import sys, re
from create_table import create_table
from multiprocessing import Pool

class WFTableGenerator:
    """Summary of class here.
    
    A class that has a generator method that yields a tuple to be inserted
    into the SQL table.

    Attributes:
        cursor: A DB cursor used to retrive terms and 
        contexts from existing table.
        termList : A list of unique terms in terms table.
        termREs : A dictionary of term as key and its compiled re object as value
        that allows basic stemming. e.g. allow, allows...
    """

    def __init__(self, db_host, db_user, db_name, 
                 db_passwd=None):
        #Connect to the database.
        self.noterm_log = open('no_term_cited_id', 'w')
        if db_passwd:
            conn = connect(host = db_host, user = db_user,
                           db = db_name, passwd = db_passwd)
        else:
            conn = connect(host = db_host, user= db_user,
                           db = db_name)
        self.cursor = conn.cursor()

        self.cursor.execute ("select distinct cited_id from citances WHERE cited_id IS NOT NULL")
        if self.cursor.rowcount == 0:
            print "No terms retrived"
            sys.exit(1)
        self.cited_id_list = map(lambda x:x[0], self.cursor.fetchall ())
        print "Number of cited id: %d" % len(self.cited_id_list)
        
    def yield_tuple(self):
        """A generator that yields a tuple to be inserted into SQL table.

        Yields a tuple to be insertd into table_name table.
        The tuple is in the form (context_id, term, cited_id, occurrences)

        Args:
        
        Yields:
           A tuple in the form (context_id, term, cited_id, occurrences)
        """
        
        for cited_id in self.cited_id_list:
            print "cited_id : ", cited_id
            self.cursor.execute ("select cited_id, sum(occurrences), term from citance_terms  " + 
                                 "where cited_id = " + str(cited_id) + " group by term;")
            if self.cursor.rowcount == 0:
                print "no terms returned for a cited_id"
                self.noterm_log.write(str(cited_id)+"\n")
            else:
                for entry in self.cursor.fetchall():
                    yield (str(entry[0]), str(entry[1]), str(entry[2]))

if __name__ == "__main__":

    usage = ("usage: %prog [options] [db_name][table_name][column_names] \n"
             "Creates term frequency table using contexts table and terms table \n\n"
             "Example: ./get_terms.py -u ungar shepard context_terms '(context_id,INT)"
             "(term,VARCHAR(50)) (cited_id,BIGINT) (occurrences,SMALLINT)'"
             )
    parser = OptionParser(usage)
    
    parser.add_option("-p", "--passwd", dest="passwd", action = "store", 
                      default=None, help="DB User password")
    parser.add_option("-o", "--hostname", dest="host", action = "store", 
                      default=None, help="DB host name")
    parser.add_option("-u", "--user", dest="user", action = "store", 
                      default=None, help="DB user name")
  
    (options, args) = parser.parse_args()
    if len(args) != 3:
        print "Need 3 arguments"
        sys.exit(1)
    #Set DB Host
    if not options.host:
        db_host = "localhost"
    else:
        db_host = options.host

    generator = WFTableGenerator(db_host, options.user, args[0], options.passwd)

    #Parse column names
    column_names = map(lambda x:tuple(x[1:-1].split(",")), args[2].split())

    num_rows_created = create_table(db_host, options.user, args[0], args[1], generator, 
                                 column_names, options.passwd, False)

    print "Number of rows created %s" % num_rows_created
    
