#!/usr/bin/python2.6

from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys, re
from create_table import create_table

def correctNone(x):
    """
    A helper method for converting None and Int type value to String.
    
    A helper method that corrects None input to string '\n' 
    so that we correctly insert NULL in SQL Table. It also returns str() of 
    any non-String values.
    
    Args:
        x : a table entry returned from fetchall() or fetchone() method.
    
    Returns:
        returns '\N' for None value, str(x) for non-String value, 
        or x otherwise.
    
    Raises:
        None
    """
    if x == None:
         return '\N'
    elif type(x) != str:
         return str(x)
    else: 
         return x
    
def uniquify(seq, idfun=None):
    """A helper method for uniquifying a collection.
     
    Args:
        seq : an iterable collection (list, tuple, etc)
        idfun : a functio that returns an object to compare with.
    
    Returns:
        A list of unique elements in the original collection.
    
    Raises:
        None
    """
    if idfun is None:
          def idfun(x): return x
    seen = {}
    result = []

    for item in seq:
        marker = idfun(item)
        if marker not in seen:
            seen[marker] = 1
            result.append(item)
    return result

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
        if db_passwd:
            conn = connect(host = db_host, user = db_user,
                           db = db_name, passwd = db_passwd)
        else:
            conn = connect(host = db_host, user= db_user,
                           db = db_name)
        self.cursor = conn.cursor()
           

    def yield_tuple(self):
        """A generator that yields a tuple to be inserted into SQL table.

        Yields a tuple to be insertd into table_name table.
        The tuple is in the form (context_id, term, cited_id, occurrences)

        Args:
        
        Yields:
           A tuple in the form (citance_id, word, occurrences)
        """
        
        self.cursor.execute("SELECT count(*) from citances")
        num_rows = self.cursor.fetchone()[0]

        # Decided to fetch contexts table one row by one because fetchall()
        # caused "out of memory" error
	print num_rows
        for x in xrange(num_rows):
            self.cursor.execute ("SELECT citance_id, sentences FROM citances WHERE citance_id = " + str(x+1))
            row = self.cursor.fetchone()
            if row == None:
		print "none"
                raise StopIteration

            row = map(correctNone, row)
            
            #TODO(patrick) : match against words more sophisticatedly
            pat = re.compile(r"\W")
            word_list = map(lambda word:pat.sub('',word), row[1].split())
            word_set = set()
            for word in word_list:
                if word not in word_set:
                    num_matches = word_list.count(word)
                    word_set.add(word)
                    #Tuple is (context.Id, term.term, 
                    #number of matches)
                    yield (row[0], word, str(num_matches))

if __name__ == "__main__":

    usage = """usage: %prog [options] [db_name] [table_name][column_names]
    Creates all words frequency table in citance level. Uses text file.

    <citance_all_count>
    Table that contains counts of all words in three-sentences in citance level
    - citance_id (int) = ID of citance
    - word (char(50)) = word string
    - counts (int) = number of occurrences of this word in this citance

    Example: ./create_table_all_words_citance_level.py -u ungar shepard citance_all_count '(citance_id,INT) (word,char(50)) (cited_it,BIGINT) (counts,int)'"""
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
    
