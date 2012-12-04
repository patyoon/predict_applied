#!/usr/bin/python2.6

from MySQLdb import connect, escape_string
from optparse import OptionParser
import sys, re

def create_table(db_host, db_user, db_name, table_name, generator, 
                 column_names, db_passwd = None, create_id = False):
    """
    A method for creating a new table in database
        
    Args:
        db_host : database host name
        db_name : name of database to be used
        table_name : name of table to be created
        generator : a generator object that has generator method named
                   'yield_tuple'
        column_names : a list of tuples of (column_name, type). 
                      Both are strings.
        db_passwd : database password if exists.
        create_id : create an auto incrementing primary key id attribute
                    in the table if set to True
    
    Returns:
        Number of rows created in the table.
    
    Raises:
        None
    """

    if db_user:
        if db_passwd:
            conn = connect(host = db_host, user = db_user,
                           db = db_name, passwd = db_passwd)
        else:
            conn = connect(host = db_host, user= db_user,
                           db = db_name)
    else:
        conn = connect(host = db_host)

    cursor = conn.cursor()
    
    create_statement = "CREATE TABLE IF NOT EXISTS  "

    #escape mysql escape characters if exists.
    s = ", ".join(map(lambda x: " ".join(map(escape_string, x)), column_names))

    if create_id :
        s = "Id INT PRIMARY KEY AUTO_INCREMENT, " + s
    create_statement += table_name + "( " + s + ")"
    
    print create_statement
    
    #create table if not exists.
    cursor.execute(create_statement)

    i = 0

    #insert rows into the table using tuples yielded by generator
    for entry in generator.yield_tuple():
        #length of tuple must be equal to # of columns
        if len(entry) != len(column_names):
            print "length of entry and number of column does not match"
            sys.exit(1)
        entry = map(lambda x:  "'"+escape_string(x)+"'"  if x !='\N'
                    else "NULL", entry)
        insert_statement = ("INSERT INTO " + table_name + "(" + 
                            ", ".join(map(lambda x:escape_string(x[0]), 
                                          column_names)) 
                            + ") VALUES (" + ", ".join(entry) + ")")
#        print insert_statement
        cursor.execute(insert_statement)
        i += cursor.rowcount
    return i
    

if __name__ == "__main__":
    
    usage = ("usage: %prog [options] [db_name] [table_name]"
             " [parser_module_name] [generator_class_name] [input_file] "
             "[column_names] \n"
             "Create a table in the database by parsing input_file. Note that "
             "column_names must be enclosd in quotes and the tuple must be "
             "separated by comma without space to be parsed correctly.\n"
             "Generator class should be able to initialized with input_file "
             "name, parse the file and generator that yields tuple " 
             "to be inserted into the table\n\n"
             "Example : ./create_table.py -u ungar -i shepard contexts "
             "parse_contexts_and_matches Parser "
             "/home/mpatek/all_files_output/contexts-and-matches.txt"
             "'(citing_id,BIGINT) (cited_id,BIGINT) (sentences,VARCHAR(3000))'")

    parser = OptionParser(usage)
    
    parser.add_option("-p", "--passwd", dest="passwd", action = "store", 
                      default=None, help="DB User password")
    parser.add_option("-o", "--hostname", dest="host", action = "store", 
                      default=None, help="DB host name")
    parser.add_option("-u", "--user", dest="user", action = "store", 
                      default=None, help="DB user name")
    parser.add_option("-i", "--createid", dest="id", action = "store_true", 
                      default=False, help="Add id as auto incrementing"
                      "primary key as attribute")
    
    (options, args) = parser.parse_args()
    if len(args) != 6:
        print "Need 6 arguments"
        sys.exit(1)
    #Set DB Host
    if not options.host:
        db_host = "localhost"
    else:
        db_host = options.host

    # get generator module
    generator_module = __import__( args[2] ) 
    # get generator class
    generator_class = getattr(generator_module, args[3] )

    # instantiate generator object
    generator = generator_class(args[4])
    # the generator object should have a generator named "yield_tuple"
    if not hasattr(generator, "yield_tuple"):
        print "parser does not have method 'yield_tuple'"
        sys.exit(1)

    # column names : string of tuples(column_name,type) separated by space.
    # use only comma without space between column_name and type inside the tuple.
    # e.g. '(Type,VARCHAR(25)) (Sentences,VARCHAR(400))'
    column_names = map(lambda x:tuple(x[1:-1].split(",")), args[5].split())

    num_rows_created = create_table(db_host, options.user, args[0], args[1], generator, 
              column_names, options.passwd, options.id)
                            
    print "Number of rows created %s" % num_rows_created
