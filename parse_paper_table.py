#!/usr/bin/python2.6
import re
from create_table import create_table

class Parser:
    
    def __init__(self, filename):
        self.infile = open(filename)
        self.bpidMap = {}
        self.log = open("log", 'w')

    def yield_tuple(self):
        for line in self.infile:
            tokens = line.split('\t')
            tokens = map(lambda x:x.strip(), tokens)
            sentences = "".join(tokens[4:7])
            if tokens[14] in self.bpidMap:
                self.bpidMap[tokens[14]] += 1
            else:
                self.bpidMap[tokens[14]] = 0
                yield (tokens[7], tokens[9], tokens[10], tokens[8], '\N', tokens[17], tokens[18])

    def printlog(self):
        for k in self.bpidMap.keys():
            self.log.write(k+"\t"+str(self.bpidMap[k]))
            
if __name__ == "__main__":
    
    usage = ("usage: %prog [options] [db_name] [table_name]"
             "[input_file] "
             "[column_names] \n"
             "Create a table in the database by parsing input_file. Note that "
             "column_names must be enclosd in quotes and the tuple must be "
             "separated by comma without space to be parsed correctly.\n"
             "Generator class should be able to initialized with input_file "
             "name, parse the file and generator that yields tuple " 
             "to be inserted into the table\n\n"
             "Example : ./create_table.py -u ungar -i shepard contexts "
             "parse_contexts_and_matches Parser "
             "/home/mpatek//home/mpatek/all_files_output/contexts-and-matches.txt"
             "'(citing_id,BIGINT) (cited_id,BIGINT) (sentences,VARCHAR(3000)')")

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

    # instantiate generator object
    generator = Parser(args[2])
    # the generator object should have a generator named "yield_tuple"
    if not hasattr(generator, "yield_tuple"):
        print "parser does not have method 'yield_tuple'"
        sys.exit(1)

    # column names : string of tuples(column_name,type) separated by space.
    # use only comma without space between column_name and type inside the tuple.
    # e.g. '(Type,VARCHAR(25)) (Sentences,VARCHAR(400))'
    column_names = map(lambda x:tuple(x[1:-1].split(",")), args[3].split())

    num_rows_created = create_table(db_host, options.user, args[0], args[1], generator, 
              column_names, options.passwd, options.id)
    generator.printlog()

    print "Number of rows created %s" % num_rows_created


