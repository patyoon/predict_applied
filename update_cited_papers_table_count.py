#!/usr/bin/python2.7
import re, sys
from create_table import create_table
from MySQLdb import connect
from optparse import OptionParser

class Parser:
    
    def __init__(self, filename, table_name, log, db_host, db_user, 
                 db_name, db_passwd = None):
        self.infile = open(filename)
        self.table_name = table_name
        self.log = None
        if log:
            self.log = open(log, 'w')
        if db_user:
            if db_passwd:
                conn = connect(host = db_host, user = db_user,
                               db = db_name, passwd = db_passwd)
            else:
                conn = connect(host = db_host, user= db_user,
                               db = db_name)
        else:
            conn = connect(host = db_host)
        self.cursor = conn.cursor()
        self.cited_id_set = set()
	self.update_dict = dict()

    def yield_tuple(self):
        i = 0
        for line in self.infile:
            tokens = line.split('\t')
            tokens = map(lambda x:x.strip(), tokens)
            i+=1
            if i%1000000 == 0:
                print 'at line ', i
            elif tokens[14] in self.update_dict.keys() and self.update_dict[tokens[14]] == None and tokens[16] != '\N':
                #need to update ncit and ncit07
                if self.log:
                    self.log.write(tokens[14]+"\t"+tokens[16]+"\t"+tokens[17]+"\n")
                    #print 'update'
                self.update_dict[tokens[14]] = (tokens[16], tokens[17])
            elif not tokens[14] in self.cited_id_set:
                title = '\N'
                #self.cursor.execute('select title from cited_id_title where cited_id='+tokens[14])
                #if self.cursor.rowcount != 1:
                    #print 'Title is missing for cpid', tokens[14] 
                #    if self.log:
                #        self.log.write('missing title\t'+tokens[14]+'\n')
                #else:
                #    title = self.cursor.fetchall()[0][0]
                if tokens[16] == '\N' and tokens[17] == '\N':
                    self.update_dict[tokens[14]] = None
                self.cited_id_set.add(tokens[14])
                yield (tokens[14], tokens[8], tokens[9],  title, tokens[11], tokens[12], tokens[10], tokens[16], tokens[17])

    def update(self):
        print "Creating index on cited_id"
        self.cursor.execute('CREATE INDEX cited_id ON ' + self.table_name + ' (cited_id)')
        print "Start updating cited_id missing ncit and ncit07"
        for key in self.update_dict.keys():
            self.cursor.execute('"UPDATE '+self.table_name+' SET ncit07='+self.update_dict[key][0]+
                                ', ncit='+self.update_dict[key][1]+' WHERE cited_id='+key)
    
if __name__ == "__main__":
    
    usage = """usage: %prog [options] [db_name] [table_name] [input_file] [column_names]

Create cited_papers table. Use cited_id_title to fill in title of cited papers. 

<cited_papers>
 Table that has characteristics of a cited paper.
- cited_id (bigint) = ID of cited paper
- authors (char(255)) = name of authors of cited paper
- journal (char(100)) = name of journal in which cited paper is published
- title (char(100)) = title of cited paper
- volume (char(20)) = volume number of cited paper in the journal
- page (char(20)) = page of cited paper in the volume
- year (int, unsigned) = published year of cited paper.
- ncit07 (int, unsigned) = number of citation in 2007
- ncit (int, unsigned) = number of citation in all years.

Example: ./create_cited_papers_table.py -u root -p shepard shepard cited_papers /home/mpatek/all_files_output/contexts-and-matches.txt '(cited_id,bigint) (authors,char(255)) (journal,char(255)) (title,char(255)) (volume,char(20)) (page,char(20)) (year,int unsigned) (ncit07,int unsigned) (ncit,int unsigned)'

"""

    parser = OptionParser(usage)
    
    parser.add_option("-p", "--passwd", dest="passwd", action = "store", 
                      default=None, help="DB User password")
    parser.add_option("-o", "--hostname", dest="host", action = "store", 
                      default=None, help="DB host name")
    parser.add_option("-u", "--user", dest="user", action = "store", 
                      default=None, help="DB user name")
    parser.add_option("-l", "--log", dest="log", action = "store", 
                      default=None, help="Write logs to specified file path.")
    parser.add_option("-t", "--title", dest="title", action = "store", 
                      default=None, help="Write title")
    (options, args) = parser.parse_args()

    if len(args) != 4:
        print "Need 4 arguments."
        sys.exit(1)
    #Set DB Host
    if not options.host:
        db_host = "localhost"
    else:
        db_host = options.host

    # instantiate generator object
    generator = Parser(args[2], args[1], options.log, db_host, options.user, args[0], options.passwd)
    # the generator object should have a generator named "yield_tuple"
    if not hasattr(generator, "yield_tuple"):
        print "parser does not have method 'yield_tuple'"
        sys.exit(1)

    # column names : string of tuples(column_name,type) separated by space.
    # use only comma without space between column_name and type inside the tuple.
    # e.g. '(Type,VARCHAR(25)) (Sentences,VARCHAR(400))'
    column_names = map(lambda x:tuple(x[1:-1].split(",")), re.findall(r'\(\w+,[\(\)\w\s]+\)',args[3]))

    # ADD auto incrementing ID. 
    num_rows_created = create_table(db_host, options.user, args[0], args[1], generator,
              column_names, options.passwd, True)
    
    generator.update()
    print "Number of rows created %s" % num_rows_created


