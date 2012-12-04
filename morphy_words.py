#!/usr/bin/python2.7

import nltk
from MySQLdb import connect, escape_string
from nltk.corpus import wordnet as wn
from whoosh.lang.morph_en import variations
from optparse import OptionParser
from create_table import create_table

"""
A library for adding new words to the wordset
It first searches if the given word is already in the wordset.
Then, it searches if the variation of the word form is in the wordset.
If not it will prompt the users with the available choice of word_labels.
The user needs to select the label to put the word into the word set.
"""

class DBAddWord:
    
    def __init__(self, db_host, db_user, db_name, db_table_name, db=passwd=None):
        self.db_host = db_host
        self.db_name = db_name
        self.db_table = db_table
        self.col_names = [['word','varchar(50)'],['label','varchar(50)']]
        if db_passwd:
            conn = connect(host = db_host, user = db_user,
                           db = db_name, passwd = db_passwd)
        else:
            conn = connect(host = db_host, user= db_user,
                           db = db_name)
        self.cursor = conn.cursor()

    #add word file with classification to DB
    def add_to_db(self, filename):
        #(TODO):need to better structure them.
        create_table(self.db_host, self.db_user, self.db_name, self.db_table_name,
                     self.gen, db_passwd = None, True)
    
    def add_word_to_db(self, filename):
    
            



class
        def yield_tuple(self):

if __name__ == "__main__":

                



    usage = ("usage: %prog [options] [db_name][table_name] \n")
    parser = OptionParser(usage)
    
    parser.add_option("-p", "--passwd", dest="passwd", action = "store", 
                      default=None, help="DB User password")
    parser.add_option("-o", "--hostname", dest="host", action = "store", 
                      default=None, help="DB host name")
    parser.add_option("-u", "--user", dest="user", action = "store", 
                      default='ungar', help="DB user name")
    
    #Set DB Host
    if not options.host:
        db_host = "localhost"
    else:
        db_host = options.host


    (options, args) = parser.parse_args()
    
    db = DBAddWord(db_host, options.user, args[0], args[1], options.passwd)

    while True:
        print "sentiment analysis word input framework" 
        "1 - Add new word list with labels to DB\n"
        "2 - Add single word to DB\n"
        "3 - Add words in a file to DB\n"
        "4 - Remove a word from DB\n"
        s = raw_input('select an option\n>>')
        if int(s) < 1 or int(s) > 4:
            print "Wrong selection"
            continue
        if int(s) == 1:
            file_name = raw_input('type file name in word_list directory\n>>')
            
            
    
    (options, args) = parser.parse_args()
    if len(args) != 4:
        print "Need 4 arguments"
        sys.exit(1)
    
