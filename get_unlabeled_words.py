#!/usr/bin/python2.7

from optparse import OptionParser

if __name__ == "__main__":
    
    usage = """usage: %prog [unlabeled word file] [class list]

             '../../word_lists/unlabeled.txt'"""
    
    parser = OptionParser(usage)

    (options, args) = parser.parse_args()
    teufel_file = open('../../word_lists/teufel_word_classification.csv')
    tech_file = open('../../word_lists/technical_words.txt')
    sentiment_file = open('../../word_lists/sentiment_words.txt')
    senti_file = open('../../word_lists/senti_words.txt')

    teufel_list = []
    classification_list = []
    for l in teufel_file:
        sp = l.split('\t')
        if len(sp) == 2:
            if len(sp[0])>0:
                classification_list.append(sp[0])
            teufel_list.append(sp[1])
        else:
            print "no"
            teufel_list.append(sp[0])
    
    non_labeled = set()
    for f in [tech_file, sentiment_file, senti_file]:
        for l in f:
            sp = l.split('\t')
            if sp[0].isdigit() and not sp[2] in teufel_list:
                non_labeled.add(sp[2])
            elif not sp[1] in teufel_list:
                non_labeled.add(sp[1])
                
    write_file = open('../../word_lists/unlabeled.txt','w')
    for l in non_labeled:
        write_file.write(l)

    write_file = open('../../word_lists/class_list.txt','w')
    for l in classification_list:
        write_file.write(l+"\n")
        

