#!/usr/bin/python2.7

import nltk, sys
from nltk.corpus import wordnet as wn
from whoosh.lang.morph_en import variations
from optparse import OptionParser
from nltk.tag.stanford import StanfordTagger


"""
A library for adding new words to the wordset
It first searches if the given word is already in the wordset.
Then, it searches if the variation of the word form is in the wordset.
If not it will prompt the users with the available choice of word_labels.
The user needs to select the label to put the word into the word set.

Text version. Will build a DB version soon.

"""
                
if __name__ == "__main__":

    teufel_file = open('../../word_lists/teufel_word_classification.csv')
    #filtered_file = open('../../word_lists/filtered.txt')
    unlabeled_file = open('../../word_lists/filtered.txt')
    unlabeled_set = map(lambda x: wn.morphy(x.strip()), unlabeled_file.readlines())
    class_list_file = open('../../word_lists/class_list.txt')
    skipped_file = open('../../word_lists/skipped.txt', 'w')
    filled_file = open('../../word_lists/filled.txt', 'w')
    files = [teufel_file, unlabeled_file, class_list_file, skipped_file, filled_file]
    st = StanfordTagger('/usr/local/lib/stanford-postagger-full-2012-01-06/models/english-bidirectional-distsim.tagger', '/usr/local/lib/stanford-postagger-full-2012-01-06/stanford-postagger.jar')
    class_dict = {}
    class_category = {}

    #make a dict with label as key and prefix as value.
    #make another dictionary with POS as key and list of labels as value.
    for l in class_list_file:
        sp = l.strip().split('_')
        class_dict[l.strip()] = '_'.join(sp[:-1])
        if sp[-1] in class_category:
            class_category[sp[-1]].append(l.strip())
        else: 
            class_category[sp[-1]] = [l.strip()]
                           
    #make a list with word as key, label as value
    teufel_dict = {}
    label = ''
    for line in teufel_file:
        tokens = line.split('\t')
        if len(tokens) > 2:
            print "invalid word list format: ", line
            sys.exit(1)
        if tokens[0] != "" and len(tokens) ==2 :
            label = tokens[0]
            word = tokens[1]
        elif len(tokens) == 2:
            word = tokens[1]
        else:
            print line, len(tokens), tokens
            sys.exit(1)
        teufel_dict[word.strip()] = label

    #print teufel_dict.keys()
    num_expanded = 0
    num_syned = 0
    num_skipped = 0
    
    for w in unlabeled_set:
        if not w in teufel_dict.keys() and not l == None:
            var = variations(w)
            #print var
            var_exists = False
            for word in var:
                if word in teufel_dict.keys():
                    var_exists = True
                    num_expanded +=1
                    print "Variation already exists: ", word, " for ", w
                    break
            #tag = nltk.pos_tag([w])[0][1]
            tag = st.tag([w])[0][1]
            tag_word = ''
            if tag in ['N', 'NP','NN']:
                tag_word = 'noun'
            elif tag in ['ADJ', 'JJ', 'JJR', 'JJS']:
                tag_word = 'adj'
            elif tag in ['ADV','RB', 'RBR', 'RBS']:
                tag_word = 'adv'
            elif tag in ['V', 'VD', 'VG', 'VN', 'VB', 'VBD', 'VBN', 'VBZ']:
                tag_word = 'action'
            if var_exists and tag_word != '':
                new_label = class_dict[teufel_dict[word]]+'_'+tag_word
                print "New label ",  new_label, " for ", w
                teufel_dict[w] = class_dict[teufel_dict[word]]+'_'+tag_word
            syn = [item for sublist in map (lambda x : x.lemma_names, wn.synsets(
                        'represent')) for item in sublist]
            syn_exists = False
            for word in var:
                if word in teufel_dict.keys():
                    syn_exists = True
                    num_syned +=1
                    print "Synonym already exists: ", word, " for ", w
                    break
            if syn_exists:
                teufel_dict[w] = teufel_dict[word]
            else:
                print "Retrieved ", tag, "as POS tag for ", w
                num_skipped +=1
                if tag_word in class_category.keys():
                    print "Existing labels: ", " ".join(class_category[tag_word])
                else:
                    print "Existing prefixes: ", " ".join(map(lambda x:class_dict[x],
                                                              class_dict.keys()))
                continue
                label = raw_input("Input label for word \'"+w+"'. Press RET to skip.")
                if label == '':
                    skipped_file.write(w+'\n')
                else:
                    teufel_dict[w] = label
        else:
            print "word ", w, " already exists"

    for word in teufel_dict.keys():
        filled_file.write(word+'\t'+teufel_dict[word]+'\n')

    for f in files:
        f.close()
    
    print "Total of ", num_expaned, " words discovered"
    print "Total of ", num_syned, " word syned"
    print "Total of ", num_skipped, " words skipped"
