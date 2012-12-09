#!/usr/bin/python2.6
import re

class Parser:
    
    def __init__(self, filename):
        self.infile = open(filename)
        self.truncated_log = open('../truncated_log', 'w')
        self.id = 1
        self.unique_context_dict = {}

    def yield_tuple(self):

        p = re.compile(r"(begin_citation\s+\-\s+(((bib\d+,?)+)\|\d+)\s+\-\s+end_citation)")
        for line in self.infile:
            tokens = line.split('\t')
            tokens = map(lambda x:x.strip(), tokens)
            sentences = "".join(tokens[4:7])
            self.bib = tokens[2]
            bibs = ""
            for match in p.finditer(sentences):
                bibs += match.group(2)
            sentences = p.sub(self.notBibrepl, sentences)
            if len(sentences.strip()) > 3000:
                self.truncated_log.write( str(len(sentences.strip())) +  "---" + sentences.strip() + ' ---' + self.bib + "\n")
                print "truncated "
            if bibs == "":
                print sentences
            if bibs in self.unique_context_dict:
                sentence_id = self.unique_context_dict[bibs]
                print "duplicate: ", bibs
            else:
                sentence_id = self.id
                self.unique_context_dict[bibs] = sentence_id
                self.id +=1
            yield (repr(sentence_id), tokens[7], tokens[14], sentences, )
    
    def notBibrepl(self, matchobj):
        if self.bib in matchobj.group(2).split(","):
            return " "+matchobj.group(2)+" "
        else:
            return ""
