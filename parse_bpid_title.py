
class Parser:

    def __init__(self, input_file):
        self.input_file = open(input_file, 'r')

    def yield_tuple(self):
        for line in self.input_file:
            tokens = line.split("\t")
            tokens = map(lambda x:x.strip(), tokens)
            
            yield (tokens[0], tokens[1],)
