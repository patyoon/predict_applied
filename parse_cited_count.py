
class Parser:

    def __init__(self, input_file):
        self.input_file = open(input_file, 'r')

    def yield_tuple(self):
        for line in self.input_file:
            tokens  = line.strip().split('\t')
            if len(tokens) == 2:
                print tokens
                cited_id = tokens[0]
                count = tokens[1]
            else:
                citing_id = tokens[0]
                yield (cited_id, count, citing_id)

