import sys, pickle, os
from optparse import OptionParser
from operator import itemgetter

if __name__ == "__main__":
    usage = ("usage: %prog [options] [model_pickle_filename] [feature_pickle_filename] [output_filename] [num_words]")
    parser = OptionParser(usage)
    (options, args) = parser.parse_args()
    if not os.path.exists(args[0]) or not os.path.exists(args[1]):
        print "file does not exist"
        sys.exit(1)

    if len(args) != 4:
        print "need 4 arguments"
        sys.exit(1)

    model = pickle.load(open(args[0], "rb"))
    word_index = pickle.load(open(args[1], "rb"))
    word_lists = map (lambda x: x[1], sorted(word_index.items(), key = lambda x:x[0]))
    weight_tuples = map (lambda x: sorted(zip(list(x), word_lists),
                                          key = lambda x: abs(float(x[0])), reverse=True),
                         list(model[0].coef_))
    weight_tuples_pos = map(lambda x: filter(lambda y: y[0] > 0, x), weight_tuples)
    weight_tuples_neg = map(lambda x: sorted(filter(lambda y: y[0] < 0, x),
                                             key = lambda x: abs(float(x[0])), reverse=True),
                                             weight_tuples)
    with open(args[2], 'w') as outfile:
        outfile.write(str(model[0])+"\n")
        outfile.write( (args[0]+" MSE Score: %0.2f (+/- %0.2f)\n"
                        % (model[1].mean(), model[1].std()/2)))
        outfile.write('intercept :' + str(model[0].intercept_)+'\nfeature weights:\n')
        for i in xrange(len(weight_tuples)):
            outfile.write('\n res_level: '+str(i+1) +"\npositive weights: " +
                          str(len(weight_tuples_pos[i])) +" words\n\n")
            for item in weight_tuples_pos[i][:int(args[3])]:
                outfile.write(str(item[0])+'\t'+item[1]+'\n')
            outfile.write("\nnegative weights: "+str(len(weight_tuples_neg[i])) +" words\n\n")
            for item in weight_tuples_neg[i][:int(args[3])]:
                outfile.write(str(item[0])+'\t'+item[1]+'\n')
