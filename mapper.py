#!/usr/local/bin/python
import sys

def read_input(file):
    for line in file:
        yield line.split()

def main(separator):
    """
    """
    data = read_input(sys.stdin)
    for words in data:
        sys.stdout.write("%s%s%d" % (word, separator, 1))

if __name__ == "__main__":
    main()
