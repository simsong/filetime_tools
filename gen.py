#!/usr/bin/env python36
#
# Make some data
import json

WORDS="/usr/share/dict/words"

if __name__=="__main__":
    words = open(WORDS).read().split("\n")
    numbered_words = [[n,words[n]] for n in range(0,len(words))]
    with open("words.json","w") as f:
        f.write("var numbered_words = ")
        f.write(json.dumps(numbered_words))
        f.write(";\n")

