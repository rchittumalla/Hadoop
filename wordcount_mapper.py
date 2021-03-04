#!/usr/bin/python
# sys package to read data from standard input
# re to process data using regular expressions
import sys
import re

# process each line that is at the standard input
for line in sys.stdin:
	# remove all the leading and trailing non-word characters.
	line = re.sub(r'^\W+|\W+$', '', line)
	# split the data at word boundaries into a list - words
	words = re.split(r"\W+", line)
	# for each word in the above words list output the word, a tab delimited and a value '1'
	for word in words:
		print(word.lower() + "\t1")

