#!/usr/bin/python
# sys - allows for reading data from the standard input.
import sys

# previous will be used to store the previous key and is initialized to None.
previous = None
# sum will be used to store the count of the current word and is initialized to 0.
sum = 0

# standard input is read line by line and put in a variable named 'line'
for line in sys.stdin:
	# key and value are extracted of each line by splitting the line on the tab character.
	# as the tab is the delimited written by mapper. 
	key, value = line.split('\t')
	
	# The current key (key) is compared to the previous one (previous).
	# If the current and previous key are different, it means that a new word has be found and
	# unless the previous key is empty, this is not the first word
	if key != previous:
		if previous is not None:
			# The variable 'count' contains the total number of occurrences of the previous word 
			# and this result can be displayed
			print str(sum) + '\t' + previous
		# As a new word is found, it is necessary to reinitiate the working variables
		previous = key
		sum = 0

	# in all cases, the loop ends with adding the current 'value' to the 'sum'	
	sum = sum + int(value)
# finally, after having exited the loop, the last result can be displayed
print str(sum) + '\t' + previous
