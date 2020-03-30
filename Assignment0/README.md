Explaining how to run your code, where we can find the output files to step 4 and 5, and how to interpret your output for step 5



#Explaining how to run your code.
Use [spark-submit step5.py wiki.txt] to run the code, "step 5" is the name of the py file I wrote.

#Where we can find the output files to step 4 and 5.
There is a folder named step 4 which includes the the output files to step 4.
There is a folder named step 5 which includes the the output files to step 5.


#How to interpret your output for step 5.
"bigram_Counts" is the output to count for the bigram for "wiki".
Within the file, (("x","y"),n) is the format. 
"n" means the number of the bigram.
("x","y") means the set of the bigram.

"bigram_frequency" is the output to display the conditional frequency distribution for "wiki".
Within the file, ("x",("x", "y"), p) is the format.
"p" means the conditional frequency for the bigram.
("x","y") means the set of the bigram.

