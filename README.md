# README #

This is a python module for reading and writing STAR format files.

Hall, S. R. (1991). The STAR file: A new format for electronic data transfer and archiving. Journal of Chemical Information and Computer …, 31(2), 326–333. doi:10.1021/ci00002a020

Back in 1991 when this format was proposed, there were no great alternatives; XML didn't exist yet, but today it is still used despite the large number of better defined, flexible and already implemented alternatives (XML, JSON, YAML).  /rant

I used the pyparsing library to implement the parsing.  

### What is this repository for? ###

TODO:
Still haven't implemented writing, but this should be simple.
Might want to wrap parsing elements in classes...  maybe subclass dict?