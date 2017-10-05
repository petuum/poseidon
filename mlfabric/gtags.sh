#!/bin/bash
find . -type f -name *.cc -o -name *.cpp -o -name *.h -o -name *.hpp > gtags.files.tmp
GTAGSFORCECPP=1 gtags -f gtags.files.tmp

