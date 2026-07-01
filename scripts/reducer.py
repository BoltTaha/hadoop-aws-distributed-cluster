#!/usr/bin/env python3
import sys

total = 0
oldkey = None

for line in sys.stdin:
    data = line.strip().split("\t")
    thiskey = data[0]
    value = data[1]

    if thiskey != oldkey and oldkey is not None:
        print("{0}\t{1}".format(oldkey, total))
        oldkey = thiskey
        total = 0

    oldkey = thiskey
    total += float(value)

if oldkey is not None:
    print("{0}\t{1}".format(oldkey, total))
