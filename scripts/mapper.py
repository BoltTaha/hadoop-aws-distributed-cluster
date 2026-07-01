#!/usr/bin/env python3
import sys

for line in sys.stdin:
    data = line.strip().split(",")
    key = data[0]
    value = 1
    print("{0}\t{1}".format(key, value))
