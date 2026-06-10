#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()

    fields = line.split(',')

    if len(fields) == 3:
        stock = fields[0]
        price = fields[2]

        print(f"{stock}\t{price}")
