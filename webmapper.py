#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()

    fields = line.split()

    if len(fields) == 2:
        ip, page = fields
        print(f"{page}\t1")
