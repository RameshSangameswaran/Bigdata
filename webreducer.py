#!/usr/bin/env python3
import sys

current_page = None
count = 0

for line in sys.stdin:
    page, value = line.strip().split('\t')
    value = int(value)

    if current_page == page:
        count += value
    else:
        if current_page:
            print(f"{current_page}\t{count}")

        current_page = page
        count = value

if current_page:
    print(f"{current_page}\t{count}")
