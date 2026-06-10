#!/usr/bin/env python3

import sys

current_city = None
temp_sum = 0
count = 0

for line in sys.stdin:
    line = line.strip()

    city, temp = line.split('\t')
    temp = float(temp)

    if current_city == city:
        temp_sum += temp
        count += 1
    else:
        if current_city:
            avg = temp_sum / count
            print(f"{current_city}\t{avg:.2f}")

        current_city = city
        temp_sum = temp
        count = 1

if current_city:
    avg = temp_sum / count
    print(f"{current_city}\t{avg:.2f}")
