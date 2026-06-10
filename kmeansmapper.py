#!/usr/bin/env python3

import sys
import math

# Initial centroids
centroids = [
    (25, 30000),
    (60, 90000)
]

for line in sys.stdin:
    line = line.strip()

    if not line:
        continue

    cid, age, income = line.split(',')

    age = float(age)
    income = float(income)

    min_dist = float('inf')
    cluster = 0

    for i, (c_age, c_income) in enumerate(centroids):

        dist = math.sqrt(
            (age - c_age)**2 +
            (income - c_income)**2
        )

        if dist < min_dist:
            min_dist = dist
            cluster = i

    print(f"{cluster}\t{age},{income}")
