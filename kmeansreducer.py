#!/usr/bin/env python3

import sys

current_cluster = None

sum_age = 0
sum_income = 0
count = 0

for line in sys.stdin:

    cluster, values = line.strip().split('\t')

    age, income = map(float, values.split(','))

    if current_cluster == cluster:

        sum_age += age
        sum_income += income
        count += 1

    else:

        if current_cluster is not None:

            new_age = sum_age / count
            new_income = sum_income / count

            print(f"{current_cluster}\t{new_age},{new_income}")

        current_cluster = cluster

        sum_age = age
        sum_income = income
        count = 1

if current_cluster is not None:

    new_age = sum_age / count
    new_income = sum_income / count

    print(f"{current_cluster}\t{new_age},{new_income}")
