#!/usr/bin/env python3
import sys

current_stock = None
total = 0.0
count = 0

for line in sys.stdin:
    stock, price = line.strip().split('\t')
    price = float(price)

    if current_stock == stock:
        total += price
        count += 1
    else:
        if current_stock:
            avg = total / count
            print(f"{current_stock}\t{avg:.2f}")

        current_stock = stock
        total = price
        count = 1

if current_stock:
    avg = total / count
    print(f"{current_stock}\t{avg:.2f}")
