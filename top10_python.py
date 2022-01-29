import cProfile
import time
from collections import Counter


class CustomerCounter:
    def __init__(self):
        self.customers = {}

    def add(self, customer, order):
        if customer in self.customers:
            self.customers[customer].add(order)
        else:
            self.customers[customer] = set(order)

    def get_count(self):
        counted = {}
        for key in self.customers:
            counted[key] = len(self.customers[key])
        return Counter(counted)


def read_dataset(filename):
    file = open(filename, 'r')
    return file.readlines()


def find_top_10_customers(lines):
    customer_counter = CustomerCounter()
    for i in range(1, len(lines)):
        parts = lines[i].split(';')
        customer_counter.add(parts[0], parts[1])
    print(customer_counter.get_count().most_common(10))


if __name__ == '__main__':
    lines = read_dataset("../sales-data.txt")
    start = time.time()
    #cProfile.run("find_top_10_customers()", sort="cumtime")
    find_top_10_customers(lines)
    print("duration " + str(time.time() - start) + "s")
