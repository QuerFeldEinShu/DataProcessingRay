import cProfile
import time
from collections import Counter
import ray


class CustomerCounter:
    def __init__(self):
        self.customers = {}

    def add(self, order):
        if order[0] in self.customers:
            self.customers[order[0]].add(order[1])
        else:
            self.customers[order[0]] = set(order[1])

    def get_count(self):
        for key in self.customers:
            self.customers[key] = len(self.customers[key])
        return Counter(self.customers)


def read_dataset(filename):
    file = open(filename, 'r')
    return file.readlines()


def find_top_10_customers():
    lines = read_dataset("sales-data.txt")
    customer_counter = CustomerCounter()
    for i in range(1, len(lines)):
        parts = lines[i].split(';')
        order = (parts[0], parts[1])
        customer_counter.add(order)
    print(customer_counter.get_count().most_common(10))


if __name__ == '__main__':
    start = time.time()
    #cProfile.run("find_top_10_customers()", sort="cumtime")
    find_top_10_customers()
    print("duration " + str(time.time() - start))
