import cProfile
import time
from collections import Counter
import ray


@ray.remote
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

    def process_chunk(self, chunk):
        for line in chunk:
            parts = line.split(';')
            order = (parts[0], parts[1])
            self.add(order)

    def merge(self, customer_dict):
        for key in customer_dict:
            if key in self.customers:
                self.customers[key].update(customer_dict[key])
            else:
                self.customers[key] = customer_dict[key]

    def as_dict(self):
        return self.customers


def read_dataset(filename):
    file = open(filename, 'r')
    return file.readlines()


def find_top_10_customers():
    lines = read_dataset("sales-data.txt")
    chunks = 12
    chunk_size = int(len(lines) / chunks + 1)
    counters = []
    for i in range(chunks):
        customer_counter = CustomerCounter.remote()
        start_index = i * chunk_size
        chunk = lines[start_index:start_index + chunk_size]
        customer_counter.process_chunk.remote(chunk)
        counters.append(customer_counter)
    counter = counters[0]
    for i in range(1, chunks):
        counter.merge.remote(counters[i].as_dict.remote())
    top10 = counter.get_count.remote()
    print(ray.get(top10).most_common(10))


if __name__ == '__main__':
    start = time.time()
    #cProfile.run("find_top_10_customers()", sort="cumtime")
    find_top_10_customers()
    print("duration " + str(time.time() - start))
