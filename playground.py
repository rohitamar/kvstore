from collections import Counter 
import matplotlib.pyplot as plt 
from random import choices, randint
import string

from hashring.HashRing import HashRing

def get_random_string():
    n = randint(1500, 2500)
    return "".join(
        choices(string.ascii_uppercase, k=n)
    )

h = HashRing(10000)
db_ids = [8081, 8082, 8083, 8084]

for db_id in db_ids:
    url = f'http://127.0.0.1/{db_id}'
    h.add_node(url)

# print(h.coverage())

c = Counter()
for i in range(100_000):
    s = get_random_string()
    indx = h.get_index(s)
    c[indx] += 1

items = c.items() 
items = list(sorted(items, key = lambda x : x[0]))

labels, values = zip(*items)

plt.figure(figsize=(12, 6))
plt.bar(labels, values)
plt.show()