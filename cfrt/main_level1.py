#!/usr/bin/python3

from CrdtSet import CrdtSet
from NaiveORSet import NaiveORSet
from OptOrSet import OptORSet
from random import shuffle
import pickle
import timeit


one = CrdtSet()
old = NaiveORSet()
alt = OptORSet()

alt2 = OptORSet()
alt.add("aap")
alt2.combine(alt)
alt2.add("noot")
alt2.remove("aap")
alt.combine(alt2)


#TODO: make a slightly better example that removes and adds stuff in a more realistic way, this way is worst case for the bloom filter
print("Run; Min new; Max new; Min old; Max old")
all_items = list(range(0, 1000))

def adds(target):
    for x in all_items:
        item = str(x)
        target.add(item)

def removes(target):
    for x in all_items:
        item = str(x)
        target.remove(item)

for run in range(0, 30):
    shuffle(all_items)
    min_one = len(pickle.dumps(one))
    time_one_add = timeit.timeit(lambda: adds(one), number=1)
    max_one = len(pickle.dumps(one))
    time_one_rem = timeit.timeit(lambda: removes(one), number=1)
    max_one = max(max_one, len(pickle.dumps(one)))
    print("%s;%s;%s;%s;%s" %(run, min_one, max_one, time_one_add, time_one_rem), end = "")

    min_old = len(pickle.dumps(old))
    time_old_add = timeit.timeit(lambda: adds(old), number=1)
    max_old = len(pickle.dumps(one))
    time_old_rem = timeit.timeit(lambda: removes(old), number=1)
    max_old = max(max_old, len(pickle.dumps(old)))

    print(";%s;%s;%s;%s" % (min_old, max_old, time_old_add, time_old_rem))