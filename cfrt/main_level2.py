from CrdtRTree import current_root, CrdtRTree, touched_nodes, get
from NaiveORSet import NaiveORSet
from hashlib import md5
from random import  shuffle
import pickle

# Generate root node
CrdtRTree()
naive = NaiveORSet()

testdata = { }

#generate test data
finger = ""
while len(testdata) < 5000:
    next_finger = md5(finger.encode()).hexdigest()
    testdata[finger] = next_finger
    finger = next_finger

i = 0
for k, v in testdata.items():
    current_root().add_leaf_item(k, v)
    naive.add((k, v))
    i += 1
    if i % 100 == 0:
        print("I: " + str(i))

# To see what the tree looks like
#current_root().debug_print()
#print()

distribution = {}
size_distribution = {}

for k, v in testdata.items():
    touched_nodes.clear()
    result = current_root().query_leaf_item(k)
    index = len(touched_nodes)
    if not index in distribution:
        distribution[index] = 0
        size_distribution[index] = 0
    distribution[index] += 1
    size_distribution[index] += sum(len(pickle.dumps(get(item))) for item in touched_nodes)
    if result != v:
        print("Query for '%s' result '%s' == '%s': '%s'" % (k, result, v, result == v))

print("nodes_touched;occurrence_count;avg_bytes_touched")
for k in sorted(distribution.keys()):
    print(str(k) + ";" + str(distribution[k]) + ";" + str(size_distribution[k]/distribution[k]))

print()
print("Naive OR-set size: " + str(len(pickle.dumps(naive))))
kv = list(testdata.items())
shuffle(kv)
for k, _ in kv:
    current_root().del_leaf_item(k)
