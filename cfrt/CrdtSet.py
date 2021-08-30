from typing import Sequence
from random import randint
from experiments.cfrt.thesis.BloomFilter import BloomFilter

# this class represents a Crdt set state
# the basic idea is to keep track of deleted tags in a bloomfilter
def generate_tag():
    return "{0:032x}".format(randint(0, 2 ** 128))

class CrdtSet:
    def __init__(self) -> None:
        self.entries = set()
        self.bloomfilters = [ BloomFilter() ]
        self.bloom_slack = 550
        self.Dirty = True

    def __str__(self):
        return "entries: [" + ", ".join(["[%s, %s]" % tup for tup in self.entries]) + "]"

    def __len__(self):
        return len(self.entries)

    def __iter__(self) -> Sequence:
        for entry in self.entries:
            yield entry[0]

    def __contains__(self, item) -> bool:
        return self.lookup(item)

    def lookup(self, entry) -> bool:
        return entry in (key for (key, _) in self.entries)

    # add element to this node. Adds a random tag to the element to force OR characteristics
    def add(self, entry) -> None:
        if not self.lookup(entry):
            self.Dirty = True
            self.entries.add((entry, generate_tag()))

    # removes an element from this node, and adds any removed tags to the bloomfilter
    def remove(self, entry) -> None:
        removals = [tup for tup in self.entries if tup[0] == entry]
        for item in removals:
            self.Dirty = True
            self.entries.remove(item)
            #print("Bloomfilter estimated size %s, max elements %s, bloom slack %s" %(self.bloomfilters[-1].estimated_size, self.bloomfilters[-1].max_elements, self.bloom_slack))
            if self.bloomfilters[-1].estimated_size >= self.bloomfilters[-1].max_elements - self.bloom_slack:
                self.bloomfilters.append(BloomFilter(max_elements=self.bloomfilters[-1].max_elements*2))
            self.bloomfilters[-1].add(item[1])

    # combines the changes contained in an other crdtset, that we have not yet seen, into this CrdtSets state
    def combine(self, other: "CrdtSet") -> None:
        for n in range(0, max(len(self.bloomfilters), len(other.bloomfilters))):
            left = self.bloomfilters[n] if len(self.bloomfilters) > n else None
            right = other.bloomfilters[n] if len(other.bloomfilters) > n else None
            if not left is None and not right is None:
                self.bloomfilters[n].combine(right)
            elif not left is None:
                self.bloomfilters[n] = left
            elif not right is None:
                self.bloomfilters.append(right)

        merged_entries = self.entries & other.entries
        for entry in self.entries ^ other.entries:
            if not any(bloom.lookup(entry[1]) for bloom in self.bloomfilters):
                merged_entries.add(entry)

        self.Dirty |= (self.entries != merged_entries)
        self.entries = merged_entries

    def whitewash(self):
        pass
