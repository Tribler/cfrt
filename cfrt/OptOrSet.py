from random import randint
from typing import Sequence


def IsInIntervalVector(vector, replica_id, timestamp):
    if replica_id in vector:
        for l, r in vector[replica_id]:
            if l <= timestamp <= r:
                return True
    return False

def CollapseVector(vector):
    for i in range(len(vector) - 1, 0, -1):
        if vector[i-1][1] >= vector[i][0] - 1:
            vector[i-1] = (min(vector[i-1][0], vector[i][0]), max(vector[i-1][1], vector[i][1]))
            del vector[i]

class OptORSet:
    def __init__(self) -> None:
        self.Entries = set()
        self.Vector = dict()
        self.Dirty = False
        self.whitewash()

    def __str__(self):
        return "entries: [" + ", ".join(item[0] for item in self.Entries) + "], version vect: %r" % self.Vector

    def __len__(self):
        return len(self.Entries)

    def __iter__(self) -> Sequence:
        for entry in self.Entries:
            yield entry[0]

    def lookup(self, item) -> bool:
        for m in self.Entries:
            if m[0] == item:
                return True
        return False

    def add(self, item) -> None:
        self.Clock += 1
        self.Entries.add((item, self.Id, self.Clock))
        if not self.Id in self.Vector:
            self.Vector[self.Id] = list()
        vect = self.Vector[self.Id]
        vect.append((self.Clock, self.Clock))
        vect.sort(key=lambda k: k[0])
        CollapseVector(vect)
        self.Dirty = True

    def remove(self, item) -> None:
        removed = False
        for m in list(self.Entries):
            if m[0] == item:
                self.Entries.remove(m)
                removed = True
        if removed:
            self.Clock += 1
            if not self.Id in self.Vector:
                self.Vector[self.Id] = list()
            vect = self.Vector[self.Id]
            vect.append((self.Clock, self.Clock))
            vect.sort(key=lambda k: k[0])
            CollapseVector(vect)
            self.Dirty = True

    def combine(self, other: "OptORSet"):
        newEntries = self.Entries.intersection(other.Entries)
        for m in self.Entries ^ other.Entries:
            if IsInIntervalVector(self.Vector, m[1], m[2]) and IsInIntervalVector(other.Vector, m[1], m[2]):
                continue
            else:
                newEntries.add(m)
        self.Dirty = self.Dirty or newEntries != self.Entries
        self.Entries = newEntries
        for key in set(self.Vector.keys()).union(other.Vector.keys()):
            if not key in self.Vector:
                self.Vector[key] = list()
            if not key in other.Vector:
                otherV = list()
            else:
                otherV = list(other.Vector[key])
            self.Vector[key] += otherV
            CollapseVector(self.Vector[key])

    def whitewash(self):
        self.Id = "%s" % randint(0, 2 ** 256)
        self.Clock = 0