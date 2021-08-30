from random import randint
from typing import Sequence


class NaiveORSet:
    def __init__(self) -> None:
        self.Insert = set()
        self.Tombstones = set()
        self.Dirty = False

    def __str__(self):
        return "entries: [" + ", ".join(["[%s, %s]" % tup for tup in self.Insert if not tup in self.Tombstones]) + "]"

    def __len__(self):
        return len(self._compute())

    def __iter__(self) -> Sequence:
        for entry in self._compute():
            yield entry[0]

    def _compute(self) -> set:
        return self.Insert.difference(self.Tombstones)

    def lookup(self, item) -> bool:
        return item in self

    def add(self, item) -> None:
        if not self.lookup(item):
            self.Dirty = True
            self.Insert.add((item, "{0:032x}".format(randint(0, 2 ** 128))))

    def remove(self, item) -> None:
        tagged_item = next((tup for tup in self.Insert if tup[0] == item and not tup in self.Tombstones), None)
        if not tagged_item is None:
            self.Dirty = True
            self.Tombstones.add(tagged_item)

    def combine(self, other: "NaiveORSet"):
        new_insert = self.Insert.union(other.Insert)
        new_tombstone = self.Tombstones.union(other.Tombstones)
        self.Dirty = self.Dirty or (new_insert != self.Insert) or (new_tombstone != self.Tombstones)
        self.Insert = new_insert
        self.Tombstones = new_tombstone

    def whitewash(self):
        pass