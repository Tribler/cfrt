from math import log, ceil
from hashlib import md5
from typing import Generator

SHIFT_BYTES_TO_BITS = 3
MASK_BIT_INDEX = (0x1 << SHIFT_BYTES_TO_BITS) - 1

COUNT_LOOKUP = [format(nibble, "b").count("1") for nibble in range(0, 256)]

class BloomFilter:
    FALSE_POSITIVE_PROBABILITY = 0.00000001
    EXPECTED_ELEMENTS = 2000

    def __init__(self, max_elements=EXPECTED_ELEMENTS) -> None:
        self.k = int(ceil(-log(BloomFilter.FALSE_POSITIVE_PROBABILITY) / log(2)))
        self.m = int(ceil(-max_elements * log(BloomFilter.FALSE_POSITIVE_PROBABILITY) / (log(2) ** 2)))
        self.max_elements = max_elements
        self.estimated_size = 0

        self.bits = bytearray(1 + (self.m >> SHIFT_BYTES_TO_BITS))

    def estimate_size(self):
        ones = sum(COUNT_LOOKUP[x] for x in self.bits)
        return -(self.m/self.k)*log(1-ones/self.m)

    def get_bit(self, index: int) -> int:
        return (self.bits[ index >> SHIFT_BYTES_TO_BITS ] >> (index & MASK_BIT_INDEX)) & 0x1

    def set_bit(self, index: int, value: int) -> None:
        byte = index >> SHIFT_BYTES_TO_BITS
        shift = index & MASK_BIT_INDEX
        self.bits[byte] = (self.bits[byte] & ~(0x1 << shift)) | (value << (index & MASK_BIT_INDEX))

    def lookup(self, item: str) -> bool:
        for index in self.indexes_for(item):
            if self.get_bit(index) == 0:
                return False
        return True

    def add(self, item: str) -> None:
        self.estimated_size += 1
        for index in self.indexes_for(item):
            self.set_bit(index, 1)

    def combine(self, other: "BloomFilter") -> None:
        for n in range(0, len(self.bits)):
            self.bits[n] |= other.bits[n]
        self.estimated_size = self.estimate_size()
        if self.estimated_size > self.max_elements:
            print("Bloom filter overflow %s > %s" % (self.estimated_size, self.max_elements))

    def indexes_for(self, item: str) -> Generator[int, None, None]:
        hash = md5(item.encode())
        for i in range(0, self.k):
            hash.update(bytes([i]))
            yield int.from_bytes(hash.digest(), "little") % self.m
