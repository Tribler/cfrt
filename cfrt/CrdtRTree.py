from typing import Sequence, Union, Tuple, Iterator
from random import randint, sample
from experiments.cfrt.thesis.CrdtSet import CrdtSet

def prefix_count(left: str, right: str) -> int:
    finger = 0
    l = min(len(left), len(right))
    while finger < l and left[finger] == right[finger]:
        finger += 1
    return finger

def weighted_prefix_score(left: str, right: str) -> float:
    finger = 0
    l = min(len(left), len(right))
    while finger < l and left[finger] == right[finger]:
        finger += 1
    return (1 << (finger+8)) + (255 - abs(left[finger].encode()[0] - right[finger].encode()[0]))

def get(node_id: str) -> Union["CrdtRTree", None]:
    return None

async def get_async(node_id: str) -> Union["CrdtRTree", None]:
    return None

def set_root(node_id: str):
    pass

def add_node(node: "CrdtRTree"):
    pass

split_count = 0
merge_count = 0

class CrdtRTreeEntry:
    def __init__(self, key: Tuple[str, str], value: str):
        self.key_min = key[0]
        self.key_max = key[1]
        self.value = value

    def __str__(self):
        return str(self.key_min) + "-" + str(self.key_max) + "=" + str(self.value)

    def __eq__(self, other):
        if isinstance(other, CrdtRTreeEntry):
            return (self.key_min == other.key_min) and (self.key_max == other.key_max) and (self.value == other.value)
        else:
            return False

    def __hash__(self):
        return hash(self.key_max) ^ hash(self.key_min) ^ hash(self.value)

    @property
    def key(self) -> str:
        return str(self.key_min) + "-" + str(self.key_max)

    @property
    def target(self) -> Union["CrdtRTree", None]:
        return get(self.value)

    @property
    def is_leaf(self) -> bool:
        return self.key_min != "None" and self.key_min == self.key_max


class CrdtRTree:
    def __init__(self):
        self.state = CrdtSet()
        self.split_threshold = randint(24, 48)
        #self.split_threshold = randint(75, 100)
        self.join_threshold = randint(8, 12)
        #self.join_threshold = randint(20, 30)
        self.content_id = str(randint(0, 2 ** 64 - 1))
        self.should_check = False
        self.touched = False

    def __len__(self) -> int:
        return len([item for item in self.state if type(item) is CrdtRTreeEntry])

    def __iter__(self) -> Sequence[CrdtRTreeEntry]:
        for item in list(self.state):
            if type(item) is CrdtRTreeEntry:
                yield item

    @property
    def parents(self) -> Sequence[str]:
        return [item for item in self.state if not type(item) is CrdtRTreeEntry]

    def add_parent(self, par):
        self.state.add(par)

    def remove_parent(self, par):
        self.state.remove(par)

    def add(self, item: CrdtRTreeEntry) -> None:
        self.state.add(item)

    def remove(self, item: CrdtRTreeEntry) -> None:
        self.state.remove(item)

    def remove_values(self, value: str) -> None:
        for item in list(self):
            if item.value == value:
                self.state.remove(item)

    def is_leaf_node(self):
        for item in self:
            if not item.is_leaf:
                return False
        return True

    def get_parents(self) -> Sequence["CrdtRTree"]:
        return [get(parent) for parent in self.parents]

    def compute_range(self) -> Tuple[str, str]:
        self_min = None
        self_max = None
        for item in self:
            if self_min is None or (item.key_min != "None" and item.key_min < self_min):
                self_min = item.key_min
            if self_max is None or (item.key_max != "None" and item.key_max > self_max):
                self_max = item.key_max
        return str(self_min), str(self_max)

    def compute_children(self) -> Sequence[str]:
        result = []
        for item in self:
            if not item.is_leaf and not item.value in result:
                result.append(item.value)
        return result

    def check(self):
        self.should_check = True

    def do_check(self):
        self.should_check = False

        # Figure out if one of our parents doesn't actually refer to us. If so it's not our parent
        if len(self.parents) > 1:
            for old_parent in self.parents:
                if get(old_parent) is None or not self.content_id in get(old_parent).compute_children():
                    self.remove_parent(old_parent)

        # TODO: Check if any of our children doesn't have self as parent?
        if len(self) <= self.join_threshold:
            self.merge()
            return

        # Check if any children overlap and merge those, in case the keys are 1d, this should subsequently result in a disjoint split.
        if not self.is_leaf_node():
            # sort children, and merge only from higher to low. This should sidestep any problems
            entries = list(self)
            entries.sort(key=lambda x: x.key_min + "," + x.key_max)
            finger = len(entries) - 1
            while finger > 0:
                left = entries[finger - 1]
                right = entries[finger]
                if left.key_max < right.key_min or right.key_min < left.key_max:
                    # children are disjoint, no sense in merging them
                    finger -= 1
                    continue
                print("%s: Overlap detected %s <-> %s" % (self.content_id, left, right))
                # children are not disjoint, merge and hope for a better split
                if right.key_min <= left.key_min and left.key_max <= right.key_max:
                    # left is contained in right
                    left.target.merge(sibling=right)
                else:
                    right.target.merge(sibling=left)
                # either left or right has changed and our entries list is stale, but we cant figure out the new values.
                # We skip the next pair since that would have left as right in the next loop, we'll eventually get it in a later round
                finger -= 2

        if len(self) > self.split_threshold:
            self.split()

        # Update our entry in parent
        my_range = self.compute_range()
        for parent in self.get_parents():
            if any(item.key_min == my_range[0] and item.key_max == my_range[1] and item.value == self.content_id for item in parent):
                continue
            parent.remove_values(self.content_id)
            parent.add(CrdtRTreeEntry(my_range, self.content_id))

        if len(self) == 0:
            self.delete()

    def delete(self):
        for parent in self.get_parents():
            parent.remove_values(self.content_id)
        for parent in self.parents:
            self.remove_parent(parent)
        #del world_content[self.content_id]

    def merge(self, sibling=None):
        global merge_count
        if len(self.parents) == 0:
            # we are root, there are no siblings to merge with.
            # however if we only have 1 child under root and we're not at the bottom, the child should become the new root
            if len(self) == 1 and not self.is_leaf_node():
                entry = list(self)[0]
                entry.target.remove_parent(self.content_id)
                set_root(entry.value)
            return

        # select sibling
        if sibling is None:
            my_range = self.compute_range()
            for parent in self.get_parents():
                for entry in parent:
                    if entry.value == self.content_id:
                        continue
                    if sibling is None:
                        sibling = entry
                        continue
                    # determine if entry is closer to self than sibling
                    if max(prefix_count(my_range[0], sibling.key_min), prefix_count(my_range[0], sibling.key_max),
                           prefix_count(my_range[1], sibling.key_min), prefix_count(my_range[1], sibling.key_max)) < max(
                            prefix_count(my_range[0], entry.key_min), prefix_count(my_range[0], entry.key_max),
                            prefix_count(my_range[1], entry.key_min), prefix_count(my_range[1], entry.key_max)):
                        sibling = entry
            if sibling is None:
                return

        merge_count += 1
        self.delete()
        for item in self:
            sibling.target.add(item)
            if not item.is_leaf:
                item.target.add_parent(sibling.value)
                item.target.remove_parent(self.content_id)
        sibling.target.do_check()

    def split(self):
        global split_count
        boundaries = []
        for item in self:
            if item.key_min != "None" and not item.key_min in boundaries:
                boundaries.append(item.key_min)
            if item.key_max != "None" and not item.key_max in boundaries:
                boundaries.append(item.key_max)
        boundaries.sort()
        if len(boundaries) < 2:
            # can't split empty or singular value node.
            return

        split_count += 1
        median = boundaries[len(boundaries) >> 1]

        left = []
        right = []
        for item in self:
            if item.key_min == "None":
                left.append(item)
            elif item.key_max == "None":
                right.append(item)
            elif item.key_max < median:
                left.append(item)
            elif item.key_min > median:
                right.append(item)
            elif prefix_count(item.key_min, median) < prefix_count(item.key_max, median):
                left.append(item)
            else:
                right.append(item)

        if len(left) == 0 or len(right) == 0:
            full = list(self)
            left = sample(full, self.split_threshold >> 1)
            right = [item for item in full if not item in left]

        if len(self.parents) == 0:
            new_parent = CrdtRTree()
            add_node(new_parent)
            self.add_parent(new_parent.content_id)
            set_root(new_parent.content_id)

        split = right
        #TODO: this way only splits off the right, should also split to the left?
        other = CrdtRTree()
        add_node(other)
        for parent in self.parents:
            other.add_parent(parent)

        for item in split:
            other.state.add(item)
            if not item.is_leaf:
                item.target.add_parent(other.content_id)
                item.target.remove_parent(self.content_id)
            self.state.remove(item)

        # force other to register entry in parent
        other.do_check()

    def add_leaf_item(self, key: str, value: str) -> None:
        if self.is_leaf_node():
            self.add(CrdtRTreeEntry((key, key), value))
        else:
            containers = [item for item in self if item.key_min <= key <= item.key_max]
            if len(containers) == 0:
                containers = list(self)
            entry = sorted(containers, key=lambda x: -max(prefix_count(x.key_min, key), prefix_count(x.key_max, key)))[0]
            entry.target.add_leaf_item(key, value)
        self.check()

    def del_leaf_item(self, key: str) -> None:
        if self.is_leaf_node():
            for item in list(self):
                if item.key_min == key and item.key_max == key:
                    self.remove(item)
        else:
            for item in list(self):
                if item.key_min <= key <= item.key_max:
                    item.target.del_leaf_item(key)
        self.check()

    def query_leaf_item(self, key: str) -> Union[str, None]:
        if self.is_leaf_node():
            for item in list(self):
                if item.key_min == key and item.key_max == key:
                    return item.value
        else:
            result = None
            for item in self:
                if item.key_min <= key <= item.key_max:
                    result = item.target.query_leaf_item(key)
                    if not result is None:
                        break
            return result

    async def query_leaf_item_async(self, key: str) -> Union[str, None]:
        if self.is_leaf_node():
            for item in list(self):
                if item.key_min == key and item.key_max == key:
                    return item.value
        else:
            result = None
            for item in self:
                if item.key_min <= key <= item.key_max:
                    target = await get_async(item.value)
                    if target is None:
                        continue
                    result = await target.query_leaf_item_async(key)
                    if not result is None:
                        break
            return result


    def debug_print(self, level: int = 0) -> None:
        print('\t'*level + self.content_id + " parents [" + ", ".join(self.parents) + "]")
        for item in self.state.entries:
            if not type(item[0]) is CrdtRTreeEntry:
                continue
            print('\t' * level + " - [" + item[0].key_min + " - " + item[0].key_max + "] -> " + item[0].value + " (tag: " + item[1] + ")")
            if not item[0].is_leaf:
                if get(item[0].value) is None:
                    print("\t"*level + "\tMISSING!!!")
                else:
                    item[0].target.debug_print(level + 1)

    def all_items(self) -> Iterator[Tuple[str, str]]:
        for item in self.state.entries:
            if not type(item[0]) is CrdtRTreeEntry:
                continue
            if item[0].is_leaf:
                yield (item[0].key_min, item[0].value)
            else:
                sub = get(item[0].value)
                if sub is None:
                    continue
                else:
                    for entry in sub.all_items():
                        yield entry
