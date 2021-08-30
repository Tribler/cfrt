import traceback
import sys
from subprocess import run

from typing import Union

import pickle
import shlex

from random import choice
from time import perf_counter, sleep

from gumby.experiment import experiment_callback
from gumby.util import run_task
from gumby.modules.pure_ipv8_module import  PureIPv8Module
from gumby.modules.experiment_module import static_module
from ipv8.configuration import Strategy, WalkerDefinition, default_bootstrap_defs

from experiments.cfrt.thesis.CrdtCommunity import CrdtCommunity
from experiments.cfrt.thesis.CrdtSet import CrdtSet
import experiments.cfrt.thesis.CrdtRTree as CrdtRTree

node_cache = {}

def ResolveRTreeNode(community: "CrdtCommunity", fetch_id: str) -> Union[None, "CrdtRTree.CrdtRTree"]:
    result = None
    if fetch_id in node_cache:
        result = node_cache[fetch_id]
    elif fetch_id in community.inner_replicas:
        result = CrdtRTree.CrdtRTree()
        result.content_id = fetch_id
        result.state = community.inner_replicas[fetch_id]
        node_cache[fetch_id] = result
    if not result is None:
        result.touched = True
    return result

async def ResolveRTreeNodeAsync(community: "CrdtCommunity", fetch_id: str) -> Union[None, "CrdtRTree.CrdtRTree"]:
    result = ResolveRTreeNode(community, fetch_id)
    if result is None:
        # ask the community about it
        result = await community.get(fetch_id)
        print("Resolving %s, community result %r" % (fetch_id, result))
        if result is None:
            print("Unable to find node for id %r" % fetch_id, file=sys.stderr)
            return None
        community.inner_replicas[fetch_id] = result
        node = CrdtRTree.CrdtRTree()
        node.content_id = fetch_id
        node.state = result
        node_cache[fetch_id] = node
        return node
    return result

def AddRTreeNode(community: "CrdtCommunity", node: "CrdtRTree.CrdtRTree"):
    print("Added node for id %r" % node.content_id, file=sys.stderr)
    community.inner_replicas[node.content_id] = node.state

def UpdateRoot(root_replica: "CrdtSet", node: str):
    for item in list(root_replica):
        if item[0] == "root":
            root_replica.remove(item)
    root_replica.add(("root", node))

def CountTouchedNodes():
    count = 0
    size = 0
    for node in node_cache.values():
        if node.touched:
            node.touched = False
            count += 1
            size += len(pickle.dumps(node.state))
    return count, size

alphabeth = "abcdefghijklmnopqrstuvwxyz0123456789"

def random_key(length=8):
    return "".join(choice(alphabeth) for i in range(0, length))

@static_module
class CfrtModule(PureIPv8Module):
    """
    This module contains code to experiment with CFRTs.
    """

    def __init__(self, experiment):
        super(CfrtModule, self).__init__(experiment)
        self.community = None
        self.tasks = {"stats": None, "add": None, "remove": None, "merge": None}
        self.my_entries = {}
        self.add_count = 0
        self.remove_count = 0
        self.touched_events = []
        self.last_touch_count = 0
        self.last_touch_size = 0
        self.check_events = []
        self.last_check_count = 0
        self.last_check_size = 0
        self.only_dirty = True
        self.root_size = "finite"
        self.last_data = None
        self.data_source = None

        CrdtRTree.get = lambda x: ResolveRTreeNode(self.community, x)
        CrdtRTree.add_node = lambda x: AddRTreeNode(self.community, x)
        CrdtRTree.set_root = lambda x: UpdateRoot(self.root_replica, x)
        CrdtRTree.get_async = self.get_node

    async def get_node(self, fetch_id):
        return await ResolveRTreeNodeAsync(self.community, fetch_id)

    def on_id_received(self):
        super(CfrtModule, self).on_id_received()
        self.config_builder.add_overlay("CrdtCommunity", "peer", [WalkerDefinition(Strategy.RandomWalk, 10, {'timeout': 3.0})],
                            default_bootstrap_defs, {}, [])
        self.extra_communities["CrdtCommunity"] = CrdtCommunity

    @property
    def root_replica(self) -> "CrdtSet":
        return self.community.replica

    @experiment_callback
    def introduce_peers_cfrt(self):
        self.community.network.print_debug()
        print("AllVars: %r" % self.all_vars)
        sys.stdout.flush()
        for peer_id in self.all_vars.keys():
            if int(peer_id) != self.my_id:
                self.community.walk_to(self.experiment.get_peer_ip_port_by_id(peer_id))

    @experiment_callback
    def set_datasource(self, source):
        start_time = perf_counter()
        runner = run(shlex.split(source), text=True, capture_output=True)
        print("Loading datasource errors '%r'" % runner.stderr)
        sys.stdout.flush()
        self.data_source = runner.stdout.splitlines()
        print("Done loading items %s %s seconds" % (len(self.data_source), (perf_counter() - start_time)))
        sys.stdout.flush()

    @experiment_callback
    def add_datasource_items(self):
        count = 0
        print("Starting adding datasource items")
        sys.stdout.flush()
        for line in self.data_source:
            last_data = line.split(';', maxsplit=1)
            self.tree_root.add_leaf_item(last_data[0], line)
            count += 1
            if count % 20 == 0:
                for node in list(node_cache.values()):
                    if node.should_check:
                        node.do_check()
            if count % 1000 == 0:
                print ("Done add %s, nodes %s" % (count, len(node_cache)))
                sys.stdout.flush()
        CountTouchedNodes()
        print("Done adding items %s @ %s" % (len(self.data_source), self.experiment_time))
        sys.stdout.flush()

    @experiment_callback
    async def verify_datasource_items(self):
        self.community.network.print_debug()
        count = 0

        await self.community.get(None)
        for item in self.root_replica:
            if item[0] == "root":
                await  self.community.get(item[1])

        for line in self.data_source:
            try:
                last_data = line.split(';', maxsplit=1)

                element_query_start_time = perf_counter()
                result = await self.tree_root.query_leaf_item_async(last_data[0])
                if line != result:
                    print("Difference on %s, expected '%s' got '%s'" % (last_data[0], line, result))
                count += 1
                print ("%s;validate" % (perf_counter() - element_query_start_time))
                sys.stdout.flush()
            except:
                traceback.print_exc()
                sys.stdout.flush()
                sys.stderr.flush()

    @experiment_callback
    async def start_ipv8(self):
        await super(CfrtModule, self).start_ipv8()
        self.community = self.ipv8.overlays[0]

    @experiment_callback
    def init_root(self):
        new_root = CrdtRTree.CrdtRTree()
        self.community.inner_replicas[new_root.content_id] = new_root.state
        CrdtRTree.set_root(new_root.content_id)

    @experiment_callback
    def cfrt_root_size_infinite(self):
        self.tree_root.split_threshold = sys.maxsize
        self.tree_root.join_threshold = -sys.maxsize
        self.root_size = "infinite"

    @experiment_callback
    def cfrt_only_dirty(self, value):
        print("Setting only dirty to %r condition %s" % (value, value == "True"))
        self.only_dirty = (value == "True")

    @property
    def tree_root(self) -> "CrdtRTree.CrdtRTree":
        return choice([CrdtRTree.get(item[1]) for item in self.root_replica if item[0] == "root"])

    @experiment_callback
    def cfrt_start_stats_task(self, interval=0.325):
        interval=float(interval)
        self.tasks["stats"] = run_task(self.cfrt_print_stats, delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)
        self.start_ipv8_statistics_monitor()

    @experiment_callback
    def cfrt_stop_stats_task(self):
        if "stats" in self.tasks and not self.tasks["stats"] is None:
            self.tasks["stats"].cancel()
            self.tasks["stats"] = None

    @experiment_callback
    def cfrt_start_add_task(self, interval=0.95, mean_count=1):
        mean_count=float(mean_count)
        interval=float(interval)
        # choice([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1])
        self.tasks["add"] = run_task(lambda : self.cfrt_add(count=mean_count), delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def cfrt_stop_add_task(self):
        if "add" in self.tasks and not self.tasks["add"] is None:
            self.tasks["add"].cancel()
            self.tasks["add"] = None

    @experiment_callback
    def cfrt_start_remove_task(self, interval=0.95, mean_count=1):
        mean_count=float(mean_count)
        interval=float(interval)
        self.tasks["remove"] = run_task(lambda : self.cfrt_remove(count=mean_count), delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def cfrt_stop_remove_task(self):
        if "remove" in self.tasks and not self.tasks["remove"] is None:
            self.tasks["remove"].cancel()
            self.tasks["remove"] = None

    @experiment_callback
    def cfrt_start_merge_task(self, interval=2):
        interval=float(interval)
        self.tasks["merge"] = run_task(self.cfrt_merge, delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def cfrt_stop_merge_task(self):
        if "merge" in self.tasks and not self.tasks["merge"] is None:
            self.tasks["merge"].cancel()
            self.tasks["merge"] = None

    @experiment_callback
    def cfrt_add(self, count=1, total=None):
        CountTouchedNodes()     # zero touched state
        try:
            if not total is None:
                # need to take care of fractions to ensure the total is accurate
                per_node = int(total) / len(self.experiment.all_vars)
                count = int((self.my_id + 1) * per_node) - int(self.my_id * per_node)
            else:
                count = int(count)
            while count > 0:
                key = "%s:%s" % (random_key(), self.my_id)
                value = "value for %s" % key
                self.tree_root.add_leaf_item(key, value)
                self.my_entries[key] = value
                #print("added k:%s v:%s" % (key, value))
                self.add_count += 1
                count -= 1
                self.touched_events.append(CountTouchedNodes())
        except:
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()

    @experiment_callback
    def cfrt_add_item(self, key, value):
        CountTouchedNodes()     # zero touched state
        try:
            self.tree_root.add_leaf_item(key, value)
            self.my_entries[key] = value
            self.add_count += 1
            self.touched_events.append(CountTouchedNodes())
        except:
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()

    @experiment_callback
    def cfrt_remove(self, count=1):
        CountTouchedNodes()     # zero touched state
        try:
            count = min(int(count), len(self.my_entries))
            while count > 0:
                rem_element = choice(list(self.my_entries.keys()))
                self.tree_root.del_leaf_item(rem_element)
                del self.my_entries[rem_element]
                #print("rem selected %s" % rem_element)
                self.remove_count += 1
                count -= 1
                self.touched_events.append(CountTouchedNodes())
        except:
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()

    @experiment_callback
    def cfrt_merge(self):
        try:
            check_count = 0
            check_size = 0
            for node in list(node_cache.values()):
                if node.should_check:
                    check_count += 1
                    check_size += len(pickle.dumps(node.state))
                    node.do_check()
            if check_count > 0:
                self.check_events.append((check_count, check_size))
            if (not self.only_dirty) or self.root_replica.Dirty:
                self.root_replica.Dirty = True
                self.community.broadcast_state()
            for replica in self.community.inner_replicas.keys():
                if (not self.only_dirty) or self.community.inner_replicas[replica].Dirty:
                    self.community.inner_replicas[replica].Dirty = True
                    self.community.broadcast_state(replicaid=replica)
        except:
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()

    @experiment_callback
    def cfrt_print_stats(self):
        if len(self.touched_events) > 0:
            count = 0
            size = 0
            for pair in self.touched_events:
                count += pair[0]
                size += pair[1]
            self.last_touch_count = count/len(self.touched_events)
            self.last_touch_size = size/len(self.touched_events)
            self.touched_events = []

        for replica in self.community.inner_replicas.keys():
            CrdtRTree.get(replica)
        self.touched_events = []

        if len(self.check_events) > 0:
            count = 0
            size = 0
            for pair in self.check_events:
                count += pair[0]
                size += pair[1]
            self.last_check_count = count/len(self.check_events)
            self.last_check_size = size/len(self.check_events)
            self.check_events = []

        all_entries = set()
        try:
            all_entries = set(self.tree_root.all_items())
        except:
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()

        try:
            print("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s" % (
                len(self.experiment.all_vars),
                self.my_id,
                int(self.experiment_time),
                self.root_size,
                len(all_entries),
                self.add_count,
                self.remove_count,
                len([item for item in node_cache.values() if len(item) > 0]),
                self.last_touch_count,
                self.last_touch_size,
                self.last_check_count,
                self.last_check_size,
                CrdtRTree.split_count,
                CrdtRTree.merge_count,
                self.community.stats["merge_count"],
                self.community.stats["pass_count"],
                self.community.stats["drop_count"],
                self.community.stats["last_merge_time"],
                self.community.stats["last_unpickle_time"],
                self.community.stats["last_buffer_fragment_time"],
                self.community.stats["last_broadcast_time"]
            ))
            #self.community.stats["pass_count"] = 0
            #self.community.stats["drop_count"] = 0

            #print("Root replica: %s" % self.root_replica)
            #for root in (CrdtRTree.get(item[1]) for item in self.root_replica if item[0] == "root"):
            #    root.debug_print(0)
            sys.stdout.flush()
        except:
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()

    @experiment_callback
    def cfrt_set_chaos_monkey(self, percentage):
        self.community.chaos_probability = int(percentage)

    @experiment_callback
    def cfrt_reset_stats(self):
        self.community.reset_stats()

    @experiment_callback
    async def stop_ipv8(self):
        self.community = None
        await super(CfrtModule, self).stop_ipv8()
