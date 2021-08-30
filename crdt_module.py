import pickle
import sys
from random import randint, choice

from gumby.experiment import experiment_callback
from gumby.util import run_task
from gumby.modules.pure_ipv8_module import  PureIPv8Module
from gumby.modules.experiment_module import static_module
from ipv8.configuration import Strategy, WalkerDefinition, default_bootstrap_defs

from experiments.cfrt.thesis.CrdtCommunity import CrdtCommunity
from experiments.cfrt.thesis.CrdtSet import CrdtSet
from experiments.cfrt.thesis.NaiveORSet import NaiveORSet
from experiments.cfrt.thesis.OptOrSet import OptORSet


@static_module
class CrdtModule(PureIPv8Module):
    """
    This module contains code to experiment with CRDTs.
    """

    def __init__(self, experiment):
        super(CrdtModule, self).__init__(experiment)
        self.community = None
        self.replica = None
        self.tasks = {"stats": None, "add": None, "remove": None, "whitewash": None, "merge": None}

    def on_id_received(self):
        super(CrdtModule, self).on_id_received()
        self.config_builder.add_overlay("CrdtCommunity", "peer", [WalkerDefinition(Strategy.RandomWalk, 10, {'timeout': 3.0})],
                            default_bootstrap_defs, {}, [])
        self.extra_communities["CrdtCommunity"] = CrdtCommunity

    @experiment_callback
    async def start_ipv8(self):
        await super(CrdtModule, self).start_ipv8()
        self.community = self.ipv8.overlays[0]
        self.replica = self.community.replica

    @experiment_callback
    def crdt_start_stats_task(self, interval=0.325):
        interval=float(interval)
        self.tasks["stats"] = run_task(self.crdt_print_stats, delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)
        self.start_ipv8_statistics_monitor()

    @experiment_callback
    def crdt_stop_stats_task(self):
        if "stats" in self.tasks and not self.tasks["stats"] is None:
            self.tasks["stats"].cancel()
            self.tasks["stats"] = None

    @experiment_callback
    def crdt_start_add_task(self, interval=0.95, mean_count=2):
        mean_count=float(mean_count)
        interval=float(interval)
        self.tasks["add"] = run_task(lambda : self.crdt_add(count=mean_count), delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def crdt_stop_add_task(self):
        if "add" in self.tasks and not self.tasks["add"] is None:
            self.tasks["add"].cancel()
            self.tasks["add"] = None

    @experiment_callback
    def crdt_start_remove_task(self, interval=0.95, mean_count=2):
        mean_count=float(mean_count)
        interval=float(interval)
        self.tasks["remove"] = run_task(lambda : self.crdt_remove(count=mean_count, own=True), delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def crdt_stop_remove_task(self):
        if "remove" in self.tasks and not self.tasks["remove"] is None:
            self.tasks["remove"].cancel()
            self.tasks["remove"] = None

    @experiment_callback
    def crdt_start_merge_task(self, interval=2):
        interval=float(interval)
        self.tasks["merge"] = run_task(self.community.broadcast_state, delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def crdt_stop_merge_task(self):
        if "merge" in self.tasks and not self.tasks["merge"] is None:
            self.tasks["merge"].cancel()
            self.tasks["merge"] = None

    @experiment_callback
    def crdt_start_whitewash_task(self, interval=0.95):
        interval=float(interval)
        self.tasks["whitewash"] = run_task(lambda : self.crdt_whitewash(), delay=self.my_id * interval / len(self.experiment.all_vars), interval=interval)

    @experiment_callback
    def crdt_stop_whitewash_task(self):
        if "whitewash" in self.tasks and not self.tasks["whitewash"] is None:
            self.tasks["whitewash"].cancel()
            self.tasks["whitewash"] = None

    @experiment_callback
    def crdt_set_type(self, type):
        if type == "bloom":
            self.replica = CrdtSet()
        elif type == "optor":
            self.replica = OptORSet()
        elif type =="or":
            self.replica = NaiveORSet()
        self.community.replica = self.replica

    @experiment_callback
    def crdt_add(self, element=None, count=1, total=None):
        if not total is None:
            # need to take care of fractions to ensure the total is accurate
            per_node = int(total) / len(self.experiment.all_vars)
            count = int((self.my_id + 1) * per_node) - int(self.my_id * per_node)
        else:
            count = int(count)
        while count > 0:
            add_element = element
            if add_element is None:
                add_element = "%s;%s" % (self.my_id, randint(0, 2 ** 31 - 1))
            #print("added %s" % add_element)
            self.replica.add(add_element)
            count -= 1

    @experiment_callback
    def crdt_remove(self, element=None, count=1, own=False):
        my_prefix = "%s;" % self.my_id
        my_elements = list(item for item in self.replica if item.startswith(my_prefix))
        #print("rem called. element %s, count %s, my_prefix %s, len(my_elements) %s" % (element, count, my_prefix, len(my_elements)))
        count = min(int(count), len(my_elements))
        while count > 0:
            rem_element = element
            if rem_element is None:
                rem_element = choice(my_elements)
                my_elements.remove(rem_element)

            #print("rem selected %s" % rem_element)
            self.replica.remove(rem_element)
            count -= 1

    @experiment_callback
    def crdt_whitewash(self):
        self.replica.whitewash()

    @experiment_callback
    def crdt_print_stats(self):
        print("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s" % (
            len(self.experiment.all_vars),
            self.my_id,
            int(self.experiment_time),
            type(self.replica).__name__,
            len(self.replica),
            len(pickle.dumps(self.replica)),
            self.community.stats["merge_count"],
            self.community.stats["last_merge_time"],
            self.community.stats["last_unpickle_time"],
            self.community.stats["last_buffer_fragment_time"],
            self.community.stats["last_broadcast_time"]
        ))
        sys.stdout.flush()

    @experiment_callback
    def crdt_reset_stats(self):
        self.community.reset_stats()

    @experiment_callback
    async def stop_ipv8(self):
        self.community = None
        await super(CrdtModule, self).stop_ipv8()
