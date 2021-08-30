import pickle
import sys

from asyncio import Future

from random import randint
from math import ceil
from time import perf_counter
from typing import Tuple

from experiments.cfrt.thesis.CrdtSet import CrdtSet
from ipv8.keyvault.crypto import default_eccrypto
from ipv8.lazy_community import lazy_wrapper
from ipv8.messaging.lazy_payload import vp_compile, VariablePayload
from ipv8.community import Community


#
# I ran into trouble on larger states, they overflow a 64k UDP packet. pyipv8 just silently drops those
# so do some quick and dirty fragmentation
# The next boundry is the local UDP stack max recv buffer size. pyipv8 sets the buffer size for that, which I enlarged
# but it requires sysctl tweaking on Linux.
#
from ipv8.messaging.payload_headers import BinMemberAuthenticationPayload

FRAGMENT_MAX_SIZE = 63 * 1024

STATS_DECAY = 0.75
STATS_DECAY_ALT = 1.0 - STATS_DECAY

@vp_compile
class CrdtStateMessage(VariablePayload):
    msg_id = 1
    format_list = ['I', 'I', 'I', 'I', 'varlenI', 'varlenI']
    names = ['id', 'ttl', 'index', 'total', 'replica_id', 'blob']

@vp_compile
class CrdtRequestMessage(VariablePayload):
    msg_id = 2
    format_list = ['varlenI']
    names = ['replica_id']

class CrdtCommunity(Community):
    community_id = b'deadbeef'

    def __init__(self, my_peer, endpoint, network, ident: str = 'root', state = None):
        if ident:
            self.community_id += ("-" + ident).encode()
        self.community_id = self.community_id.ljust(20, b' ')
        super().__init__(my_peer, endpoint, network)
        self.add_message_handler(1, self.on_state)
        self.add_message_handler(2, self.on_request)
        self.replica = state if not state is None else CrdtSet()
        self.inner_replicas = {}
        self.message_fragments = dict()
        self.chaos_probability = 0
        self.reset_stats()
        self.expected_replicas = dict()

    def broadcast_state(self, replicaid = None):
        if replicaid is None or len(replicaid) == 0:
            replica = self.replica
        else:
            replica = self.inner_replicas[replicaid]
        if not replica.Dirty:
            return
        broadcast_time = perf_counter()
        buffer = pickle.dumps(replica)
        for p in self.get_peers():
            self.send_state(p, replicaid, buffer)
        replica.Dirty = False
        self.stats["last_broadcast_time"] = STATS_DECAY * self.stats["last_broadcast_time"] + STATS_DECAY_ALT * (perf_counter() - broadcast_time)

    def send_state(self, peer, replicaid, buffer):
        id = randint(0, 2 ** 31 - 1)
        fragments = int(ceil(len(buffer) / FRAGMENT_MAX_SIZE))
        if replicaid is None or len(replicaid) == 0:
            replicaid = bytes()
        else:
            replicaid = replicaid.encode()
        for n in range(0, fragments):
            offset = n * FRAGMENT_MAX_SIZE
            length = min(FRAGMENT_MAX_SIZE, len(buffer) - offset)
            self.ez_send(peer, CrdtStateMessage(id, 1, n, fragments, replicaid, buffer[offset:offset+length]))

    def reset_stats(self):
        self.stats = {"merge_count": 0, "drop_count": 0, "pass_count": 0, "last_merge_time": 0, "last_unpickle_time": 0, "last_buffer_fragment_time": 0, "last_broadcast_time": 0}

    @lazy_wrapper(CrdtRequestMessage)
    def on_request(self, peer, payload):
        print("Community getting request from %r" % peer)
        self.broadcast_state(str(payload.replica_id, "utf8"))

    @lazy_wrapper(CrdtStateMessage)
    def on_state(self, peer, payload):
        buffer_fragment_time = perf_counter()
        buffer = None
        if payload.total > 1:
            if not payload.id in self.message_fragments:
                self.message_fragments[payload.id] = list()
            payload_list = self.message_fragments[payload.id]
            payload_list.append(payload)
            if len(payload_list) == payload.total:
                del self.message_fragments[payload.id]
                payload_list.sort(key=lambda p: p.index)
                buffer = bytearray()
                for p in payload_list:
                    buffer.extend(p.blob)
        else:
            buffer = payload.blob
        self.stats["last_buffer_fragment_time"] = STATS_DECAY * self.stats["last_buffer_fragment_time"] + STATS_DECAY_ALT * (perf_counter() - buffer_fragment_time)

        if buffer is None:
            return

        if randint(0, 100) < self.chaos_probability:
            print("Chaos monkey dropped message")
            self.stats["drop_count"] += 1
            return
        else:
            if self.chaos_probability != 0:
                print("Chaos monkey passed message")
            self.stats["pass_count"] += 1

        pickle_time = perf_counter()
        other = pickle.loads(buffer)
        self.stats["last_unpickle_time"] = STATS_DECAY * self.stats["last_unpickle_time"] + STATS_DECAY_ALT * (perf_counter() - pickle_time)
        #if type(other) != type(self.replica):
        #    return

        replicaid = str(payload.replica_id, "utf8")
        target = None
        if len(replicaid) == 0:
            target = self.replica
        else:
            if not replicaid in self.inner_replicas:
                self.inner_replicas[replicaid] = CrdtSet()

            target = self.inner_replicas[replicaid]

        merge_time = perf_counter()
        target.combine(other)
        self.stats["last_merge_time"] = STATS_DECAY * self.stats["last_merge_time"] + STATS_DECAY_ALT * (perf_counter() - merge_time)
        self.stats["merge_count"] += 1
        if replicaid in self.expected_replicas:
            print("Replica %s is expected! Setting result." % replicaid)
            sys.stdout.flush()
            self.expected_replicas[replicaid].set_result(target)
            del self.expected_replicas[replicaid]

    async def get(self, replica_id):
        if replica_id is None:
            replica_id = ""
        if not replica_id in self.expected_replicas:
            expectation = Future()
            # send request message
            self.expected_replicas[replica_id] = expectation

            for p in self.get_peers():
                print("Community getting %s, sending request to %r" % (replica_id, p))
                self.ez_send(p, CrdtRequestMessage(replica_id.encode()))

        return await self.expected_replicas[replica_id]

    def _verify_signature(self, auth: BinMemberAuthenticationPayload, data: bytes) -> Tuple[bool, bytes]:
        return True, data[2 + len(auth.public_key_bin):-default_eccrypto.get_signature_length(default_eccrypto.key_from_public_bin(auth.public_key_bin))]
