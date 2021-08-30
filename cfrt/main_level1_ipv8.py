#!/usr/bin/python3

from CrdtCommunity import CrdtCommunity

from asyncio import ensure_future, get_event_loop
from ipv8.configuration import ConfigBuilder, Strategy, WalkerDefinition, default_bootstrap_defs
from ipv8_service import IPv8

async def start_communities():
    for i in [1, 2, 3, 4]:
        builder = ConfigBuilder().clear_keys().clear_overlays()
        builder.add_key("peer", "medium", "ec%s.pem" % i)
        builder.add_overlay("CrdtCommunity", "peer", [WalkerDefinition(Strategy.RandomWalk, 10, {'timeout': 3.0})],
                            default_bootstrap_defs, {}, [('started',)])
        ipv8 = IPv8(builder.finalize(), extra_communities={'CrdtCommunity': CrdtCommunity})
        await ipv8.start()

ensure_future(start_communities())
get_event_loop().run_forever()