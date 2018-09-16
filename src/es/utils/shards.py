
from functools import reduce
from operator import add
import re
from typing import NamedTuple, List

import requests

from . import config as es_config
from . import humansize


class EsShard(NamedTuple):
    node_type: str
    node: str
    index: str
    shard_type: str
    num_shard: int
    size: str
    status: str
    extra: str


class SummarizedShards(NamedTuple):
    node: str
    size: str
    amount: int


def _match_to_shard(re_match) -> EsShard:
    """
    Returns an InternalShard.
    """
    return EsShard(node_type=re_match.group('nodetype'),
                   index=re_match.group('index'),
                   num_shard=re_match.group('numshard'),
                   status=re_match.group('status'),
                   size=re_match.group('size'),
                   node=re_match.group('node'),
                   shard_type=re_match.group('shardtype'),
                   extra=re_match.group('extra'))


def get_shards(include_hot=True, include_warm=False, include_percolate=False,
               include_all_status=False) -> List[EsShard]:
    """
        Retrieves the list of shards from ES and returns a list of EsShard. It respects the shard type and status type
        passed as parameters.
    """
    env = es_config.env()
    shard_regex = r'^(?P<index>\S+)\s+' \
                  r'(?P<numshard>\d+)\s+' \
                  r'(?P<shardtype>[pr])\s+' \
                  r'(?P<status>\S+)\s+' \
                  r'\d+\s+' \
                  r'(?P<size>\S+)\s+' \
                  r'\S+\s+' \
                  r'(?P<node>{}-es-(?P<nodetype>data-hot|data-warm|percolate)-\S+)\s*' \
                  r'(?P<extra>.*)$'
    pattern = re.compile(shard_regex.format(env))
    lines = requests.get("{}/_cat/shards".format(es_config.es_host()), stream=True).iter_lines()

    shards = []
    for shard_line in lines:
        shard_line = shard_line.decode('utf8')
        match = pattern.match(shard_line)
        if match:
            shard = _match_to_shard(match)
            if include_hot and shard.node_type == 'data-hot':
                shards.append(shard)
            elif include_warm and shard.node_type == 'data-warm':
                shards.append(shard)
            elif include_percolate and shard.node_type == 'percolate':
                shards.append(shard)

    # Remove non-started shards, unless we explicitly want them
    shards = [shard for shard in shards if shard.status == 'STARTED' or include_all_status]
    return shards


def summarize_shards(shards: List[EsShard]) -> List[SummarizedShards]:
    """
    Summarizes shards grouping them by node.
    """
    grouped = {}
    for shard in shards:
        if shard.node not in grouped:
            grouped[shard.node] = []

        grouped[shard.node].append(humansize.parse(shard.size))

    summaries = []
    for node, sizes in grouped.items():
        total_size = reduce(add, sizes)
        summaries.append(SummarizedShards(node=node,
                                          size=total_size,
                                          amount=len(sizes)))
    return summaries
