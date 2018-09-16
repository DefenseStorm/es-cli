
import errno
import logging

from ..utils import shards as esshards
from ..utils import humansize

logger = logging.getLogger("es-shards")


def execute(args):
    if args.only_warm:
        shards = esshards.get_shards(include_all_status=True, include_hot=False, include_warm=True,
                                     include_percolate=False)
    elif args.only_percolate:
        shards = esshards.get_shards(include_all_status=True, include_hot=False, include_warm=False,
                                     include_percolate=True)
    else:
        shards = esshards.get_shards(include_all_status=True, include_hot=True, include_warm=False,
                                     include_percolate=False)

    try:
        if args.summary:
            show_summary(shards)
        else:
            show_details(shards)
    except IOError as e:
        # A SIGPIPE is normal when `es-cli` is piped to a command that ends prematurely (like `es shards | head`)
        if e.errno == errno.EPIPE:
            logger.debug("Broken Pipe")
        else:
            raise


def show_details(shards):
    str_format = "{0:50s}\t{1}\t{2}\t{3}\t{4:35s}\t{5}\t{6}"
    print(str_format.format('INDEX', 'NUM', 'TYPE', 'SIZE', 'NODE', 'STATUS', ''))
    for shard in shards:
        print(str_format.format(shard['index'], 
                                shard['num-shard'],
                                shard['shard-type'],
                                shard['size'],
                                shard['node'],
                                shard['status'],
                                shard['extra']))


def show_summary(shards):
    grouped_shards = {shard['node']: esshards.summarize_shards(shard['node'], shards) for shard in shards}

    str_format = "{0:40s}\t{1}\t{2}"
    print(str_format.format('NODE', 'NUM', 'SIZE'))
    for node in sorted(grouped_shards.keys()):
        print(str_format.format(node,
                                grouped_shards[node][0],
                                humansize.stringify(grouped_shards[node][1])))
