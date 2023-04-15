import argparse

import kafka
import numpy as np
import rocksdb
import torch_geometric
import tqdm
import datetime

from protobuf import event_pb2 


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-k",
        "--dump-to-kafka",
        default=False,
        action="store_true",
        dest="dump_to_kafka",
    )
    parser.add_argument(
        "-r",
        "--dump-to-rocksdb",
        default=False,
        action="store_true",
        dest="dump_to_rocksdb",
    )
    parser.add_argument("-s", "--savedir", default="dataset-test")
    parser.add_argument("-t", "--topic", default="test")
    parser.add_argument("--servers", default=["localhost:9092"], nargs="+", type=str)
    return parser


# Type Labels
# 0 -> Train
# 1 -> Val
# 2 -> Test


class OrderBySourceNode(rocksdb.interfaces.Comparator):
    def compare(self, left, right):
        s0, t0 = left.decode("UTF-8").split("|")
        s1, t1 = right.decode("UTF-8").split("|")
        if int(s0) < int(s1):
            return -1
        if int(s1) > int(s0):
            return 1
        return 0

    def name(self):
        return "OrderBySourceNode".encode("UTF-8")


class GraphDB:
    def __init__(self, num_nodes, read_only=False):
        self._read_only = read_only
        opts = GraphDB.get_options()
        self.nodesdb = rocksdb.DB(f"{savedir}/nodes.db", opts, read_only=read_only)

        opts = GraphDB.get_options()
        self.edgesdb = rocksdb.DB(f"{savedir}/edges.db", opts, read_only=read_only)

        opts = GraphDB.get_options()
        self.neighbordb = rocksdb.DB(
            f"{savedir}/neighbor.db", opts, read_only=read_only
        )
        self._nodes = set(range(num_nodes))

    @staticmethod
    def get_options():
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.max_open_files = 300000
        opts.write_buffer_size = 67108864
        opts.max_write_buffer_number = 3
        opts.target_file_size_base = 67108864
        opts.table_factory = rocksdb.BlockBasedTableFactory(
            filter_policy=rocksdb.BloomFilterPolicy(10),
            block_cache=rocksdb.LRUCache(2 * (1024**3)),
            block_cache_compressed=rocksdb.LRUCache(500 * (1024**2)),
        )
        return opts

    def disconnected_nodes_so_far(self):
        return self._nodes

    def insert_edge(self, source, target):
        assert not self._read_only
        key = f"{source}|{target}".encode("UTF-8")
        self.neighbordb.put(str(source).encode("UTF-8"), key)
        self.edgesdb.put(key, b"\x01")

        key = f"{target}|{source}".encode("UTF-8")
        self.neighbordb.put(str(target).encode("UTF-8"), key)
        self.edgesdb.put(key, b"\x01")

    def insert_node(self, idx, feature, label):
        assert not self._read_only
        mask = 0
        value = mask.to_bytes(2, byteorder="big") + int(label).to_bytes(
            4, byteorder="big"
        )
        value = value + feature.tobytes()
        self.nodesdb.put(str(idx).encode("UTF-8"), value)
        self._nodes.remove(idx)

    def insert(source, target, features, labels):
        source_feat, target_feat = features
        source_label, target_label = labels

        self.insert_node(source, source_feat, source_label)
        self.insert_node(target, target_feat, target_label)
        self.insert_edge(source, target)


class DumpToKafka:
    def __init__(self, servers, topic):
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=servers, value_serializer=lambda x: x.SerializeToString()
        )
        self._topic = topic

    def dump(self, source, target, labels, feats):
        event = event_pb2.Event()
        event.timestamp.FromDatetime(datetime.datetime.now())
        event.source = source
        event.target = target
        event.source_label = labels[0]
        event.target_label = labels[1]
        event.source_data_hex = feats[0].tobytes().hex()
        event.target_data_hex = feats[1].tobytes().hex()

        self.producer.send(self._topic, value=event)


def main(
    dump_to_kafka=True,
    dump_to_rocksdb=True,
    savedir="dataset-test",
    kafka_topic="test",
    bootstrap_servers=["localhost:9092"],
):
    dataset = torch_geometric.datasets.KarateClub()[0]

    if dump_to_rocksdb:
        graphdb = GraphDB(dataset.num_nodes)
    if dump_to_kafka:
        kafkadumper = DumpToKafka(bootstrap_servers, kafka_topic)

    for idx in tqdm.tqdm(range(dataset.num_edges)):
#        if not dataset.val_mask[source] or not dataset.test_mask[source]:
#            continue
        source, target = sorted(list(dataset.edge_index[:, idx].numpy()))
        feats = [dataset.x[source].numpy(), dataset.x[target].numpy()]
        labels = [dataset.y[source].numpy(), dataset.y[target].numpy()]
        if dump_to_rocksdb:
            graphdb.insert(source, target, feats, labels)
        if dump_to_kafka:
            kafkadumper.dump(
                source,
                target,
                dataset.y[[source, target]].numpy(),
                dataset.x[[source, target]].numpy(),
            )

    if dump_to_rocksdb:
        for node in graphdb.disconnected_nodes_so_far():
            graphdb.insert_node(node, dataset.x[node].numpy(), dataset.y[node].numpy())


if __name__ == "__main__":
    args, _ = build_parser().parse_known_args()
    main(
        args.dump_to_kafka, args.dump_to_rocksdb, args.savedir, args.topic, args.servers
    )
