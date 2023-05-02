import argparse
import collections
import datetime
import json
import pathlib

import kafka
import numpy as np
import rocksdb
import torch_geometric
import tqdm
from protobuf import event_pb2


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pretrained", required=True, type=pathlib.Path)
    parser.add_argument(
        "-k", "--to-kafka", action="store_true", default=False, dest="tokafka"
    )
    parser.add_argument("-m", "--mode", default="train")
    parser.add_argument("-s", "--savedir", default="dataset-test")
    parser.add_argument("-t", "--topic", default="test")
    parser.add_argument("-n", "--neighbor-path", default="neighbors.json")
    parser.add_argument("--servers", default=["localhost:9092"], nargs="+", type=str)
    return parser


class OrderByCount(rocksdb.interfaces.Comparator):
    def compare(self, left, right):
        s0, t0 = left.decode("UTF-8").split("|")
        s1, t1 = right.decode("UTF-8").split("|")
        if int(s0) < int(s1):
            return -1
        if int(s0) > int(s1):
            return 1
        if int(s0) == int(s1):
            if int(t0) < int(t1):
                return -1
            if int(t1) > int(t0):
                return 1
        return 0

    def name(self):
        return "OrderByCount".encode("UTF-8")


class GraphDB:
    def __init__(self, num_nodes, savedir, read_only=False):
        self._read_only = read_only
        opts = GraphDB.get_options()
        path = pathlib.Path(f"{savedir}/nodes.db")
        path.mkdir(exist_ok=True, parents=True)
        self.nodesdb = rocksdb.DB(str(path), opts, read_only=read_only)

        opts = GraphDB.get_options(True)
        path = pathlib.Path(f"{savedir}/edges.db")
        path.mkdir(exist_ok=True, parents=True)
        self.edgesdb = rocksdb.DB(str(path), opts, read_only=read_only)

        opts = GraphDB.get_options()
        self.neighbordb = rocksdb.DB(
            f"{savedir}/neighbor.db", opts, read_only=read_only
        )
        self._nodes = set(range(num_nodes))

    @staticmethod
    def get_options(edges=False):
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
        if edges:
            opts.comparator = OrderByCount()
        return opts

    def disconnected_nodes_so_far(self):
        return self._nodes

    def edge_exists(self, source, target):
        neighbors = self.get_neighbors(source)
        if not neighbors:
            return False
        if (source, target) in neighbors or (target, source) in neighbors:
            return True

        return False

    def get_neighborhood_size(self, source):
        found, data = self.neighbordb.key_may_exist(
            str(source).encode("UTF-8"), fetch=True
        )

        if found:
            if data is None:
                data = self.neighbordb.get(str(source).encode("UTF-8"))
            data = int(data.decode("UTF-8"))
        else:
            data = -1
        return data

    def insert_edge(self, source, target):
        assert not self._read_only
        if self.edge_exists(source, target):
            return

        size = self.get_neighborhood_size(source)
        self.edgesdb.put(
            f"{source}|{size + 1}".encode("UTF-8"), str(target).encode("UTF-8")
        )
        self.neighbordb.put(str(source).encode("UTF-8"), str(size + 1).encode("UTF-8"))

        size = self.get_neighborhood_size(target)
        self.edgesdb.put(
            f"{target}|{size + 1}".encode("UTF-8"), str(source).encode("UTF-8")
        )
        self.neighbordb.put(str(target).encode("UTF-8"), str(size + 1).encode("UTF-8"))

    def insert_node(self, idx, feature, label):
        assert not self._read_only
        mask = 0
        if self.nodesdb.key_may_exist(str(idx).encode("UTF-8"))[0]:
            return

        if idx not in self._nodes:
            return

        label = int(label).to_bytes(4, byteorder="big")
        value = label + feature.tobytes()
        self.nodesdb.put(str(idx).encode("UTF-8"), value)
        self._nodes.remove(idx)

    def insert(self, source, target, features, labels):
        source_feat, target_feat = features
        source_label, target_label = labels

        self.insert_node(source, source_feat, source_label)
        self.insert_node(target, target_feat, target_label)
        self.insert_edge(source, target)

    def get_neighbors(self, source):
        key = f"{source}|0".encode("UTF-8")
        if not self.edgesdb.key_may_exist(key)[0]:
            return set()
        iterator = self.edgesdb.iteritems()
        iterator.seek(key)
        neighbors = set()
        s = t = -1
        for k, v in iterator:
            s = int(k.decode("UTF-8").split("|")[0])
            t = int(v.decode("UTF-8"))
            if s != source:
                break
            neighbors.add((s, t))
            neighbors.add((t, s))
        return neighbors


class DumpToKafka:
    def __init__(self, servers, topic, mode="train"):
        print(f"Trying connect to kafka servers: {servers} for topic: {topic}", end=" ")
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=servers, value_serializer=lambda x: x.SerializeToString()
        )
        print("Connected!")
        self._topic = topic
        self._mode = mode

    def dump_train(self, source, target, labels, feats):
        assert self._mode == "train"
        event = event_pb2.Event()
        event.timestamp.FromDatetime(datetime.datetime.now())
        event.source = source
        event.target = target
        event.source_label = labels[0]
        event.target_label = labels[1]
        event.source_data = feats[0].tobytes()
        event.target_data = feats[1].tobytes()

        self.producer.send(self._topic, value=event)


def main(
    pretrained_graph_path,
    savedir="dataset-test",
    tokafka=False,
    mode="train",
    kafka_topic="test",
    bootstrap_servers=["localhost:9092"],
    neighbor_path="neighbors.json",
):
    assert pretrained_graph_path.exists()
    print("Loading Data")
    pretrained_graph = np.load(str(pretrained_graph_path), allow_pickle=True)[()]
    dataset = torch_geometric.datasets.Planetoid("/opt/data/tmp/pubmed", "PubMed", split ='full')[0]

    print("Converting data to primitives for faster access...", end=" ")
    pt_edges = set(zip(*pretrained_graph["pt_edges"]))
    eis = dataset.edge_index.tolist()
    train_mask = dataset.train_mask.tolist()
    num_edges = dataset.num_edges
    pretrained_graph = pretrained_graph["pt_mask"].tolist()
    allfeats = dataset.x.numpy()#byterizer(dataset.x.numpy()).tolist()
    alllabels = dataset.y.tolist()
    print("Done!")

    if mode == "train" and not tokafka:
        graphdb = GraphDB(dataset.num_nodes, savedir)
        for source, target in tqdm.tqdm(pt_edges):
            feats = [allfeats[source], allfeats[target]]
            labels = [alllabels[source], alllabels[target]]
            graphdb.insert(source, target, feats, labels)

        nodes_so_far = list(graphdb.disconnected_nodes_so_far())
        for node in tqdm.tqdm(nodes_so_far):
          feat = allfeats[source]
          label = alllabels[source]
          graphdb.insert_node(node, feat, label)

    if tokafka:
        kafkadumper = DumpToKafka(bootstrap_servers, kafka_topic, mode)
        graphdb = GraphDB(dataset.num_nodes, savedir, mode == "infer")
        for idx in tqdm.tqdm(range(num_edges)):
            source, target = eis[0][idx], eis[1][idx]
            if (
                train_mask[source]
                and train_mask[target]
                and (source, target) not in pt_edges
                and (target, source) not in pt_edges
            ):
                if mode == "train":
                    kafkadumper.dump_train(
                        source,
                        target,
                        dataset.y[[source, target]].numpy(),
                        dataset.x[[source, target]].numpy(),
                    )


if __name__ == "__main__":
    args, _ = build_parser().parse_known_args()
    main(
        args.pretrained,
        args.savedir,
        args.tokafka,
        args.mode,
        args.topic,
        args.servers,
        args.neighbor_path,
    )
