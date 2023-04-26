import pandas as pd
import numpy as np
from yhs_flink_dst import TestFlinkParser

def decode_edge(edge_str):
    edge_strs = edge_str.split('|')
    edges = [[],[]]
    for estr in edge_strs:
        node1, node2 = list(map(int, estr.split('-')))
        edges[0] += [node1, node2]
        edges[1] += [node2, node1]
    return edges

df = pd.read_csv('/opt/src/main/python/python_only_test/sample_data.csv')
for idx, key in enumerate(["src", "edges"]):
    print(key, df[key][0].__class__)
    if key == "edges":
        edge_str = df[key][0]
        print(edge_str)
        edges = decode_edge(edge_str)
        print(edges)
        tester = TestFlinkParser()
        # tester.setUp()
        tester.test_flink_parser_setup(edges)





