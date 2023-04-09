import rocksdb
import tqdm
import torch_geometric
import numpy as np

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

import os
if not os.path.exists('dataset-test'):
   os.makedirs('dataset-test')
    
dataset = torch_geometric.datasets.KarateClub()[0]

opts = rocksdb.Options()
opts.create_if_missing = True
opts.max_open_files = 300000
opts.write_buffer_size = 67108864
opts.max_write_buffer_number = 3
opts.target_file_size_base = 67108864
opts.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))


nodesdb = rocksdb.DB("dataset-test/nodes.db", opts)


opts2 = rocksdb.Options()
opts2.create_if_missing = True
opts2.comparator = OrderBySourceNode()
opts2.max_open_files = 300000
opts2.write_buffer_size = 67108864
opts2.max_write_buffer_number = 3
opts2.target_file_size_base = 67108864
opts2.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

edgesdb = rocksdb.DB("dataset-test/edges.db", opts2)

opts = rocksdb.Options()
opts.create_if_missing = True
opts.max_open_files = 300000
opts.write_buffer_size = 67108864
opts.max_write_buffer_number = 3
opts.target_file_size_base = 67108864
opts.inplace_update_support = True
opts.allow_concurrent_memtable_write = False
opts.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))
neighbordb = rocksdb.DB("dataset-test/neighbor.db", opts)

for idx in tqdm.tqdm(range(dataset.num_nodes)):
  x = dataset.x[idx].numpy()
  y = dataset.y[idx].numpy()
  mask = 0 # Train Mask
#  mask = 1 & int(dataset.val_mask[idx])
#  mask = 2 & int(dataset.test_mask[idx])
  # First 8 java bytes is mask
  # Next 8 java bytes is label
  # Next remaining bytes == numpy array of feature vector
  value = mask.to_bytes(2, byteorder="big") + int(y).to_bytes(4, byteorder="big")
  value = value + x.tobytes()
  nodesdb.put(str(idx).encode("UTF-8"), value)

for idx in tqdm.tqdm(range(dataset.num_edges)):
  source, target = sorted(list(dataset.edge_index[:, idx].numpy()))
  key = f"{source}|{target}".encode('UTF-8')
  neighbordb.put(str(source).encode('UTF-8'), key)
  edgesdb.put(key, b'\x01')

  
  key = f"{target}|{source}".encode('UTF-8')
  neighbordb.put(str(target).encode('UTF-8'), key)
  edgesdb.put(key, b'\x01')
