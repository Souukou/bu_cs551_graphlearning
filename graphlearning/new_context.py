from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from dl_on_flink_pytorch.flink_stream_dataset import FlinkStreamDataset, DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE
from io import StringIO

class NewPyTorchContext(PyTorchContext):
    def get_dataset_from_flink(self) -> FlinkStreamDataset:
        """
        Get the data loader to read data from Flink.
        """
        return NewFlinkStreamDataset(self)

class NewFlinkStreamDataset(FlinkStreamDataset):
    def __init__(self, context: Context):
        super(NewFlinkStreamDataset, self).__init__(context)
        # get the byte length here
        # self.byte_len = int(self.pytorch_context.get_property('BYTE_LEN'))

    def decoding_features(self, feats_str):
        assert len(feats_str) % self.byte_len == 0
        num_feats = len(feats_str) // self.byte_len
        feat_strs = [feats_str[i * self.byte_len : (i+1) * self.byte_len] for i in range(num_feats)]
        # modify this to decode the feat
        feats = [np.frombuffer(feat_str, np.dtype(int)) for feat_str in feat_strs]
        return feats
    def decoding_neighbors(self, feats_str):
        neighbor_ls = map(int, feats_str.decode("utf-8").split(','))
        self.byte_len = len(neighbor_ls) + 1
        return neighbor_ls
    def decoding_single(self, feats_str):
        return int(feats_str.decode("utf-8"))
    def parse_record(self, record):
        df = pd.read_csv(StringIO(record), header=None)
        tensors = []
        print("inside parse record")
        for idx, key in enumerate(df.columns):
            print(idx, key)
            tensors.append(tensor.tensors(1.))
            continue
            if key not in ['feature', 'neighbor']:
                cur = torch.tensor([self.decoding_single(df[key][0])], dtype=DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
            elif key == 'neighbor':
                cur = torch.tensor(self.decoding_neighbors(df[key][0]), dtype=DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]])
            elif key == 'feature':
                feats = self.decoding_features(df[key][0], self.byte_len)
                feat_dtype = DL_ON_FLINK_TYPE_TO_PYTORCH_TYPE[self.input_types[idx]]
                feat_tensors = [torch.from_numpy(feat).unsqueeze(0).to(feat_dtype) for feat in feats]
                cur = torch.cat(feat_tensors)
            tensors.append(cur)
        return tensors

