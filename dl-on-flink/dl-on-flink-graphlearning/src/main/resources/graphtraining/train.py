#import models
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PytorchContext
from pyflink.common import Row



def train(context: Context):
  pytorch_context = PytorchContext(context)
  os.environ['MASTER_ADDR'] = pytorch_context.get_master_port()
  dist.init_process_group(backend='gloo', world_size=pytorch_context.get_master_port(), rank=pytorch_context.get_rank())
  breakpoint()
  data_loader = DataLoader(pytorch_context.get_dataset.from_flink(), batch_size=128)

#  model = DDP(models.GCN())
#
#  loss_fn = torch.nn.MSELoss()
#
#  optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
#
#  current_epoch = 1
