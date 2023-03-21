import utils 
import model
from new_context import NewPyTorchContext
import torch


def train(context):
  print("Inside train")
  pc = NewPyTorchContext(context)
  dataloader = torch.utils.data.DataLoader(pc.get_dataset_from_flink())
  for data in dataloader:
    break

def inference(context):
  print("inside inference")
