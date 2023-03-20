import utils 
import model
from new_context import NewPyTorchContext


def train(context):
  print("Inside train")
  pc = NewPyTorchContext(context)
  pc.get_dataset_from_flink()

def inference(context):
  print("inside inference")
