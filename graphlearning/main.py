import utils 
import model
from new_context import NewPyTorchContext


def train(context):
  pc = NewPyTorchContext(context)
  pc.get_dataset_from_flink()

def inference(context):
  print("inside inference")
