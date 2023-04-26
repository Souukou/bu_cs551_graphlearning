import logging
import os

import torch
import torch.distributed as dist
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from pyflink.common import Row
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP
# from torch.optim import SGD
# from torch.utils.data import DataLoader
from torch_geometric.data import Data as PyG_Data
from torch_geometric.loader import DataLoader as PyG_DataLoader
from tensorboardX import SummaryWriter
import modelio
import utils
from model import GCN, GS_model
from new_pycontext import NewPyTorchContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def train(context: Context):
    pytorch_context = NewPyTorchContext(context)
    os.environ["MASTER_ADDR"] = pytorch_context.get_master_ip()
    os.environ["MASTER_PORT"] = str(pytorch_context.get_master_port())
    dist.init_process_group(
        backend="gloo",
        world_size=pytorch_context.get_world_size(),
        rank=pytorch_context.get_rank(),
    )
    
    if pytorch_context.get_rank() == 0:
        model_save_path = pytorch_context.get_property("model_save_path")
        tsb_limit = 90
        writer = SummaryWriter(model_save_path+'_tsb_valid')
    
    dataset = pytorch_context.get_dataset_from_flink()
    data_loader = PyG_DataLoader(dataset, batch_size=1)

    reddit_cofig = (301, 256, 41)
    kc_config = (34, 32, 4)
    config_tp = kc_config
    

    model_save_path = pytorch_context.get_property("model_save_path")
    

    gs_model = GS_model(*config_tp, 2)
    ptm_path = model_save_path + '.pth'
    ptm_dict = torch.load(ptm_path,map_location=torch.device('cpu'))
    gs_model.load_state_dict(ptm_dict)
    model = gs_model
    model = model.to(device)
    
    current_epoch = 9999
    logger.info(f"Epoch: {current_epoch}")
    
    for batch_idx, data in enumerate(data_loader):
        
        loss, acc = utils.val_step(model, data, device = device)
        if pytorch_context.get_rank() == 0:
            writer.add_scalar('validation/loss', loss, batch_idx)
            writer.add_scalar('validation/acc', acc, batch_idx)
        
        logger.info(
                f"batch: {batch_idx} "
                f"loss: {loss:>7f}"
            )
        if (batch_idx + 1) % 100 == 0:
            logger.info(
                f"rank: {pytorch_context.get_rank()} batch: {batch_idx} "
                f"loss: {loss:>7f}"
            )
            if pytorch_context.get_rank() == 0:
                if batch_idx >= tsb_limit:
                    writer.close()
                
                
