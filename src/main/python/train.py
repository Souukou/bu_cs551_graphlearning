import logging
import os

import torch
import torch.distributed as dist
from dl_on_flink_framework.context import Context
from dl_on_flink_pytorch.pytorch_context import PyTorchContext
from pyflink.common import Row
from torch import nn
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.optim import SGD
from torch.utils.data import DataLoader
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
    dataset_path = pytorch_context.get_property("dataset_path")
    os.environ["MASTER_ADDR"] = pytorch_context.get_master_ip()
    os.environ["MASTER_PORT"] = str(pytorch_context.get_master_port())
    dist.init_process_group(
        backend="gloo",
        world_size=pytorch_context.get_world_size(),
        rank=pytorch_context.get_rank(),
    )
    if pytorch_context.get_rank() == 0:
        model_save_path = pytorch_context.get_property("model_save_path")
        print("the save path is", model_save_path)
        writer = SummaryWriter(model_save_path+'_tsb')
    
    dataset = pytorch_context.get_dataset_from_flink()
    data_loader = PyG_DataLoader(dataset, batch_size=1)


    reddit_cofig = (301, 256, 41)
    kc_config = (34, 32, 4)
    pub_med_config = (500, 256, 3)
    config_tp = pub_med_config
    gs_model = GS_model(*config_tp, 2)
    ptm_path = '/opt/src/main/python/python_only_test/dataset/pretrianed_graph_sage.pth'
    ptm_dict = torch.load(ptm_path,map_location=torch.device('cpu'))
    gs_model.load_state_dict(ptm_dict)
    model = DDP(gs_model)
    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)

    # where to get the epoch?
    current_epoch = 9999
    logger.info(f"Epoch: {current_epoch}")
    tsb_limit = 90
    
    for batch_idx, data in enumerate(data_loader):
        
        loss = utils.train_step(model, optimizer, data, device = device)
        if pytorch_context.get_rank() == 0:
            writer.add_scalar('training/loss', loss, batch_idx)
        
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
                model_save_path = pytorch_context.get_property("model_save_path")
                logger.info("the save path is" + model_save_path)
                os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
                torch.save(model.module.cpu().state_dict(), model_save_path+'.pth')
                
                if batch_idx >= tsb_limit:
                    writer.close()
                
