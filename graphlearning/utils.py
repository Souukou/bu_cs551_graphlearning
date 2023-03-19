import torch

def train_step(model, optimizer, data_step, iter_num = 10):
    model.train()
    # loss_ls = []
    for epoch in range(iter_num):
        optimizer.zero_grad()
        out = model(data_step)
        loss = F.nll_loss(out, data_step.y)
        loss.backward()
        # loss_ls.append(loss.item())
        optimizer.step()
    return loss.item()

def val_step(model, data_step):
    model.eval()
    out = model(data_step)
    loss = F.nll_loss(out, data_step.y)

    return loss.item()

def infer_step(model, data_step):
    model.eval()
    out = model(data_step)

    return out
