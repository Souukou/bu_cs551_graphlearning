import io
import mmap

import filelock
import torch

lock = filelock.FileLock("model.lock")


def savemodel(model, filename):
    with io.open(filename, mode="wb") as f, lock:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE) as mmap_obj:
            torch.save(model, mmap_obj)


def readmodel(filename):
    with io.open(filename, mode="rb") as f, lock:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmap_obj:
            return torch.load(mmap_obj)
