import math
import torch
from torch.utils.data import Sampler
import torch.distributed as dist


class DistributedSampler(Sampler):
    """
        In MemHub, the DistributedSampler generates the request index sequence for each worker. 
        The random access sequence (RAS) is created by the random_sampler in MemHub's Server using a random seed. 
        Each training node handles the training task on a section of the random access sequence. 
        For example:
            - With two training nodes, each containing two workers:
                Training data: {0,1,2,3,4,5,6,7} → Random Access Sequence (RAS): {2,6,0,4,1,7,5,3}
                RAS for node'0: {2,0,1,5}; RAS for node'1: {6,4,7,3}
        Therefore:
            - The request index sequence for each node corresponds to the indices in the Random Access Sequence: {0,1,2,3}.
            - For each node, worker'0 has indices {0,1}, and worker'1 has indices {2,3}.
        In MemHub's Server, the request index will be mapped to the data index in the random access sequence.

        Arguments:
            dataset: Dataset used for sampling.
            num_replicas (int, optional): Number of processes participating in distributed training. 
                By default, :attr:`rank` is retrieved from the current distributed group.
            gpu: GPU ID in each node.
            gpu_num: Number of GPUs in each node.
    """

    def __init__(self, dataset, num_replicas=None, rank=None, gpu=0, gpu_num=1):
        self.dataset = dataset
        if num_replicas is None:
            if not dist.is_available():
                raise RuntimeError(
                    "Requires distributed package to be available")
            num_replicas = dist.get_world_size()
        self.num_replicas = num_replicas
        if rank is None:
            if not dist.is_available():
                raise RuntimeError(
                    "Requires distributed package to be available")
            rank = dist.get_rank()
        self.rank = rank
        self.gpu = gpu
        self.gpu_num = gpu_num
        self.init_indices()

    def init_indices(self):
        total_size = len(self.dataset)
        nodes_num = self.num_replicas // self.gpu_num
        node_id = self.rank // self.gpu_num
        node_indices_num = (total_size - node_id + nodes_num - 1) // nodes_num
        gpu_indices_num = math.ceil(node_indices_num/self.gpu_num)
        self.indices = [idx for idx in range(node_indices_num)]
        self.indices = self.indices[self.gpu *
                                    gpu_indices_num:(self.gpu+1)*gpu_indices_num]
        self.num_samples = len(self.indices)

    def __iter__(self):
        return iter(self.indices)

    def __len__(self):
        return self.num_samples
