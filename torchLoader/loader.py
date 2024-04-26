
import os
from ctypes import *
import io
import base64
import time
from PIL import Image
import numpy as np
# import cv2
#from turbojpeg import TurboJPEG, TJPF_GRAY, TJSAMP_GRAY, TJFLAG_PROGRESSIVE, TJFLAG_FASTUPSAMPLE, TJFLAG_FASTDCT

# https://blog.csdn.net/joeblackzqq/article/details/10441017


"""
    TrainDataInfo: Information about the training data returned by the Client of MemHub in C++.
    Attributes:
        file_content (POINTER(c_char)): Pointer of the training data.
        label_index (c_int): Index representing the label of the training data.
        file_size (c_long): Size of the training data.
"""


class TrainDataInfo(Structure):
    _fields_ = [("file_content", POINTER(c_char)),
                ("label_index", c_int), ("file_size", c_long)]


class MemHub_Client:
    def __init__(self, lib_dir, job_id, train):
        self.job_id = job_id
        self.lib = CDLL(lib_dir)
        self.lib.Setup(job_id, train)

    """
        Send data request to and get training data from MemHub
    Attributes:
        request_index: Represents the index of a random access request in the random access sequence, not the actual data index.
         In the Server of MemHub, request_index will be mapped to the actual data index.
"""

    def __getitem__(self, request_index):

        self.lib.get_sample.restype = POINTER(TrainDataInfo)
        data_info_p = self.lib.get_sample(request_index)
        label = data_info_p.contents.label_index
        file_size = data_info_p.contents.file_size
        content = data_info_p.contents.file_content[0:file_size]
        # Delete the pointer allocated by C++.
        self.lib.delete_char_pointer(data_info_p.contents.file_content)
        self.lib.delete_train_data_info_pointer(data_info_p)
        # Convert binary data to Image format.
        byte_img = io.BytesIO(content)
        img = Image.open(byte_img).convert('RGB')
        return img, label


class MemHub_Server:
    def __init__(self, lib_dir, dataset_dir, chunk_size, batch_size, cache_ratio,
                 nodes_num, worker_num, gpu_num_per_node, rank,
                 seed, is_train=1):
        self.lib = CDLL(lib_dir)
        self.lib.Setup(c_wchar_p(dataset_dir), chunk_size, batch_size, c_float(cache_ratio),
                       nodes_num, worker_num, gpu_num_per_node, rank,
                       seed, is_train)

    def set_epoch(self, epoch):
        self.lib.SetEpoch(epoch)
