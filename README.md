# MemHub

## Vision
MemHub is a novel memory management system for DLT with limited memory resources. Itadopts a novel N-to-M mapping between data and memory to enable efficient random data access. This mapping mechanism enables the optimization of batch data loading from storage and batch data prefetching from distributed nodes to minimize the I/O overhead.

## Install prerequisites
- Python 3.8
- Pytorch 1.7.0
- CUDA 11.2
- CMAKE 3.18.0
- GCC 7.5.0
- libconfig
- gRPC
## Compile the system
### Compile the server of MemHub
 ```bash
  mkdir libMemHub/server/build/
  cd libMemHub/server/build/
  cmake ../
  make
  ```
### Compile the client of MemHub
  ```bash
  mkdir libMemHub/client/build/
  cd libMemHub/client/build/
  cmake ../
  make
  ```
## Run the System
### Setting Up Configuration Information
Before running the system, you'll need to set up the configuration parameters according to your training requirements. Follow these steps:
```bash
cd  libMemHub/configure/
vim grpc_configure.cfg
```
Within this file, you can adjust the following configurations:

- `max_check_num`: This parameter determines the maximum number of data items to check during target node prefetching in cross-node sharing.
- `max_reply_num`: Sets the maximum number of data items to reply on the target node during cross-node sharing.
- `node_ip_port`: Specifies communication information for each node involved in cross-data sharing.

### Model Training
Based on MemHub, training a model is not much different from training a model using native PyTorch. Simply execute the following command:
```bash
source run.sh
```
This script will initiate the system and run it according to the specified training configuration, dataset, and model information. You can set these parameters in the `run.sh` file, and then simply execute the above command to start running the system.

It's worth noting that MemHub has two special parameters:

- `chunk-size`: Specifies the size of the data chunk used for this training, representing the number of training data contained in each data chunk.
- `cache-ratio`: Specifies the ratio of dataset to be cached in memory for this training.




