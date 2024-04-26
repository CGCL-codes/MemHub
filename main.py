import argparse
import os
import random
from re import T
import shutil
import time
import warnings
from enum import Enum
import multiprocessing
import torch
import torch.backends.cudnn as cudnn
import torch.distributed as dist
import torch.multiprocessing as mp
import torch.nn as nn
import torch.nn.parallel
import torch.optim
import torch.utils.data
import torch.utils.data.distributed
import torchvision.datasets as datasets
import torchvision.models as models
import torchvision.transforms as transforms
from torch.optim.lr_scheduler import StepLR
from torch.utils.data import Subset
from torchLoader import folder, dataloader, distributed
from torchLoader.loader import MemHub_Server as Server

model_names = sorted(name for name in models.__dict__
                     if name.islower() and not name.startswith("__")
                     and callable(models.__dict__[name]))

parser = argparse.ArgumentParser(description='PyTorch ImageNet Training')
parser.add_argument('data', metavar='DIR', nargs='?', default=os.path.join('/home', 'um202173567', 'Workplace', 'data', "imagenet"),
                    help='path to dataset (default: imagenet)')
parser.add_argument('--dataset', default='imagenet',
                    help='name of dataset (default: imagenet)')
parser.add_argument('-a', '--arch', metavar='ARCH', default='resnet50',
                    choices=model_names,
                    help='model architecture: ' +
                    ' | '.join(model_names) +
                    ' (default: resnet50)')
parser.add_argument('--file-nums', default=1281167, type=int, metavar='N',
                    help='number of data  (default: 1281167(imagenet))')
parser.add_argument('--label-classess', default=1000, type=int, metavar='N',
                    help='number of data  (default: 1000(imagenet))')
parser.add_argument('--cache-ratio', default=0.2, type=float, metavar='N',
                    help='cache ratio (default: 0.2)')
parser.add_argument('--chunk-size', default=128, type=int, metavar='N',
                    help='chunk size (default: 128)')

parser.add_argument('-j', '--workers', default=16, type=int, metavar='N',
                    help='number of data loading workers (default: 16)')
parser.add_argument('--epochs', default=100, type=int, metavar='N',
                    help='number of total epochs to run(default: 100)')
parser.add_argument('--start-epoch', default=0, type=int, metavar='N',
                    help='manual epoch number (useful on restarts)(default: 0)')
parser.add_argument('-bs', '--batch-size', default=256, type=int,
                    metavar='N',
                    help='mini-batch size (default: 256), this is the total '
                         'batch size of all GPUs on the current node when '
                         'using Data Parallel or Distributed Data Parallel')
parser.add_argument('--fp16', default=False, action='store_true',
                    help='Use FP16/AMP training')
# Optimization.
parser.add_argument('--lr', type=float, default=None,
                    help='Learning rate')
parser.add_argument('--start-lr', type=float, default=0.1,
                    help='Initial learning rate for warmup(default: 0.1)')
parser.add_argument('--base-batch', type=int, default=256,
                    help='Base batch size for learning rate scaling(default: 256)')
parser.add_argument('--momentum', type=float, default=0.9,
                    help='Momentum(default: 0.9)')
parser.add_argument('--warmup-epochs', type=int, default=10,
                    help='Number of epochs to warm up(default: 10)')
parser.add_argument('--decay-epochs', type=int, nargs='+',
                    default=[30, 60, 80],
                    help='Epochs at which to decay the learning rate(default: [30, 60, 80])')
parser.add_argument('--decay-factors', type=float, nargs='+',
                    default=[0.1, 0.1, 0.1],
                    help='Factors by which to decay the learning rate(default: [0.1, 0.1, 0.1])')
parser.add_argument('--decay', type=float, default=0.0001,
                    help='L2 weight decay(default: 0.0001)')

parser.add_argument('-p', '--print-freq', default=200, type=int,
                    metavar='N', help='print frequency (default: 200)')
parser.add_argument('--resume', default='', type=str, metavar='PATH',
                    help='path to latest checkpoint (default: none)')
parser.add_argument('-e', '--evaluate', dest='evaluate', action='store_true',
                    help='evaluate model on validation set')
parser.add_argument('--pretrained', dest='pretrained', action='store_true',
                    help='use pre-trained model')
parser.add_argument('--world-size', default=3, type=int,
                    help='number of nodes for distributed training(default: 2)')
parser.add_argument('--rank', default=0, type=int,
                    help='node rank for distributed training(default: 0)')
parser.add_argument('--ngpus-per-node', default=1, type=int,
                    help='gpu number per node(default: 1)')

parser.add_argument('--dist-url', default='tcp://192.168.1.171:55555', type=str,
                    help='url used to set up distributed training')
parser.add_argument('--dist-backend', default='nccl', type=str,
                    help='distributed backend')
parser.add_argument('--seed', default=16, type=int,
                    help='seed for initializing training.(default: 16)')
parser.add_argument('--gpu', default=None, type=int,
                    help='GPU id to use.')
parser.add_argument('--multiprocessing-distributed', default=True, action='store_true',
                    help='Use multi-processing distributed training to launch '
                         'N processes per node, which has N GPUs. This is the '
                         'fastest way to use PyTorch for either single node or '
                         'multi node data parallel training')
parser.add_argument('--dummy', action='store_true',
                    help="use fake data to benchmark")

best_acc1 = 0


def main():
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        torch.manual_seed(args.seed)
        cudnn.deterministic = True
        cudnn.benchmark = False
        warnings.warn('You have chosen to seed training. '
                      'This will turn on the CUDNN deterministic setting, '
                      'which can slow down your training considerably! '
                      'You may see unexpected behavior when restarting '
                      'from checkpoints.')

    if args.gpu is not None:
        warnings.warn('You have chosen a specific GPU. This will completely '
                      'disable data parallelism.')

    if args.dist_url == "env://" and args.world_size == -1:
        args.world_size = int(os.environ["WORLD_SIZE"])

    args.distributed = args.world_size > 1 and args.multiprocessing_distributed

    if torch.cuda.is_available():
        args.ngpus_per_node = torch.cuda.device_count()
        args.batch_size = int(args.batch_size / args.ngpus_per_node)
    else:
        args.ngpus_per_node = 1

    if args.multiprocessing_distributed:
        # Start MemHub's Server work
        smp = mp.get_context('spawn')
        new_epoch_queue = smp.Queue()
        synch_queue = smp.Queue()
        server = smp.Process(target=server_worker,
                             args=(new_epoch_queue, synch_queue, args))
        server.start()
        # Call main_worker function
        mp.spawn(main_worker, nprocs=args.ngpus_per_node,
                 args=(args, new_epoch_queue, synch_queue))
    else:
        # Simply call main_worker function
        main_worker(args.gpu, args, new_epoch_queue, synch_queue)

    server.join()


def server_worker(new_epoch_queue, synch_queue, args):
    # MemHub's main logic is implemented in the Server.
    server_so_path = os.path.join(
        'libMemHub', 'server', 'build', 'libSERVER_MEMHUB.so')
    data_dir = os.path.join(args.data, 'train')
    train_server = Server(server_so_path, data_dir, args.chunk_size, args.batch_size, args.cache_ratio,
                          args.world_size, args.workers, args.ngpus_per_node, args.rank, args.seed)
    cur_epoch = -1
    synch_worker_num = 0
    # Epoch synchronization, ensuring all workers start a new epoch simultaneously.
    while True:
        new_epoch = new_epoch_queue.get()
              new_epoch, " cur_epoch: ", cur_epoch)
        if new_epoch == cur_epoch:
            synch_worker_num += 1
        elif new_epoch == cur_epoch+1:
            cur_epoch = new_epoch
            synch_worker_num = 1
        else:
            break
        if synch_worker_num == args.ngpus_per_node:
            train_server.set_epoch(cur_epoch)
            for i in range(args.ngpus_per_node):
                synch_queue.put(1)


def main_worker(gpu, args, new_epoch_queue, synch_queue):
    global best_acc1
    args.gpu = gpu

    if args.gpu is not None:
        print("Use GPU: {} for training".format(args.gpu))

    # Adjust the total world_size due to ngpus_per_node processes per node
    args.world_size = args.ngpus_per_node * args.world_size
    if args.distributed:
        if args.dist_url == "env://" and args.rank == -1:
            args.rank = int(os.environ["RANK"])
        if args.multiprocessing_distributed:
            # Adjust rank to the global rank for multiprocessing distributed training
            args.rank = args.rank * args.ngpus_per_node + gpu
        print("rank:", args.rank)
        dist.init_process_group(backend=args.dist_backend, init_method=args.dist_url,
                                world_size=args.world_size, rank=args.rank)

    # Create model
    if args.pretrained:
        print("=> using pre-trained model '{}'".format(args.arch))
        model = models.__dict__[args.arch](pretrained=True)
    else:
        print("=> creating model '{}'".format(args.arch))
        model = models.__dict__[args.arch]()
        if not args.dataset == 'imagenet':
            if not (args.arch == 'resnet50' or args.arch == 'resnet18' or args.arch == 'resnet101'):
                if args.arch == "densenet121":
                    num_features = model.classifier.in_features
                    model.classifier = nn.Linear(
                        num_features, args.label_classess)
                elif args.arch == "shufflenet_v2_x2_0":
                    model.fc = nn.Linear(
                        model.fc.in_features, args.label_classess)
                else:
                    model.classifier.add_module(
                        "add_linear", nn.Linear(1000, args.label_classess))
            else:
                fc_features = model.fc.in_features
                model.fc = nn.Linear(fc_features, args.label_classess)
    if not torch.cuda.is_available():
        print('using CPU, this will be slow')
    elif args.distributed:
        if torch.cuda.is_available():
            if args.gpu is not None:
                torch.cuda.set_device(args.gpu)
                model.cuda(args.gpu)
                args.workers = int(
                    (args.workers + args.ngpus_per_node - 1) / args.ngpus_per_node)
                model = torch.nn.parallel.DistributedDataParallel(
                    model, device_ids=[args.gpu])
            else:
                model.cuda()
                model = torch.nn.parallel.DistributedDataParallel(model)
    elif args.gpu is not None and torch.cuda.is_available():
        torch.cuda.set_device(args.gpu)
        model = model.cuda(args.gpu)
    else:
        if args.arch.startswith('alexnet') or args.arch.startswith('vgg'):
            model.features = torch.nn.DataParallel(model.features)
            model.cuda()
        else:
            model = torch.nn.DataParallel(model).cuda()

    if torch.cuda.is_available():
        if args.gpu:
            device = torch.device('cuda:{}'.format(args.gpu))
        else:
            device = torch.device("cuda")
    else:
        device = torch.device("cpu")

    # Define loss function (criterion), optimizer, and learning rate scheduler
    criterion = nn.CrossEntropyLoss().to(device)
    # Gradient scaler for AMP
    scaler = torch.cuda.amp.GradScaler(enabled=args.fp16)
    optimizer = torch.optim.SGD(model.parameters(), args.start_lr,
                                momentum=args.momentum,
                                weight_decay=args.decay)
    # Set up learning rate schedule.
    scheduler = get_learning_rate_schedule(optimizer, args)

    # optionally resume from a checkpoint
    if args.resume:
        if os.path.isfile(args.resume):
            print("=> loading checkpoint '{}'".format(args.resume))
            if args.gpu is None:
                checkpoint = torch.load(args.resume)
            elif torch.cuda.is_available():
                # Map model to be loaded to specified single gpu
                loc = 'cuda:{}'.format(args.gpu)
                checkpoint = torch.load(args.resume, map_location=loc)
            args.start_epoch = checkpoint['epoch']
            best_acc1 = checkpoint['best_acc1']
            if args.gpu is not None:
                # best_acc1 may be from a checkpoint from a different GPU
                best_acc1 = best_acc1.to(args.gpu)
            model.load_state_dict(checkpoint['state_dict'])
            optimizer.load_state_dict(checkpoint['optimizer'])
            scheduler.load_state_dict(checkpoint['scheduler'])
            print("=> loaded checkpoint '{}' (epoch {})"
                  .format(args.resume, checkpoint['epoch']))
        else:
            print("=> no checkpoint found at '{}'".format(args.resume))

    # Data loading code

    train_dataset, val_dataset = get_dataset(args)
    if args.distributed:
        train_sampler = distributed.DistributedSampler(
            train_dataset, gpu=args.gpu, gpu_num=args.ngpus_per_node)
        val_sampler = torch.utils.data.distributed.DistributedSampler(
            val_dataset, shuffle=False)
    else:
        train_sampler = None
        val_sampler = None
    train_loader = dataloader.DataLoader(
        train_dataset, batch_size=args.batch_size, shuffle=(
            train_sampler is None),
        num_workers=args.workers, pin_memory=True, sampler=train_sampler, gpu=args.gpu)
    val_loader = torch.utils.data.DataLoader(
        val_dataset, batch_size=args.batch_size, shuffle=False,
        num_workers=args.workers, pin_memory=True, sampler=val_sampler)
    if args.evaluate:
        validate(val_loader, model, criterion, args)
        return

    # Iteratively train
    for epoch in range(args.start_epoch, args.epochs):
        # All workers synchronize to start a new epoch of training together
        if args.distributed:
            new_epoch_queue.put(epoch)
            synch_value = synch_queue.get()
        train(train_loader, model, criterion,
              scaler, optimizer, epoch, device, args)
        acc1 = validate(val_loader, model, criterion, epoch, args)
        scheduler.step()
        is_best = acc1 > best_acc1
        best_acc1 = max(acc1, best_acc1)

        if not args.multiprocessing_distributed or (args.multiprocessing_distributed
                                                    and args.rank % args.ngpus_per_node == 0):
            save_checkpoint({
                'epoch': epoch + 1,
                'arch': args.arch,
                'state_dict': model.state_dict(),
                'best_acc1': best_acc1,
                'optimizer': optimizer.state_dict(),
                'scheduler': scheduler.state_dict()
            }, is_best)

    new_epoch_queue.put(-1)


def get_dataset(args):
    # Dataset for dummy training
    if args.dummy:
        print("=> Dummy data is used!")
        train_dataset = datasets.FakeData(
            1281167, (3, 224, 224), 1000, transforms.ToTensor())
        val_dataset = datasets.FakeData(
            50000, (3, 224, 224), 1000, transforms.ToTensor())
    else:
        traindir = os.path.join(args.data, 'train')
        valdir = os.path.join(args.data, 'val')

        if args.dataset == 'imagenet' or args.dataset == 'imagenet-21k':
            normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                             std=[0.229, 0.224, 0.225])
            train_transforms = transforms.Compose([
                transforms.RandomResizedCrop(224),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                normalize,
            ])
            val_transforms = transforms.Compose([
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                normalize,
            ])
        elif args.dataset == 'cifar-10' or args.dataset == 'cifar-100':
            normalize = transforms.Normalize(mean=[0.5070751592371323, 0.48654887331495095, 0.4409178433670343],
                                             std=[0.2673342858792401, 0.2564384629170883, 0.27615047132568404])
            train_transforms = transforms.Compose([
                transforms.RandomCrop(32, padding=4),
                transforms.RandomHorizontalFlip(),
                transforms.RandomRotation(15),
                transforms.ToTensor(),
                normalize
            ])
            val_transforms = transforms.Compose([
                transforms.ToTensor(),
                normalize
            ])

        train_dataset = folder.ImageFolder(
            traindir,
            train_transforms)
        val_dataset = datasets.ImageFolder(
            valdir,
            val_transforms)

    return train_dataset, val_dataset


def train(train_loader, model, criterion, scaler, optimizer, epoch, device, args):
    batch_time = AverageMeter('Time', ':6.3f', str_sum=True)
    data_time = AverageMeter('Data', ':6.3f', str_sum=True)
    losses = AverageMeter('Loss', ':.4e')
    top1 = AverageMeter('Acc@1', ':6.2f')
    top5 = AverageMeter('Acc@5', ':6.2f')
    progress = ProgressMeter(
        len(train_loader),
        [batch_time, data_time, losses, top1, top5], True, args,
        prefix="Epoch: [{}]".format(epoch))

    # Switch to train mode
    model.train()
    end = time.time()
    for i, (images, target) in enumerate(train_loader):
        # Measure data loading time
        data_time.update(time.time() - end)

        images = images.to(device, non_blocking=True)
        target = target.to(device, non_blocking=True)
        output = model(images)

        loss = criterion(output, target)
        acc1, acc5 = accuracy(output, target, topk=(1, 5))
        losses.update(loss.item(), images.size(0))
        top1.update(acc1[0], images.size(0))
        top5.update(acc5[0], images.size(0))
        scaler.scale(loss).backward()
        scaler.step(optimizer)
        scaler.update()
        optimizer.zero_grad()

        # Measure batch time
        batch_time.update(time.time() - end)
        end = time.time()

        if i % args.print_freq == 0:
            progress.display(i + 1, args.gpu)

    progress.display(i + 1, args.gpu)
    progress.display_summary(args.gpu)


def validate(val_loader, model, criterion, epoch, args):

    def run_validate(loader, base_progress=0):
        with torch.no_grad():
            end = time.time()
            for i, (images, target) in enumerate(loader):
                i = base_progress + i
                if args.gpu is not None and torch.cuda.is_available():
                    images = images.cuda(args.gpu, non_blocking=True)
                if torch.cuda.is_available():
                    target = target.cuda(args.gpu, non_blocking=True)
                output = model(images)
                loss = criterion(output, target)

                acc1, acc5 = accuracy(output, target, topk=(1, 5))
                losses.update(loss.item(), images.size(0))
                top1.update(acc1[0], images.size(0))
                top5.update(acc5[0], images.size(0))

                # Measure batch time
                batch_time.update(time.time() - end)
                end = time.time()

                if i % args.print_freq == 0 and not (args.gpu is not None and args.gpu != 0):
                    progress.display(i + 1)

    batch_time = AverageMeter('Time', ':6.3f', Summary.NONE, str_sum=True)
    losses = AverageMeter('Loss', ':.4e', Summary.NONE)
    top1 = AverageMeter('Acc@1', ':6.2f', Summary.AVERAGE)
    top5 = AverageMeter('Acc@5', ':6.2f', Summary.AVERAGE)
    progress = ProgressMeter(
        len(val_loader) + (args.distributed and (len(val_loader.sampler)
                                                 * args.world_size < len(val_loader.dataset))),
        [batch_time, losses, top1, top5], False, args,
        prefix="(Val) Epoch: [{}]".format(epoch))

    # Switch to evaluate mode
    model.eval()

    run_validate(val_loader)
    if args.distributed:
        top1.all_reduce()
        top5.all_reduce()

    if args.distributed and (len(val_loader.sampler) * args.world_size < len(val_loader.dataset)):
        aux_val_dataset = Subset(val_loader.dataset,
                                 range(len(val_loader.sampler) * args.world_size, len(val_loader.dataset)))
        aux_val_loader = torch.utils.data.DataLoader(
            aux_val_dataset, batch_size=args.batch_size, shuffle=False,
            num_workers=args.workers, pin_memory=True)
        run_validate(aux_val_loader, len(val_loader))

    if not (args.gpu is not None and args.gpu != 0):
        progress.display_summary()

    return top1.avg


def save_checkpoint(state, is_best, filename='checkpoint.pth.tar'):
    torch.save(state, filename)
    if is_best:
        shutil.copyfile(filename, 'model_best.pth.tar')


class Summary(Enum):
    NONE = 0
    AVERAGE = 1
    SUM = 2
    COUNT = 3


class AverageMeter(object):
    """Computes and stores the average and current value"""

    def __init__(self, name, fmt=':f', summary_type=Summary.AVERAGE, str_sum=False):
        self.name = name
        self.fmt = fmt
        self.summary_type = summary_type
        self.str_sum = str_sum
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count

    def all_reduce(self):
        if torch.cuda.is_available():
            device = torch.device("cuda")
        else:
            device = torch.device("cpu")
        total = torch.tensor([self.sum, self.count],
                             dtype=torch.float32, device=device)
        dist.all_reduce(total, dist.ReduceOp.SUM, async_op=False)
        self.sum, self.count = total.tolist()
        self.avg = self.sum / self.count

    def __str__(self):
        fmtstr = '{name} {val' + self.fmt + '} ({avg' + self.fmt + '})'
        if self.str_sum:
            fmtstr += '({sum' + self.fmt+'})'

        return fmtstr.format(**self.__dict__)

    def summary(self):
        fmtstr = ''
        if self.summary_type is Summary.NONE:
            fmtstr = ''
        elif self.summary_type is Summary.AVERAGE:
            fmtstr = '{name} {avg:.3f}'
        elif self.summary_type is Summary.SUM:
            fmtstr = '{name} {sum:.3f}'
        elif self.summary_type is Summary.COUNT:
            fmtstr = '{name} {count:.3f}'
        else:
            raise ValueError('invalid summary type %r' % self.summary_type)

        return fmtstr.format(**self.__dict__)


class ProgressMeter(object):
    def __init__(self, num_batches, meters, is_train, args, prefix=""):
        self.batch_fmtstr = self._get_batch_fmtstr(num_batches)
        self.meters = meters
        self.prefix = prefix

        batch_size = "_batch_size"+str(args.batch_size)
        node_id = args.rank//args.ngpus_per_node

        gpus_per_node = "_gpus_per_node"+str(args.ngpus_per_node)
        nodes_num = args.world_size/args.ngpus_per_node
        nodes = "_nodes"+str(nodes_num)

        cache_ratio = "_cache_ratio"+str(args.cache_ratio)
        chunk_size = "_chunk_size"+str(args.chunk_size)

        if is_train:
            self.log_path = os.path.join('.', 'run_info', str(
                node_id)+args.arch+'_'+args.dataset+cache_ratio+chunk_size+batch_size+nodes+gpus_per_node+'_train_log.txt')
            self.summary_log_path = os.path.join('.', 'run_info', str(
                node_id)+args.arch+'_'+args.dataset+cache_ratio+chunk_size+batch_size+nodes+gpus_per_node+'_summary_train_log.txt')

        else:
            self.log_path = os.path.join('.', 'run_info', str(
                node_id)+args.arch+'_'+args.dataset+cache_ratio+chunk_size+batch_size+nodes+gpus_per_node+'_val_log.txt')
            self.summary_log_path = os.path.join('.', 'run_info', str(
                node_id)+args.arch+'_'+args.dataset+cache_ratio+chunk_size+batch_size+nodes+gpus_per_node+'_summary_val_log.txt')

    def display(self, batch, gpu=0):
        entries = []
        if gpu is not None:
            entries += ["gpu:"+str(gpu)]
        entries += [self.prefix + self.batch_fmtstr.format(batch)]
        entries += [str(meter) for meter in self.meters]
        print('\t'.join(entries))
        with open(self.log_path, mode='a+') as f:
            f.write(' '.join(entries))
            f.write('\n')

    def display_summary(self, gpu=0):
        entries = []
        if gpu is not None:
            entries += ["gpu:"+str(gpu)]
        entries += [self.prefix + " *"]
        entries += [meter.summary() for meter in self.meters]
        print(' '.join(entries))
        with open(self.summary_log_path, mode='a+') as f:
            f.write(' '.join(entries))
            f.write('\n')

    def _get_batch_fmtstr(self, num_batches):
        num_digits = len(str(num_batches // 1))
        fmt = '{:' + str(num_digits) + 'd}'
        return '[' + fmt + '/' + fmt.format(num_batches) + ']'


def accuracy(output, target, topk=(1,)):
    # Computes the accuracy over the k top predictions for the specified values of k
    with torch.no_grad():
        maxk = max(topk)
        batch_size = target.size(0)

        _, pred = output.topk(maxk, 1, True, True)
        pred = pred.t()
        correct = pred.eq(target.view(1, -1).expand_as(pred))
        res = []
        for k in topk:
            correct_k = correct[:k].reshape(-1).float().sum(0, keepdim=True)
            res.append(correct_k.mul_(100.0 / batch_size))
        return res


def get_learning_rate_schedule(optimizer, args):
    # Return the learning rate schedule for training
    # Determine the target LR if needed
    if args.lr is None:
        nodes_num = args.world_size/args.ngpus_per_node
        global_batch_size = args.batch_size * nodes_num
        batch_factor = max(global_batch_size // args.base_batch, 1.0)
        args.lr = args.start_lr * batch_factor
    if args.warmup_epochs:
        target_warmup_factor = args.lr / args.start_lr
        warmup_factor = target_warmup_factor / args.warmup_epochs

    def lr_sched(epoch):
        factor = 1.0
        if args.warmup_epochs:
            if epoch > 0:
                if epoch < args.warmup_epochs:
                    factor = warmup_factor * epoch
                else:
                    factor = target_warmup_factor
        for step, decay in zip(args.decay_epochs, args.decay_factors):
            if epoch >= step:
                factor *= decay
        return factor

    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_sched)


def group_weight_decay(net, decay_factor, skip_list):
    # Set up weight decay groups
    # skip_list is a list of module names to not apply decay to
    decay, no_decay = [], []
    for name, param in net.named_parameters():
        if not param.requires_grad:
            continue
        if any([pattern in name for pattern in skip_list]):
            no_decay.append(param)
        else:
            decay.append(param)
    return [{'params': no_decay, 'weight_decay': 0.0},
            {'params': decay, 'weight_decay': decay_factor}]


if __name__ == '__main__':
    main()
