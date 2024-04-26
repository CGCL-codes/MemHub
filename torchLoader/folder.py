import os
import os.path
from PIL import Image
from .loader import MemHub_Client as Loader
from torchvision.datasets import VisionDataset


def has_file_allowed_extension(filename, extensions):
    """Checks if a file is an allowed extension.

    Args:
        filename (string): path to a file
        extensions (tuple of strings): extensions to consider (lowercase)

    Returns:
        bool: True if the filename ends with one of given extensions
    """
    return filename.lower().endswith(extensions)


def cal_sample_size(directory, extensions=None, is_valid_file=None):
    sample_size = 0
    directory = os.path.expanduser(directory)
    classes = [d.name for d in os.scandir(directory) if d.is_dir()]
    both_none = extensions is None and is_valid_file is None
    both_something = extensions is not None and is_valid_file is not None
    if both_none or both_something:
        raise ValueError(
            "Both extensions and is_valid_file cannot be None or not None at the same time")
    if extensions is not None:
        def is_valid_file(x):
            return has_file_allowed_extension(x, extensions)
    for target_class in classes:
        target_dir = os.path.join(directory, target_class)
        if not os.path.isdir(target_dir):
            continue
        for root, _, fnames in sorted(os.walk(target_dir, followlinks=True)):
            for fname in sorted(fnames):
                path = os.path.join(root, fname)
                if is_valid_file(path):
                    sample_size += 1
    return sample_size


class DatasetFolder(VisionDataset):
    """A generic data loader where the origin dataset is arranged in this way: ::

        root/class_x/xxx.ext
        root/class_x/xxy.ext
        root/class_x/xxz.ext

        root/class_y/123.ext
        root/class_y/nsdf3.ext
        root/class_y/asd932_.ext

    Args:
        root (string): Root directory path.
        extensions (tuple[string]): A list of allowed extensions.
            both extensions and is_valid_file should not be passed.
        transform (callable, optional): A function/transform that takes in
            a sample and returns a transformed version.
            E.g, ``transforms.RandomCrop`` for images.
        target_transform (callable, optional): A function/transform that takes
            in the target and transforms it.
        is_valid_file (callable, optional): A function that takes path of a file
            and check if the file is a valid file (used to check of corrupt files)
            both extensions and is_valid_file should not be passed.
    """

    def __init__(self, root, extensions=None, transform=None,
                 target_transform=None, is_valid_file=None, train=1):
        super(DatasetFolder, self).__init__(root, transform=transform,
                                            target_transform=target_transform)
        self.train = train
        self.sample_size = cal_sample_size(root, extensions, is_valid_file)
        if self.sample_size == 0:
            msg = "Found 0 files in subfolders of: {}\n".format(self.root)
            if extensions is not None:
                msg += "Supported extensions are: {}".format(
                    ",".join(extensions))
            raise RuntimeError(msg)

    def _init_loader(self, worker_id):
        self.worker_id = worker_id
        so_path = os.path.join('libMemHub', 'client',
                               'build', 'libCLIENT_MEMHUB.so')
        self.loader = Loader(so_path, worker_id, self.train)

    def __getitem__(self, index):
        """
        Args:
            index (int): Represents the index of a random access request in the random access sequence.
        Returns:
            tuple: (sample, target) where target is class_index of the target class.
        """
        sample, target = self.loader[index]
        if self.transform is not None:
            sample = self.transform(sample)
        if self.target_transform is not None:
            target = self.target_transform(target)

        return sample, target

    def __len__(self):
        return self.sample_size


IMG_EXTENSIONS = ('.jpg', '.jpeg', '.png', '.ppm', '.bmp',
                  '.pgm', '.tif', '.tiff', '.webp')


class ImageFolder(DatasetFolder):
    """
    Args:
        root (string): Root directory path for the origin dataset.
        transform (callable, optional): A function/transform that  takes in an PIL image
            and returns a transformed version. E.g, ``transforms.RandomCrop``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        loader (callable, optional): A function to load an image given its path.
        is_valid_file (callable, optional): A function that takes path of an Image file
            and check if the file is a valid file (used to check of corrupt files)
    """

    def __init__(self, root, transform=None, train=1, target_transform=None, is_valid_file=None):
        super(ImageFolder, self).__init__(root, IMG_EXTENSIONS if is_valid_file is None else None,
                                          transform=transform,
                                          target_transform=target_transform,
                                          is_valid_file=is_valid_file,
                                          train=train)
