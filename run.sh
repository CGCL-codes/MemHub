python -u main.py  --rank=$1 --world-size=3 --workers=4  --dist-url=tcp://192.168.1.171:55500 --dataset=imagenet --file-nums=1281167 --label-classess=1000 --arch=resnet50 --batch-size=128 --chunk-size=64 --cache-ratio=0.5

