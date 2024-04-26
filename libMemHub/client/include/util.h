#ifndef CLIENT_UTIL_H
#define CLIENT_UTIL_H

#include <string>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>
#include <iostream>
#include <codecvt>
#include <locale>

typedef struct TrainDataInfo
{
    char *file_content;
    int label_index;
    unsigned long file_size;
    bool operator==(const TrainDataInfo p)
    {
        return (this->file_content == p.file_content) && (this->label_index == p.label_index) && (this->file_size == p.file_size);
    }
} TrainDataInfo;
#endif
