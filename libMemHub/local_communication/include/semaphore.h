/**
 * Synchronizes access to shared resources between threads
 */

#ifndef SEMAPHORE_H
#define SEMAPHORE_H

#include "util.h"
class Semaphore
{

public:
    explicit Semaphore(int count = 0);
    void Signal();
    void Wait();

private:
    std::mutex m_mutex;
    std::condition_variable m_condition_var;
    int m_count;
};

#endif
