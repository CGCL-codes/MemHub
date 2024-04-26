#include "../include/semaphore.h"

Semaphore::Semaphore(int count) : m_count(count) {}

/**
 * Signals (increments) the semaphore count.
 */
void Semaphore::Signal()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    ++m_count;
    m_condition_var.notify_one();
}

/**
 * Waits (decrements) the semaphore count.
 */
void Semaphore::Wait()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_condition_var.wait(lock, [=]
                         { return m_count > 0; });
    --m_count;
}