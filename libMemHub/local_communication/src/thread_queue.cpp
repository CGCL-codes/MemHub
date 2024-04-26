#ifndef THREAD_QUEUE_CPP
#define THREAD_QUEUE_CPP

#include "../include/thread_queue.h"

template <class TRequest, class TReply>
CThreadQueue<TRequest, TReply>::CThreadQueue(int worker_id)
{
    this->m_worker_id = worker_id;
}

template <class TRequest, class TReply>
CThreadQueue<TRequest, TReply>::~CThreadQueue()
{
}

/**
 * Adds a request to the queue.
 */
template <class TRequest, class TReply>
void CThreadQueue<TRequest, TReply>::SendRequest(TRequest data)
{
    std::unique_lock<std::mutex> lock{this->m_request_mutex};
    this->m_request_collection.emplace_back(data);
    lock.unlock();
    this->m_request_conditionV.notify_one();
}

/**
 * Retrieves a request from the queue.
 */
template <class TRequest, class TReply>
TRequest CThreadQueue<TRequest, TReply>::RecvRequest()
{
    std::unique_lock<std::mutex> lock{this->m_request_mutex};
    while (this->m_request_collection.empty())
    {
        this->m_request_conditionV.wait(lock);
    }
    auto result = std::move(this->m_request_collection.front());
    this->m_request_collection.pop_front();
    lock.unlock();
    return result;
}

/**
 * Checks if the request queue is empty.
 */
template <class TRequest, class TReply>
bool CThreadQueue<TRequest, TReply>::EmptyRequest()
{
    std::unique_lock<std::mutex> lock{this->m_request_mutex};
    bool result = this->m_request_collection.empty();
    lock.unlock();
    return result;
}

/**
 * Returns the size of the request queue.
 */
template <class TRequest, class TReply>
int CThreadQueue<TRequest, TReply>::SizeRequest()
{
    std::unique_lock<std::mutex> lock{this->m_request_mutex};
    int result = this->m_request_collection.size();
    lock.unlock();
    return result;
}

#endif
