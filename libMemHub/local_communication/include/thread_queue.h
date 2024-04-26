/**
 * Inter-thread communication queue
 */

#ifndef THREAD_QUEUE_H
#define THREAD_QUEUE_H

#include "msg_info.h"
#include "util.h"

template <class TRequest, class TReply>
class CThreadQueue
{

public:
    CThreadQueue(int worker_id);
    ~CThreadQueue();
    void SendRequest(TRequest data);
    TRequest RecvRequest();
    bool EmptyRequest();
    int SizeRequest();

private:
    int m_worker_id;
    std::deque<TRequest> m_request_collection;
    std::deque<TReply> m_reply_collection;
    std::mutex m_request_mutex;
    std::mutex m_reply_mutex;
    std::condition_variable m_request_conditionV;
    std::condition_variable m_reply_conditionV;
};

#endif
