
// Interface of communicator_backend.

#ifndef ICOMMUNICATOR_BACKEND_H
#define ICOMMUNICATOR_BACKEND_H

class CICommunicatorBackend
{
public:
    virtual void set_node_status(int node_id, int status, int synch_id) = 0;
    virtual int get_node_status(int node_id, int synch_id) = 0;
};

#endif