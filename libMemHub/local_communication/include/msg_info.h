#ifndef MSG_INFO_H
#define MSG_INFO_H

#define END_FLAG -1

// IPC
#define MSG_PATH "../msg"

#define MSG_TRAIN_QUEUE_JOB_ID 0
#define MSG_TRAIN_SHM_JOB_ID 64
#define MSG_VAL_QUEUE_JOB_ID 128
#define MSG_VAL_SHM_JOB_ID 192

#define MSG_CLIENT_TYPE 12800
#define MSG_SERVER_TYPE 12801

/**
 * Indicates blocking mode for IPC operations.
 *
 * Setting IPC_WAIT to 0 signifies that the IPC operation should block
 * until it can proceed, typically waiting for a resource to become available.
 */
#define IPC_WAIT 0

#define CLIENT_MSG_SIZE sizeof(ClientMsg) - sizeof(long)
#define Server_MSG_SIZE sizeof(ServerMsg) - sizeof(long)
#define MAX_FILE_SIZE 16777216 // 16MB

typedef struct
{
    long msg_type;
    int label_index;
    unsigned long file_size;
    int cache_file_idx;
} ServerMsg;

typedef struct
{
    long msg_type;
    int request_idx;
} ClientMsg;

#endif
