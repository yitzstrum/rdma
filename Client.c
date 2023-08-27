#include "Client.h"
#include <stdio.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>
#include "bw_template.h"

char* get_wr_details2(KvHandle *kv_handle,MessageData * messageData){
    char* buffer=kv_handle->ctx->buf;

    memcpy(&messageData->Protocol, buffer, sizeof(messageData->Protocol));
    buffer+=sizeof(messageData->Protocol);

    memcpy(&messageData->operationType, buffer, sizeof(messageData->operationType));
    buffer+=sizeof(messageData->operationType);

    memcpy(&messageData->keySize, buffer, sizeof(messageData->keySize));
    buffer+=sizeof(messageData->keySize);

    memcpy(&messageData->valueSize, buffer, sizeof(messageData->valueSize));
    return buffer+=sizeof(messageData->valueSize);
}

char* add_message_data_to_buf(char* buf_pointer, size_t keySize, size_t valueSize,enum OperationType operation)
{
    MessageData messageData;
    memset(&messageData, 0, sizeof(MessageData));
    messageData.operationType = operation;
    messageData.Protocol = EAGER;
    messageData.keySize = keySize;
    messageData.valueSize = valueSize;
    memcpy(buf_pointer, &messageData, sizeof(MessageData));
    return buf_pointer + sizeof(MessageData);
}

int eager_send(KvHandle* kv_handle, const char* key, const char* value, size_t keySize, size_t valueSize,enum OperationType operation,int iters)
{
    char* buf_pointer = kv_handle->ctx->buf;

    buf_pointer = add_message_data_to_buf(buf_pointer, keySize, valueSize,operation);

    strcpy(buf_pointer, key);
//    printf("key: %s\n",buf_pointer);
    buf_pointer += sizeof(key);
//    printf("val: %s\n",buf_pointer);
    strcpy(buf_pointer, value);
    printf("22\n");
    if (pp_post_send_and_wait(kv_handle, kv_handle->ctx, NULL, iters))
    {
        perror("Client failed to post send the request");
        return 1;
    }
    return 0;
}

int eager_get(MessageData* MD,char* data,char** value)
{
    *value = malloc(MD->valueSize);
    if (!*value)
    {return 1;}

//    char* data = (char *)((uint8_t*)response + header_size);
    memcpy(*value , data, MD->valueSize);
    return 0;
}

int rendezvous_set(KvHandle* kv_handle, const char* key, char* value, size_t key_len, size_t val_len)
{
    return 0;
}

int rendezvous_get(KvHandle* kv_handle, size_t val_size, char** valuePtr)
{
    return 0;
}



int kv_open(char *servername, void** obj)
{
    KvHandle* networkContext = *((KvHandle **) obj);

    networkContext->rem_dest = pp_client_exch_dest(servername, &networkContext->my_dest);
    if (!networkContext->rem_dest) {
        return 1;
    }

    inet_ntop(AF_INET6, &networkContext->rem_dest->gid, networkContext->gid, sizeof networkContext->gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           networkContext->rem_dest->lid, networkContext->rem_dest->qpn, networkContext->rem_dest->psn, networkContext-> gid);

    if (pp_connect_ctx(networkContext->ctx->qp, networkContext->my_dest.psn, networkContext->rem_dest, networkContext->gidx)) {
        return 1;
    }

    return 0;
}

int kv_set(void* obj, const char *key, const char *value)
{
    KvHandle* kv_handle = (KvHandle *) obj;

    // We add 1 because strlen does not cont the null byte \0
    size_t key_size = strlen(key) + 1;
    size_t value_size = strlen(value) + 1;

    if (key_size + value_size < MAX_EAGER_SIZE)
    {
        return eager_send(kv_handle, key, value, key_size, value_size,SET,1);
    }
    printf("11\n");
    return eager_send(kv_handle, key, value, key_size, value_size,SET,1);
//    return rendezvous_set();

}

int kv_get(void *obj, const char *key, char **value)
{
    size_t key_size = strlen(key) + 1;
    KvHandle* kv_handle = (KvHandle *) obj;
    eager_send(kv_handle,key,"",key_size,0,GET,2);
//    char* buf_pointer = kv_handle->ctx->buf;
    MessageData messageData;
    char* data = get_wr_details2(kv_handle,&messageData);


    switch (messageData.Protocol) {
        case EAGER:
            return eager_get(&messageData,data,value);
//        case RENDEZVOUS:
//            return rendezvous_get();
        default:
            return 1;
    }
    return 0;
}

void kv_release(char* value)
{
    free(value);
}

int kv_close(void *KvHandle){
    return 1;
}
