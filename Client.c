#include "Client.h"
#include <stdio.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>
#include "bw_template.h"

int pp_post_send_client(struct pingpong_context *ctx)
{
    struct ibv_sge list = {
            .addr	= (uintptr_t)ctx->resources[ctx->count_send].buf,
            .length = ctx->size,
            .lkey	= ctx->resources[ctx->count_send].mr->lkey
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id	    = ctx->count_send + MAX_RESOURCES,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

int eager_set(KvHandle* kv_handle, const char* key, const char* value, size_t keySize, size_t valueSize)
{
    init_resource(&kv_handle->ctx->resources[kv_handle->ctx->count_send], kv_handle->ctx->pd, MAX_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE);
    char* buf_pointer = kv_handle->ctx->resources[kv_handle->ctx->count_send].buf;
    buf_pointer = add_message_data_to_buf(buf_pointer, keySize, valueSize, SET,EAGER);

    strcpy(buf_pointer, key);
    buf_pointer += sizeof(key);
    strcpy(buf_pointer, value);
    if(pp_post_send_client(kv_handle->ctx)){
        printf("error send\n");
        return 1;
    }
    kv_handle->ctx->count_send += 1 % MAX_RESOURCES;
    return 0;
}


int kv_get_helper(KvHandle* kv_handle, const char* key, const char* value, size_t keySize, size_t valueSize, enum OperationType operation, int iters)
{
    char* buf_pointer = kv_handle->ctx->buf;
    buf_pointer = add_message_data_to_buf(buf_pointer, keySize, valueSize, operation);
    strcpy(buf_pointer, key);

    if (pp_post_send_and_wait(kv_handle, kv_handle->ctx, NULL, iters)){
        perror("Client failed to post send the request");
        return 1;
    }
    return 0;
}

int eager_get(MessageData* messageData, char* data, char** value)
{
    printf("-------------eager_get-------------start-------------\n");
    *value = malloc(messageData->valueSize);
    if (!*value) {return 1;}
//    char* data = (char *)((uint8_t*)response + header_size);
    memcpy(*value , data, messageData->valueSize);
//    printf("Value: %s\n", *value);
    return 0;
}

int rendezvous_set(KvHandle* kv_handle, const char* key, const char* value, size_t keySize, size_t valueSize)
{
    init_resource(&kv_handle->ctx->resources[kv_handle->ctx->count_send],kv_handle->ctx->pd,MAX_BUF_SIZE,IBV_ACCESS_REMOTE_READ);
    char* buf_pointer =kv_handle->ctx->resources[kv_handle->ctx->count_send].buf;
    buf_pointer = add_message_data_to_buf(buf_pointer, keySize, valueSize, SET,RENDEZVOUS);
    strcpy(buf_pointer, key);
    buf_pointer += sizeof(key);

    void* value_data = buf_pointer;
    *(void **)value_data = value;
    memcpy(value_data + sizeof(value), &kv_handle->ctx->resources[kv_handle->ctx->count_send].mr->rkey, sizeof(kv_handle->ctx->resources[kv_handle->ctx->count_send].mr->rkey));
//    strcpy(buf_pointer, value);
    if(pp_post_send_client(kv_handle->ctx)){
        printf("error send\n");
        return 1;
    }
    kv_handle->ctx->count_send += 1 % MAX_RESOURCES;
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

void free_set_malloc(KvHandle* kv_handle){
    struct ibv_wc wc;
    int ne = ibv_poll_cq(kv_handle->ctx->cq, 1, &wc);
    if(ne==0){
        return;
    }

}

int kv_set(void* obj, const char *key, const char *value)
{
    KvHandle* kv_handle = (KvHandle *) obj;
//    free_set_malloc(kv_handle); todo
    // We add 1 because strlen does not cont the null byte \0
    size_t key_size = strlen(key) + 1;
    size_t value_size = strlen(value) + 1;

    if (key_size + value_size < MAX_EAGER_SIZE)
    {
        return eager_set(kv_handle, key, value, key_size, value_size);
    }
    return rendezvous_set(kv_handle, key, value, key_size, value_size);
}


int kv_get(void *obj, const char *key, char **value)
{
    size_t key_size = strlen(key) + 1;
    KvHandle* kv_handle = (KvHandle *) obj;

    kv_get_helper(kv_handle, key, "", key_size, 0, GET, 2);
    MessageData messageData;
    char* data = get_wr_details_client(kv_handle, &messageData);
//    printf("Message Protocol -> %u\n", messageData.Protocol);
//    printf("The client received the following value: %s\n", data);
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
