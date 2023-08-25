#include "Client.h"
#include <stdio.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>
#include "bw_template.h"

char* add_message_data_to_buf(char* buf_pointer, size_t keySize, size_t valueSize)
{
    MessageData messageData;
    memset(&messageData, 0, sizeof(MessageData));
    messageData.operationType = SET;
    messageData.Protocol = EAGER;
    messageData.keySize = keySize;
    messageData.valueSize = valueSize;
    memcpy(buf_pointer, &messageData, sizeof(MessageData));
    return buf_pointer + sizeof(MessageData);
}

int eager_set(KvHandle* kv_handle, const char* key, const char* value, size_t keySize, size_t valueSize)
{
    char* buf_pointer = kv_handle->ctx->buf;

    buf_pointer = add_message_data_to_buf(buf_pointer, keySize, valueSize);

    strcpy(buf_pointer, key);
    buf_pointer += sizeof(key);
    strcpy(buf_pointer, value);

    if (pp_post_send_and_wait(kv_handle, kv_handle->ctx, NULL, 1) == 1)
    {
        perror("Client failed to post send the request");
        return 1;
    }

    return 0;
}

int rendezvous_set(KvHandle* kv_handle, const char* key, char* value, size_t key_len, size_t val_len)
{

}

int rendezvous_get(KvHandle* kv_handle, size_t val_size, char** valuePtr)
{
}

int eager_get(KvHandle* kv_handle, size_t header_size, size_t val_size, char** value)
{

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
        return eager_set(kv_handle, key, value, key_size, value_size);
    }

    return eager_set(kv_handle, key, value, key_size, value_size);
//    return rendezvous_set();

}

int kv_get(void *obj, const char *key, char **value)
{

}

void kv_release(char* value)
{
    free(value);
}

int kv_close(void *My_kv){
    return 1;
}
