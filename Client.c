#include "Client.h"
#include <stdio.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include "bw_template.h"



#include "Client.h"
#include <stdio.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <stdlib.h>
#include "bw_template.h"

int kv_open(char *servername, void** obj)
{
    My_kv* pHandle = *((My_kv **) obj);

    pHandle->rem_dest = pp_client_exch_dest(servername, &pHandle->my_dest);
    if (!pHandle->rem_dest) {
        return 1;
    }

    inet_ntop(AF_INET6, &pHandle->rem_dest->gid, pHandle->gid, sizeof pHandle->gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           pHandle->rem_dest->lid, pHandle->rem_dest->qpn, pHandle->rem_dest->psn, pHandle-> gid);

    if (pp_connect_ctx(pHandle->ctx->qp, pHandle->my_dest.psn, pHandle->rem_dest, pHandle->gidx)) {
        return 1;
    }

    return 0;
}

int eager_set(My_kv* my_kv, const char* key, const char* value, size_t key_len, size_t val_len)
{
    printf("eager set start\n");
//    todo mybe change
    size_t header_size = create_header(my_kv->ctx->buf, EAGER, SET, key_len, val_len);
    char* data_start = (char*)((uint8_t*)my_kv->ctx->buf + header_size);

    strcpy(data_start, key);
    strcpy(data_start + key_len, value);

    if (pp_post_send_and_wait(my_kv, my_kv->ctx, NULL, 1, false) != 0) {
        perror("Client couldn't post set request");
        fprintf(stderr, "\n");
        return 1;
    }
    printf("eager set end\n");
    return 0;
}
int rendezvous_set(My_kv* pHandler, const char* key, char* value, size_t key_len, size_t val_len)
{
//    printf("randevous_set start\n");
    size_t header_size = create_header(pHandler->ctx->buf, RANDEVOUS, SET, key_len, val_len);
    void* header_data = (uint8_t*)pHandler->ctx->buf + header_size;

    strcpy(header_data, key);

    struct ibv_mr* mr = init_mr(pHandler->ctx->pd, value, val_len, IBV_ACCESS_REMOTE_READ);
    if (!mr) {
        fprintf(stderr, "Failed to register value memory region for remote read\n");
        return 1;
    }

    void* value_data = header_data + key_len;
    uint32_t rkey = mr->rkey;
    *(void **)value_data = value;
    memcpy(value_data + sizeof(value), &rkey, sizeof(rkey));

    struct ibv_wc wc;
    if (pp_post_send_and_wait(pHandler, pHandler->ctx, &wc, 2, false) != 0) {
        perror("Client randevous_set");
        fprintf(stderr, "Client couldn't post send\n");
        return 1;
    }

//    printf("randevous_set end: %s\n", (char *)wc.wr_id);

    ibv_dereg_mr(mr);
    return 0;
}

int rendezvous_get(My_kv* pHandler, size_t val_size, char** valuePtr)
{
//    printf("randevous_get start\n");
    *valuePtr = malloc(val_size);
    char* value = *valuePtr;
    if (value == NULL) {
        fprintf(stderr, "Client allocate value failed\n");
        return 1;
    }

    struct ibv_mr* mr = ibv_reg_mr(pHandler->ctx->pd, value, val_size, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);
    if (mr == NULL) {
        perror("Failed to register value memory region for remote write");
        return 1;
    }

    *(void**)pHandler->ctx->buf = value;
    memcpy(pHandler->ctx->buf + sizeof(value), &mr->rkey, sizeof(mr->rkey));

    struct ibv_wc wc;
    if (pp_post_send_and_wait(pHandler, pHandler->ctx, &wc, 2, false) != 0) {
        perror("Client randevous_set");
        fprintf(stderr, "Client couldn't post send\n");
        return 1;
    }

//    printf("randevous_get end: %s\n", (char *)wc.wr_id);
//    printf("client rendezvous value: %s\n", value);

    ibv_dereg_mr(mr);
    return 0;
}


int kv_set(void* obj, const char *key, const char *value)
{
    My_kv* my_kv = (My_kv *)obj;
    size_t key_len = strlen(key) + 1;
    size_t val_len = strlen(value) + 1;
//    if (key_len + val_len < MAX_EAGER_MSG_SIZE)
//    {
    return eager_set(my_kv, key, value, key_len, val_len);
//    }

//    return rendezvous_set(my_kv, key, value, key_len, val_len);
}

int eager_get(My_kv* my_kv,size_t header_size,size_t val_size,char** value){
    *value = malloc(val_size);
    char* data = (char*)my_kv->ctx->buf + header_size;
    memcpy(*value , data, val_size);
    printf("Eager get end\n");
    return 0;
}

int kv_get(void *obj, const char *key, char **value)
{
//    printf("Get start\n");
    My_kv* my_kv = (My_kv *)obj;
    size_t key_size = strlen(key) + 1;

    size_t header_size = create_header(my_kv->ctx->buf, EAGER, GET, key_size, 0);
    char* data = (char*)((uint8_t*)my_kv->ctx->buf + header_size);
    strcpy(data, key);

    struct ibv_wc wc;
    if (pp_post_send_and_wait(my_kv, my_kv->ctx, &wc, 2, false) != 0) {
        fprintf(stderr, "Client couldn't request get\n");
        return 1;
    }


    size_t val_size;
    enum Protocol protocol;
    enum Operation_t operation;
    void* response = my_kv->ctx->buf;

    parse_header(response, &protocol, &operation, &key_size, &val_size);

    switch (protocol) {
        case RANDEVOUS:
            return rendezvous_get(my_kv, val_size, value);
        case EAGER:
            return eager_get(my_kv,header_size,val_size,value);
//            *value = malloc(val_size);
//            data = (char*)my_kv->ctx->buf + header_size;
//            memcpy(*value , data, val_size);
//            printf("Eager get end\n");
//            return 0;
        default:
            return 1;
    }
}

void kv_release(char* value)
{
    free(value);
}

int kv_close(void *My_kv){
    return 1;
}
