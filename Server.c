//
// Created by danieltal on 8/16/23.
//

#include <infiniband/verbs.h>
#include <stdio.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include "bw_template.h"
#include "Server.h"
#include "db.h"

int init_clients_ctx(KvHandle* networkContext, int i){
    networkContext->clients_ctx[i] = pp_init_ctx(networkContext, networkContext->ctx->cq);
    if (networkContext->clients_ctx[i] == NULL)
    {
        fprintf(stderr, "Server failed to connect clients, ctx is null\n");
        return 1;
    }
    return 0;
}

int init_clients_routs(KvHandle* networkContext, int i){
    networkContext->clients_ctx[i]->routs = pp_post_recv_server(networkContext->clients_ctx[i], networkContext->clients_ctx[i]->rx_depth);
    if (networkContext->clients_ctx[i]->routs < networkContext->clients_ctx[i]->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", networkContext->clients_ctx[i]->routs);
        ibv_destroy_qp(networkContext->clients_ctx[i]->qp);
        return 1;
    }
    return 0;
}

int init_clients_dest(KvHandle* networkContext, int i){
//    todo
    networkContext->my_dest.qpn = networkContext->clients_ctx[i]->qp->qp_num;
    networkContext->rem_dest = pp_server_exch_dest(networkContext->clients_ctx[i]->qp, &networkContext->my_dest, networkContext->gidx);
    if (!networkContext->rem_dest) {
        return 1;
    }
    return 0;
}

int connect_to_clients(KvHandle* networkContext)
{
    int flag_success=0;
    for (int i = 0; i < NUM_OF_CLIENTS; ++i) {
        if(init_clients_ctx(networkContext, i)){flag_success=1;break;}
        if(init_clients_routs(networkContext, i)){flag_success= 1;break;}
        if(init_clients_dest(networkContext, i)){flag_success= 1;break;}
        inet_ntop(AF_INET6, &networkContext->rem_dest->gid, networkContext->gid, sizeof(networkContext->gid));
    }
    if (flag_success){
        fprintf(stderr, "Couldn't connect clients\n");
        return 1;
    }
    return 0;
}

int check_who_send(struct ibv_wc* wc){
    if(wc->wr_id == I_SEND){
        printf("------------ check_who_send ------------\n");
        return 1;
    }
    return 0;
}

int get_client_id(KvHandle *kv_handle, struct ibv_wc* wc){
    for(int client=0; client<NUM_OF_CLIENTS; client++){
        if (wc->qp_num == kv_handle->clients_ctx[client]->qp->qp_num){
            return client;
        }
    }
}

void free_and_reset_ptr(void* resource)
{
    free(resource);
    resource = NULL;
}

void free_and_reset_mr(struct ibv_mr* mr)
{
    ibv_dereg_mr(mr);
    mr = NULL;
}

char* get_job(KvHandle *kv_handle, MessageData* messageData, struct ibv_wc* wc){
    printf("-------------get_job_func-------------\n");
    int wr_id = wc->wr_id;
    int client_id = get_client_id(kv_handle, wc);

    void* buffer = kv_handle->clients_ctx[client_id]->resources[wr_id].buf;
    memcpy(messageData, buffer, sizeof(MessageData));

    messageData->client_id = client_id;
    messageData->wr_id = wr_id;

    printf("Protocol: %u\n", messageData->Protocol);
    printf("operationType: %u\n", messageData->operationType);
    printf("keySize: %zu\n", messageData->keySize);
    printf("valueSize: %zu\n", messageData->valueSize);

    printf("-------------get_job-------------end-------------\n");

    return buffer + sizeof(MessageData);
}

int eager_set_server(KvHandle *kv_handle, MessageData* messageData, char* data){
    char* key = malloc(messageData->keySize);
    char *value = malloc(messageData->valueSize);

    strcpy(key, data);
    data += sizeof(key);
    strcpy(value, data);

    // TODO: Check that set works.
    printf("key: %s, value: %s\n", key, value);
    if (hashTable_set(key, value, kv_handle->hashTable))
    {
        fprintf(stderr, "hashtable set failed\n");
        return 1;
    }
    return 0;
}

int eager_get_server(KvHandle *kv_handle, MessageData* messageData, char* data){
    printf("-------------eager_get_server-------------\n");
    char* key = malloc(messageData->keySize);
    memcpy(key, data, messageData->keySize);
    printf("The key is: %s\n", key);
    char* value = NULL;
    hashTable_get(key, &value,kv_handle->hashTable);
    char* bufferPointer = kv_handle->clients_ctx[messageData->client_id]->buf;
    printf("The size of the value is: %lu\n", strlen(value));
    printf("The value is: %s\n", value);
    bufferPointer = copy_message_data_to_buf(bufferPointer, 0, strlen(value) + 1, GET, EAGER, NULL, 0);
    strcpy(bufferPointer, value);
    if (pp_post_send_and_wait(kv_handle, kv_handle->clients_ctx[messageData->client_id], NULL, 1))
    {
        perror("Server failed to send the value");
        return 1;
    }
    return 0;
}

int rendezvous_set_server(KvHandle *kv_handle, MessageData* messageData, char* data){
    printf("-------------rendezvous_set_server-------------\n");
    int client_id = messageData->client_id;
    int wr_id = messageData->wr_id;

    // ---------------------------------------------- Memory Allocation ----------------------------------------------

    kv_handle->clients_ctx[client_id]->resources[wr_id].value_buffer = malloc(messageData->valueSize);
    if (kv_handle->clients_ctx[client_id]->resources[wr_id].value_buffer == NULL)
    {
        perror("Failed to allocate memory for value");
        return 1;
    }
    kv_handle->clients_ctx[client_id]->resources[wr_id].key_buffer = malloc(messageData->keySize);
    if (kv_handle->clients_ctx[client_id]->resources[wr_id].key_buffer == NULL)
    {
        perror("Failed to allocate memory for key");
        free(kv_handle->clients_ctx[client_id]->resources[wr_id].value_buffer);
        return 1;
    }

    kv_handle->clients_ctx[client_id]->resources[wr_id].value_mr = init_mr(
            kv_handle->clients_ctx[client_id]->pd,
            kv_handle->clients_ctx[client_id]->resources[wr_id].value_buffer,
            messageData->valueSize,
            IBV_ACCESS_LOCAL_WRITE);

    if (kv_handle->clients_ctx[client_id]->resources[wr_id].value_mr == NULL)
    {
        perror("Failed to init memory region");
        free(kv_handle->clients_ctx[client_id]->resources[wr_id].key_buffer);
        free(kv_handle->clients_ctx[client_id]->resources[wr_id].value_buffer);
        return 1;
    }

    // ---------------------------------------------------------------------------------------------------------------

    memcpy(kv_handle->clients_ctx[messageData->client_id]->resources[messageData->wr_id].key_buffer, data, messageData->keySize);
    printf("-------------pp_post_rdma-------------\n");
    if(pp_post_rdma(kv_handle->clients_ctx[messageData->client_id], messageData, IBV_WR_RDMA_READ))
    {
        perror("Failed to send RDMA read");
        free(kv_handle->clients_ctx[client_id]->resources[wr_id].key_buffer);
        free(kv_handle->clients_ctx[client_id]->resources[wr_id].value_buffer);
        ibv_dereg_mr(kv_handle->clients_ctx[client_id]->resources[wr_id].value_mr);
        return 1;
    }


    return 0;
}

//int rendezvous_get(){
//    return 0;
//}

int kv_set_server(KvHandle *kv_handle, MessageData* messageData, char* data){
    switch (messageData->Protocol) {
        case EAGER:
            return eager_set_server(kv_handle, messageData, data);
        case RENDEZVOUS:
            return rendezvous_set_server(kv_handle, messageData, data);
    }

    return 1;
}


int kv_get_server(KvHandle *kv_handle, MessageData* messageData, char* data){
    printf("-------------kv_get_server-------------start-------------\n");
    switch (messageData->Protocol) {
        case EAGER:
            return eager_get_server(kv_handle, messageData, data);
        case RENDEZVOUS:
            break;
    }
    return 1;
}

int rdma_read_returned(KvHandle* kv_handle, int wr_id, int client_id)
{
    // TODO: Which buffer do we want to send here?
    strcpy(kv_handle->clients_ctx[client_id]->buf, "FIN");

    if (pp_post_send(kv_handle->clients_ctx[client_id]))
    {
        fprintf(stderr, "Failed to send FIN");
        return 1;
    }

    Resource resource = kv_handle->clients_ctx[client_id]->resources[wr_id];
    printf("-------------hash_table_set--------------------------\n");
    if (hashTable_set(resource.key_buffer, resource.value_buffer, kv_handle->hashTable))
    {
        fprintf(stderr, "Failed to set the (key, value) pair");
        return 1;
    }

    free_and_reset_ptr(resource.key_buffer);
    free_and_reset_ptr(resource.value_buffer);
    free_and_reset_ptr(resource.buf);
    free_and_reset_mr(resource.value_mr);
    free_and_reset_mr(resource.mr);

    return 0;
}


int process(KvHandle *kv_handle){
    printf("-------------Starting Server process-------------\n");
    struct ibv_wc wc;
    if(pull_cq(kv_handle,&wc,1)){
        perror("Server failed pull cq:");
        return 0;
    }

    if(check_who_send(&wc)){return 0;}

    int client_id = get_client_id(kv_handle, &wc);
    printf("opcode: %d\n", wc.opcode);
    if (wc.opcode == IBV_WC_RDMA_READ)
    {
        printf("-------------rdma_read_returned-------------\n");
        return rdma_read_returned(kv_handle, wc.wr_id, client_id);
    }

    MessageData* messageData = malloc(sizeof(MessageData));
    char* data = get_job(kv_handle, messageData, &wc);



    printf("Protocol: %u\n", messageData->Protocol);
    printf("operationType: %u\n", messageData->operationType);
    printf("keySize: %zu\n", messageData->keySize);
    printf("valueSize: %zu\n", messageData->valueSize);

    switch (messageData->operationType) {
        case SET:
            kv_set_server(kv_handle, messageData, data);
            break;
        case GET:
            kv_get_server(kv_handle, messageData, data);
            break;
        default:
            return 1;
    }

    return 0;
}

void start_server(void* kv){
    KvHandle *kv_handle = ((KvHandle *) kv);
    kv_handle->hashTable = initializeHashTable();
    printf("connect to client\n");
    if (connect_to_clients(kv_handle)) {return;}
    while(process(kv_handle)==0){};
}