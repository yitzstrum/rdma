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
        printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
               networkContext->rem_dest->lid, networkContext->rem_dest->qpn, networkContext->rem_dest->psn, networkContext->gid);
    }
    if (flag_success){
        fprintf(stderr, "Couldn't connect clients\n");
        return 1;
    }
    return 0;
}



int check_who_send(struct ibv_wc* wc){
    if(wc->wr_id == I_SEND){
        return 1;
    }
    return 0;
}



void get_id_client(KvHandle *kv_handle,MessageDataGetServer* messageDataGetServer,struct ibv_wc* wc){
    for(int client=0; client<NUM_OF_CLIENTS; client++){
        if (wc->qp_num == kv_handle->clients_ctx[client]->qp->qp_num){
            messageDataGetServer->client_id = client;
            break;
        }
    }
}

void get_wr_id(MessageDataGetServer* messageDataGetServer,struct ibv_wc* wc){
    messageDataGetServer->wr_id = wc->wr_id;
}

char* get_job(KvHandle *kv_handle, MessageDataGetServer* messageDataGetServer, struct ibv_wc* wc){
    printf("-------------get_job_func-------------\n");
    get_id_client(kv_handle,messageDataGetServer, wc);
    get_wr_id(messageDataGetServer, wc);
    return get_wr_details_server(
            kv_handle->clients_ctx[messageDataGetServer->client_id]->resources[messageDataGetServer->wr_id].buf,
            messageDataGetServer);
}

int eager_set_server(KvHandle *kv_handle, MessageDataGetServer* messageDataGetServer, char* data){
    char* key = malloc(messageDataGetServer->keySize);
    char *value = malloc(messageDataGetServer->valueSize);

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

int eager_get_server(KvHandle *kv_handle, MessageDataGetServer* messageDataGetServer, char* data){
    printf("-------------eager_get_server-------------\n");
    char* key = malloc(messageDataGetServer->keySize);
    memcpy(key, data, messageDataGetServer->keySize);
    char* value = NULL;
    hashTable_get(key, &value,kv_handle->hashTable);
    char* bufferPointer = kv_handle->clients_ctx[messageDataGetServer->client_id]->resources[messageDataGetServer->wr_id].buf;
    printf("The size of the value is: %lu\n", sizeof(*value));
    printf("The value is: %s\n", value);
    bufferPointer = add_message_data_to_buf(bufferPointer, 0, sizeof(*value), GET);
    strcpy(bufferPointer, value);
    if (pp_post_send_and_wait(kv_handle, kv_handle->clients_ctx[messageDataGetServer->client_id], NULL, 1))
    {
        perror("Server failed to send the value");
        return 1;
    }
    return 0;
}


//
//int rendezvous_set(){
//    return 0;
//
//}
//int rendezvous_get(){
//    return 0;
//}

int kv_set_server(KvHandle *kv_handle,MessageDataGetServer* messageDataGetServer,char* data){
    switch (messageDataGetServer->Protocol) {
        case EAGER:
            return eager_set_server(kv_handle,messageDataGetServer,data);
//        case RENDEZVOUS:
//            return rendezvous_set();
    }

    return 1;
}


int kv_get_server(KvHandle *kv_handle, MessageDataGetServer* messageDataGetServer, char* data){
    printf("-------------kv_get_server-------------start-------------\n");
    switch (messageDataGetServer->Protocol) {
        case EAGER:
            return eager_get_server(kv_handle, messageDataGetServer, data);
        case RENDEZVOUS:
            break;
    }
    return 1;
}

int process(KvHandle *kv_handle){
    printf("-------------Starting Server process-------------\n");
    struct ibv_wc wc;
    if(pull_cq(kv_handle,&wc,1)){
        perror("Server failed pull cq:");
        return 0;}

    if(check_who_send(&wc)){return 0;}

    MessageDataGetServer messageDataGetServer;
    char* data = get_job(kv_handle, &messageDataGetServer, &wc);
//    printf("protocol: %d \n",messageDataGetServer.Protocol);
//    printf("op: %d \n",messageDataGetServer.operationType);
//    printf("key_size: %zu \n",messageDataGetServer.keySize);
//    printf("data: %s \n",data);
//    printf("data: %s \n",data);
    switch (messageDataGetServer.operationType) {
        case SET:
            kv_set_server(kv_handle, &messageDataGetServer, data);
            break;
        case GET:
            kv_get_server(kv_handle, &messageDataGetServer, data);
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


//void start_server2(void* obj){
//
//    while (1) {
//        if (pull_cq(pHandler, &wc, 1) != 0)
//        {
//            perror("Server failed pull cq:");
//            return;
//        }
//
//
//        if ((client_idx = get_client_identifier(pHandler, wc.qp_num)) == -1)
//        {
//            fprintf(stderr, "failed to find client\n");
//        }
//
//
//        if (wc.opcode == IBV_WC_RDMA_READ)
//        {
//            end_rendezvous_read(pHandler, client_idx, hashTable);
//            continue;
//        }
//
//
//        if (wc.opcode == IBV_WC_RDMA_WRITE)
//        {
//            end_rendezvous_write(pHandler, client_idx);
//            continue;
//        }
//
//
//        int resource_idx = (int)wc.wr_id;
//
//
//        if (resource_idx >= MAX_RESOURCES || resource_idx < 0)
//        {
////            continue;
//            fprintf(stderr, "Invalid wr_id %d (client idx %d)\n", resource_idx, client_idx);
//            parse_header(pHandler->clients_ctx[client_idx]->buf, &protocol, &operation, &key_size, &val_size);
//            char* protocol_name = protocol == RANDEVOUS ? "rendezvous" : "eager";
//            char* operation_name = operation == GET ? "get" : "set";
//            printf("client %d %s %s resource %d key_s %zu, val_s %zu\n", client_idx, protocol_name, operation_name, resource_idx, key_size, val_size);
//            printf("is_server %d\n", ((bool *)pHandler->clients_ctx[client_idx]->buf)[MAX_BUF_SIZE - 1]);
//            break;
//        }
//
//
//
//        void* buf = pHandler->clients_ctx[client_idx]->resources[resource_idx].buf;
//        size_t header_size = parse_header(buf, &protocol, &operation, &key_size, &val_size);
//        char *data = (char *) ((uint8_t *) buf + header_size);
//
//        char* protocol_name = protocol == RANDEVOUS ? "rendezvous" : "eager";
//        char* operation_name = operation == GET ? "get" : "set";
//        printf("client %d %s %s resource %d key_s %zu, val_s %zu\n", client_idx, protocol_name, operation_name, resource_idx, key_size, val_size);
//
//
//
//        switch (operation) {
//            case SET:
//                if (handle_set(pHandler, data, hashTable, client_idx, protocol, key_size, val_size) != 0) {
//                    fprintf(stderr, "Server failed to handle set\n");
//                    error = true;
//                    break;
//                }
//                pp_post_recv(pHandler->clients_ctx[client_idx], resource_idx);
//                break;
//
//            case GET:
//                if (handle_get(pHandler, data, hashTable, client_idx, resource_idx, key_size) != 0) {
//                    fprintf(stderr, "Server failed to handle get\n");
//                    error = true;
//                    break;
//                }
//                break;
//            default:
//                fprintf(stderr, "Server does not support the requested operation\n");
//                error = true;
//                break;
//        }
//
//        if (error) {
//            hashTable_delete_cleanup(hashTable);
//            return;
//        }
//    }
//}