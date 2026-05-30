#define _GNU_SOURCE

#include "server.h"
#include "protocol.h"
#include "MThelpers.h"
#include <pthread.h>
#include <signal.h>

void sigusr1_handler(int signal) {
    sigusr1_flag = 1;
}

void sigint_handler(int signal) {
    sigint_flag = 1;
}

// === INITIALIZATION ===

int Open(const char* filename, int flag, mode_t mode) {
    int fd = open(filename, flag, mode);
    if (fd == -1)
        exit(2);
    return fd;
}

void Sigaction(int signal, const struct sigaction *act, void* null) {
    if (sigaction(signal, act, null) == -1)
        printf("signal handler failed to install\n");
}

void clientid_deleter(void* data) {
    free(data);
}

void init_locks() {
    pthread_mutex_init(&maxDonations_lock,NULL);
    pthread_mutex_init(&donation_log_lock,NULL);
    for (int i = 0; i < 5; i++) {
        pthread_mutex_init(&charity_locks[i], NULL);
    }
}

void init_server_stats() {
    clientCnt = 0;
    memset(maxDonations, 0, 3 * sizeof(uint64_t));
    memset(charities, 0, 5 * sizeof(charity_t));
}

// === Client Thread ===

void update_maxDonations(uint64_t total_donations) {
    pthread_mutex_lock(&maxDonations_lock);
    uint64_t temp;
    if (maxDonations[0] < total_donations) {
        temp = maxDonations[0];
        maxDonations[0] = total_donations;
        total_donations = temp;
    }
    if (maxDonations[1] < total_donations) {
        temp = maxDonations[1];
        maxDonations[1] = total_donations;
        total_donations = temp;
    }
    if (maxDonations[2] < total_donations) {
        temp = maxDonations[2];
        maxDonations[2] = total_donations;
        total_donations = temp;
    }
    pthread_mutex_unlock(&maxDonations_lock);
}

int client_donate(message_t* msg, int client_fd) {
    uint8_t c = (msg->msgdata).donation.charity;
    if (c > 4) {
        client_error(msg,client_fd);
        return -1;
    }
    uint64_t amt = (msg->msgdata).donation.amount;
    pthread_mutex_lock(&charity_locks[c]);
    charities[c].totalDonationAmt += amt;
    if (amt > charities[c].topDonation)
        charities[c].topDonation = amt;
    charities[c].numDonations++;
    pthread_mutex_unlock(&charity_locks[c]);
    
    write(client_fd, msg, sizeof(message_t));
    
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d DONATE %u %" PRIu64 "\n", client_fd, c, amt);
    pthread_mutex_unlock(&donation_log_lock);
    return 0;
}

void client_info(message_t* msg, int client_fd){
    uint8_t c = (msg->msgdata).donation.charity;
    if (c > 4) {
        client_error(msg,client_fd);
        return;
    }
    pthread_mutex_lock(&charity_locks[c]);
    (msg->msgdata).charityInfo = charities[c];
    pthread_mutex_unlock(&charity_locks[c]);
    
    write(client_fd, msg, sizeof(message_t));
    
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d CINFO %u\n", client_fd, c);
    pthread_mutex_unlock(&donation_log_lock);
}

void client_top(message_t*msg, int client_fd) {
    pthread_mutex_lock(&maxDonations_lock);
    (msg->msgdata).maxDonations[0] = maxDonations[0];
    (msg->msgdata).maxDonations[1] = maxDonations[1];
    (msg->msgdata).maxDonations[2] = maxDonations[2];
    pthread_mutex_unlock(&maxDonations_lock);

    write(client_fd, msg, sizeof(message_t));

    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d TOP\n", client_fd);
    pthread_mutex_unlock(&donation_log_lock);
}

void client_logout(int client_fd, uint64_t total_donations){
    close(client_fd);
    
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d LOGOUT\n", client_fd);
    pthread_mutex_unlock(&donation_log_lock);
    
    update_maxDonations(total_donations);
}

void client_error(message_t*msg, int client_fd){
    
    msg->msgtype = 0xFF;
    write(client_fd, msg, sizeof(message_t));
        
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d ERROR\n", client_fd);
    pthread_mutex_unlock(&donation_log_lock);
}

// === Client Thread ID Linked List ===
void add_client_thread(list_t* thread_id_list, pthread_t tid){
    InsertAtHead(thread_id_list, tid);
}


void reap_current_client_threads(list_t* thread_id_list) {
    node_t* curr = thread_id_list->head;
    while(curr != NULL) {
        node_t* next = curr->next;
        pthread_t* curr_tid = curr->data;
        int err = pthread_tryjoin_np(*curr_tid, NULL);
        if (err == 0) {
            RemoveFromList(thread_id_list, curr);
        } 
        curr = next;
    }
}

// === Closing Server ===

void destroy_locks() {
    pthread_mutex_destroy(&maxDonations_lock);
    pthread_mutex_destroy(&donation_log_lock);
    for (int i = 0; i < 5; i++) {
        pthread_mutex_destroy(&charity_locks[i]);
    }
}
void close_server(list_t* thread_id_list, int clientCnt, uint64_t* maxDonations, charity_t* charities){
    kill_client_threads(thread_id_list);
    reap_client_threads(thread_id_list);
    print_data(clientCnt, maxDonations, charities);
}

void kill_client_threads(list_t* thread_id_list){
    node_t* curr = thread_id_list->head;
    while(curr != NULL) {
        pthread_t* curr_tid = curr->data; 
        pthread_kill(*curr_tid, SIGUSR1);
        curr = curr->next;
    }
}

void reap_client_threads(list_t* thread_id_list) {
    node_t* curr = thread_id_list->head;
    while(curr != NULL) {
        pthread_t* curr_tid = curr->data;
        pthread_join(*curr_tid, NULL);
        curr = curr->next;
    }
}

void print_data(int clientCnt, uint64_t* maxDonations, charity_t* charities) {
    int i;
    for (i = 0; i < 5; i++) {
        charity_t c = charities[i];
        fprintf(stdout, "%d, %" PRIu32 ", %" PRIu64 ", %" PRIu64 "\n", i, c.numDonations, c.topDonation, c.totalDonationAmt);
    }
    fprintf(stderr,"%d\n%" PRIu64 ", %" PRIu64 ", %" PRIu64 "\n", clientCnt, maxDonations[0], maxDonations[1], maxDonations[2]);
}