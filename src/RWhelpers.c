#define _GNU_SOURCE

#include "server.h"
#include "protocol.h"
#include "RWhelpers.h"
#include <pthread.h>
#include <signal.h>


void sigusr1_handler(int signal) {
    sigusr1_flag = 1;
}

void sigint_handler(int signal) {
    sigint_flag = 1;
}

void P(sem_t *s) {
    sem_wait(s);
}

void V(sem_t *s) {
    sem_post(s);
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

void init_server_stats() {
    clientCnt = 0;
    memset(maxDonations, 0, 3 * sizeof(uint64_t));
    memset(charities, 0, 5 * sizeof(charity_t));
}

void init_locks() {
    sem_init(&charities_lock, 0, 1);
    sem_init(&charities_w, 0, 1);
    sem_init(&maxDonations_lock, 0, 1);
    sem_init(&maxDonations_w, 0, 1);
    pthread_mutex_init(&donation_log_lock,NULL);
    pthread_mutex_init(&clientCnt_lock, NULL);
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

void threadid_deleter(void* data) {
    free(data);
}

// === Thread Work ===

void writer_logout(int client_fd, uint64_t total_donations){
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
// === Writer Thread Work ===
int check_update_maxDonations(uint64_t total_donations) {
    int update_check = 0;
    
    P(&maxDonations_lock);
    maxDonations_readcnt++;
    if (maxDonations_readcnt == 1) P(&maxDonations_w);
    V(&maxDonations_lock);

    if (total_donations > maxDonations[2]) update_check = 1;
    
    P(&maxDonations_lock);
    maxDonations_readcnt--;
    if (maxDonations_readcnt == 0) V(&maxDonations_w);
    V(&maxDonations_lock);
    
    return update_check;

}

void update_maxDonations(uint64_t total_donations) {
    P(&maxDonations_w);
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
    V(&maxDonations_w);
}

int writer_donate(message_t* msg, int client_fd) {
    uint8_t c = (msg->msgdata).donation.charity;
    if (c > 4) {
        client_error(msg,client_fd);
        return -1;
    }
    uint64_t amt = (msg->msgdata).donation.amount;
    P(&charities_w);
    charities[c].totalDonationAmt += amt;
    if (amt > charities[c].topDonation)
        charities[c].topDonation = amt;
    charities[c].numDonations++;
    V(&charities_w);
    
    write(client_fd, msg, sizeof(message_t));
    
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d DONATE %u %" PRIu64 "\n", client_fd, c, amt);
    pthread_mutex_unlock(&donation_log_lock);
    return 0;
}

// === Reader Thread Work ===

void reader_info(message_t* msg, int client_fd){
    uint8_t c = (msg->msgdata).donation.charity;
    if (c > 4) {
        client_error(msg,client_fd);
        return;
    }
    P(&charities_lock);
    charities_readcnt++;
    if (charities_readcnt == 1) P(&charities_w);
    V(&charities_lock);
    
    (msg->msgdata).charityInfo = charities[c];
    
    P(&charities_lock);
    charities_readcnt--;
    if (charities_readcnt == 0) V(&charities_w);
    V(&charities_lock);
    
    write(client_fd, msg, sizeof(message_t));
    
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d CINFO %u\n", client_fd, c);
    pthread_mutex_unlock(&donation_log_lock);
}

void reader_top(message_t*msg, int client_fd) {
    P(&maxDonations_lock);
    maxDonations_readcnt++;
    if (maxDonations_readcnt == 1) P(&maxDonations_w);
    V(&maxDonations_lock);
    
    (msg->msgdata).maxDonations[0] = maxDonations[0];
    (msg->msgdata).maxDonations[1] = maxDonations[1];
    (msg->msgdata).maxDonations[2] = maxDonations[2];
    
    P(&maxDonations_lock);
    maxDonations_readcnt--;
    if (maxDonations_readcnt == 0) V(&maxDonations_w);
    V(&maxDonations_lock);

    write(client_fd, msg, sizeof(message_t));

    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d TOP\n", client_fd);
    pthread_mutex_unlock(&donation_log_lock);
}

void reader_stats(message_t*msg, int client_fd) {
    uint8_t charity_high = 0;
    uint64_t charity_high_amt;

    uint8_t charity_low = 0;
    uint64_t charity_low_amt;

    int i;
    P(&charities_lock);
    charities_readcnt++;
    if (charities_readcnt == 1) P(&charities_w);
    V(&charities_lock);

    charity_high_amt = charities[0].totalDonationAmt;
    charity_low_amt = charities[0].totalDonationAmt;
    
    for(i = 1; i < 5; i++) {
        if (charities[i].totalDonationAmt > charity_high_amt) {
            charity_high = i;
            charity_high_amt = charities[i].totalDonationAmt;
        }
        if (charities[i].totalDonationAmt < charity_low_amt) {
            charity_low = i;
            charity_low_amt = charities[i].totalDonationAmt;
        }
    }
    
    P(&charities_lock);
    charities_readcnt--;
    if (charities_readcnt == 0) V(&charities_w);
    V(&charities_lock);

    (msg->msgdata).stats.charityID_high = charity_high;
    (msg->msgdata).stats.charityID_low = charity_low;
    (msg->msgdata).stats.amount_high = charity_high_amt;
    (msg->msgdata).stats.amount_low = charity_low_amt;
    
    write(client_fd, msg, sizeof(message_t));

    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d STATS %u:%" PRIu64 " %u:%" PRIu64 "\n", client_fd, charity_high, charity_high_amt, charity_low, charity_low_amt);
    pthread_mutex_unlock(&donation_log_lock);
}

void reader_logout(int client_fd){
    close(client_fd);
    
    pthread_mutex_lock(&donation_log_lock);
    dprintf(log_fd, "%d LOGOUT\n", client_fd);
    pthread_mutex_unlock(&donation_log_lock);
}

// === Closing Server === 
void destroy_locks() {
    sem_destroy(&charities_lock);
    sem_destroy(&charities_w);
    sem_destroy(&maxDonations_lock);
    sem_destroy(&maxDonations_w);
    pthread_mutex_destroy(&donation_log_lock);
    pthread_mutex_destroy(&clientCnt_lock);
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