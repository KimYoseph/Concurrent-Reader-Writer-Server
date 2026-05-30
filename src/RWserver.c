#include "server.h"
#include "protocol.h"
#include "RWhelpers.h"
#include "linkedlist.h"
#include <pthread.h>
#include <signal.h>
#include <errno.h>

/**********************DECLARE ALL LOCKS HERE BETWEEN THES LINES FOR MANUAL GRADING*************/

sem_t charities_lock; // lock for the readcnt of maxDonations_readcnt;
sem_t charities_w; // lock for writing into charities

sem_t maxDonations_lock; // lock for the readcnt of maxDonations_readcnt;
sem_t maxDonations_w; // lock for writing  into maxDonations

pthread_mutex_t donation_log_lock; // lock for the donation log
pthread_mutex_t clientCnt_lock; // lock for clientCnt

/***********************************************************************************************/

// Global variables, statistics collected since server start-up
int clientCnt;  // # of client connections made, Updated by the main thread
uint64_t maxDonations[3];  // 3 highest total donations amounts (sum of all donations to all  
                           // charities in one connection), updated by client threads
                           // index 0 is the highest total donation
charity_t charities[5]; // Global variable, one charity per index

int charities_readcnt = 0;
int maxDonations_readcnt = 0;

volatile sig_atomic_t sigint_flag = 0;
volatile sig_atomic_t sigusr1_flag = 0;
int log_fd;

void* w_thread(void* writer_listener_fd);

void* r_thread(void* readerfd_ptr);

int main(int argc, char *argv[]) {

    // Arg parsing
    int opt;
    while ((opt = getopt(argc, argv, "h")) != -1) {
        switch (opt) {
            case 'h':
                fprintf(stderr, USAGE_MSG_RW);
                exit(EXIT_FAILURE);
        }
    }

    // 3 positional arguments necessary
    if (argc != 4) {
        fprintf(stderr, USAGE_MSG_RW);
        exit(EXIT_FAILURE);
    }
    unsigned int r_port_number = atoi(argv[1]);
    unsigned int w_port_number = atoi(argv[2]);
    char *log_filename = argv[3];


    // INSERT SERVER INITIALIZATION CODE HERE
    log_fd = Open(log_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644); // open file log

    list_t* thread_id_list = CreateList(threadid_deleter);
    struct sigaction myaction = {{0}}; 
    myaction.sa_handler = sigint_handler; 
    Sigaction(SIGINT, &myaction, NULL); // install sigchild_handler.
    
    struct sigaction myaction2 = {{0}}; 
    myaction2.sa_handler = sigusr1_handler; 
    Sigaction(SIGUSR1, &myaction2, NULL); // install sigchild_handler.
    
    init_locks();// initialize all locks.
    init_server_stats(); // initialize server stats.


    pthread_t tid;
    
    // CREATE WRITER THREAD HERE

    sigset_t sigint_mask;
    sigemptyset(&sigint_mask);
    sigaddset(&sigint_mask, SIGINT);
    
    int writer_listen_fd = socket_listen_init(w_port_number);

    int* w_listen_fd = malloc(sizeof(int));
    *w_listen_fd = writer_listen_fd;
    
    pthread_sigmask(SIG_BLOCK, &sigint_mask, NULL); // block sigint
    pthread_create(&tid, NULL, w_thread, (void*) w_listen_fd);
    pthread_sigmask(SIG_UNBLOCK, &sigint_mask, NULL);

    add_client_thread(thread_id_list, tid);
    
    printf("Listening for writers on port %d.\n", w_port_number);
    
    // Initiate server socket for listening for reader clients
    int reader_listen_fd = socket_listen_init(r_port_number); 
    printf("Listening for readers on port %d.\n", r_port_number);

    int reader_fd;
    struct sockaddr_in client_addr;
    unsigned int client_addr_len = sizeof(client_addr);

    while(1) {
        // Wait and Accept the connection from client
        if (sigint_flag == 1) {
            break;
        }
        do {
            reader_fd = accept(reader_listen_fd, (SA*)&client_addr, &client_addr_len);
        }
        while ((reader_fd < 0 && errno == EINTR && sigint_flag == 0)); // perfect
        if (sigint_flag == 1)
            break;
        if (reader_fd < 0) { // failed, but not because of a signal.
            printf("server accept failed\n");
            exit(EXIT_FAILURE);
        }
        
        // INSERT SERVER ACTIONS FOR CONNECTED READER CLIENT CODE HERE
        int* r_thread_fd = malloc(sizeof(int));
        *r_thread_fd = reader_fd;
        
        reap_current_client_threads(thread_id_list);

        pthread_sigmask(SIG_BLOCK, &sigint_mask, NULL);
        if (pthread_create(&tid, NULL, r_thread, (void*) r_thread_fd) == -1){
            free(r_thread_fd);
            close(reader_fd);
            continue;
        }
        pthread_sigmask(SIG_UNBLOCK, &sigint_mask, NULL);
        add_client_thread(thread_id_list, tid);

        pthread_mutex_lock(&clientCnt_lock);
        clientCnt++; // total client connections made. NOW NEED MASK;
        pthread_mutex_unlock(&clientCnt_lock);
    }

    close(reader_listen_fd);
    close_server(thread_id_list, clientCnt, maxDonations, charities);
    destroy_locks();
    
    close(log_fd);
    DeleteList(thread_id_list);
    free(thread_id_list);
    return 0;
}


void* w_thread(void* w_listener_fd_ptr) {
    int w_listener_fd = *(((int*) w_listener_fd_ptr));
    free(w_listener_fd_ptr);
    
    struct sockaddr_in client_addr;
    unsigned int client_addr_len = sizeof(client_addr);

    int running = 1;
    int writer_fd;
    while(running) {
        if (sigusr1_flag == 1) {
            break;
        }
        do{
            writer_fd = accept(w_listener_fd, (SA*)&client_addr, &client_addr_len);
        }
        while (writer_fd < 0 && errno == EINTR && sigusr1_flag == 0);
        if (sigusr1_flag == 1) {
            break;
        }
        if (writer_fd < 0) {
            printf("writer thread accept failed\n");
            continue;
        }
        pthread_mutex_lock(&clientCnt_lock);
        clientCnt++;
        pthread_mutex_unlock(&clientCnt_lock);

        uint64_t total_donations = 0; // thread tracking donations to charities.
        message_t msg;
        int connected = 1;
        int need_close = 1;
        int logout_updated = 0;
        while(connected){
            if (sigusr1_flag == 1) {
                running = 0;
                break;
            }
            memset(&msg, 0, sizeof(message_t));
            int size = read(writer_fd, &msg, sizeof(message_t));
            if (size == -1) {
                if (errno == EINTR) {
                    if (sigusr1_flag == 1) {
                        running = 0;
                        break;
                    }
                    continue;
                }
                break;
            }
            if (size == 0) {
                break; // client disconnected
            }
            if (size < sizeof(message_t)) {
                client_error(&msg, writer_fd);
                continue;
            }
            switch(msg.msgtype) {
                case DONATE:
                    if (writer_donate(&msg, writer_fd) == 0)
                        total_donations += msg.msgdata.donation.amount;
                    break;
                case LOGOUT:
                    writer_logout(writer_fd, total_donations);
                    need_close = 0;
                    connected = 0;
                    logout_updated = 1;
                    break;
                default:
                    client_error(&msg, writer_fd);
                    break;
            }
        }
        if (need_close == 1)
            close(writer_fd);
        if(sigusr1_flag == 0 && logout_updated == 0) {
            if (check_update_maxDonations(total_donations) == 1)
                update_maxDonations(total_donations);
        }
    }
    close(w_listener_fd);
    return NULL;
}

void* r_thread(void* readerfd_ptr) {
    int reader_fd =*(((int*) readerfd_ptr));
    free(readerfd_ptr);

    message_t msg;
    while(1) {
        if (sigusr1_flag == 1)
            break;
        memset(&msg, 0, sizeof(message_t));

        int size = read(reader_fd, &msg, sizeof(message_t));
        if (size == -1) {
            if (errno == EINTR) {
                if (sigusr1_flag == 1) {
                    break;
                }
                continue;
            }
            break;
        }
        if (size == 0) {
            break; // client disconnected
        }
        if (size < sizeof(message_t)) {
            client_error(&msg, reader_fd);
            continue;
        }
        switch(msg.msgtype) {
            case CINFO:
                reader_info(&msg, reader_fd);
                break;
            case TOP:
                reader_top(&msg, reader_fd);
                break;
            case STATS:
                reader_stats(&msg, reader_fd);
                break;
            case LOGOUT:
                reader_logout(reader_fd);
                return NULL;
            default:
                client_error(&msg, reader_fd);
                break;
        }
    }
    close(reader_fd);
    return NULL;
}

int socket_listen_init(int server_port){
    int sockfd;
    struct sockaddr_in servaddr;

    // socket create and verification
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd == -1) {
        printf("socket creation failed...\n");
        exit(EXIT_FAILURE);
    }
    else
        printf("Socket successfully created\n");

    bzero(&servaddr, sizeof(servaddr));

    // assign IP, PORT
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(server_port);

    int opt = 1;
    
    if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, (char *)&opt, sizeof(opt))<0)
    {
    	perror("setsockopt");exit(EXIT_FAILURE); 
    }

    // Binding newly created socket to given IP and verification
    if ((bind(sockfd, (SA*)&servaddr, sizeof(servaddr))) != 0) {
        printf("socket bind failed\n");
        exit(EXIT_FAILURE);
    }
    else
        printf("Socket successfully binded\n");

    // Now server is ready to listen and verification
    if ((listen(sockfd, 1)) != 0) {
        printf("Listen failed\n");
        exit(EXIT_FAILURE);
    }
    return sockfd;
}


