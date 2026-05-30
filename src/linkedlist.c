
#include "linkedlist.h"

list_t* CreateList(void (*delete)(void*)) {
    list_t* list = malloc(sizeof(list_t));
    list->deleter = delete;  // Implementation assumes Deleter deletes data referenced in node_t only
    list->length = 0;
    list->head = NULL;
    return list;
}

void InsertAtHead(list_t* list, pthread_t tid) { // approved
    if (list->length == 0) list->head = NULL;
    
    node_t* new_node = malloc(sizeof(node_t));
    pthread_t* new_tid = malloc(sizeof(pthread_t));
    *new_tid = tid;
    new_node->data = new_tid;
    new_node->next = list->head;
    new_node->prev = NULL;

    if(list->head != NULL)
        (list->head)->prev = new_node;
    
    list->head = new_node;
    list->length++;
}


void RemoveFromList(list_t* list, node_t* node) { // approved
    if (list == NULL || node == NULL)
        return;
    node_t* next_node = node->next;
    node_t* prev_node = node->prev;
    if (next_node != NULL && prev_node != NULL) {
        next_node->prev = prev_node;
        prev_node->next = next_node;
    } else if (next_node != NULL && prev_node == NULL) {
        next_node->prev = NULL;
        list->head = next_node;
    } else if (next_node == NULL && prev_node != NULL) {
        prev_node->next = NULL;
    } else {
        list->head = NULL;
    }
    list->deleter(node->data);
    free(node);
    list->length--;
}

void DeleteList(list_t* list) { // approved
    if (list->length == 0)
        return;
    
    while (list->head != NULL){
        RemoveFromList(list, list->head);
    }
    list->length = 0;
}
