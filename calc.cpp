#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <math.h>

#include "util.h"
#include <iostream>
#include <fstream>

using namespace std;

#define REST 0
#define PREPARE 1
#define RESOLVE 2
#define CAPTURE 3
#define COMPLETE 4

#define LINE_SIZE 4096

typedef struct {
    long long int size;
    char *live;
    char *stable;
    int *bit;
    pthread_mutex_t *live_lock;
    pthread_mutex_t *stable_lock;
    int STATE;
} database;

database global_db;
int is_finished = 0;     //程序是否结束
long long int throughput = 0;   //  系统最大并行度
long long int active = 0, prepare = 0, complete = 0;


int *sec_throughput;
long long int timestamp = 0;


void load_db(long long int size) {
    global_db.size = size;
    global_db.live = (char *) malloc(global_db.size * LINE_SIZE);
    global_db.stable = (char *) malloc(global_db.size * LINE_SIZE);
    global_db.bit = (int *) malloc(global_db.size * sizeof(int));
    global_db.live_lock = (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    global_db.stable_lock = (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    global_db.STATE = REST;

    long long int i = 0;
    while (i < global_db.size) {
        pthread_mutex_init(&(global_db.live_lock[i]), NULL);
        pthread_mutex_init(&(global_db.stable_lock[i]), NULL);
        i++;
    }
}

void ApplyWrite(int start_state, long long int index) {
    if (start_state == PREPARE) {
        //if (global_db.bit[index] == 0)
            memcpy(global_db.stable + LINE_SIZE * index, global_db.live + LINE_SIZE * index, LINE_SIZE);
    } else if (start_state == RESOLVE || start_state == CAPTURE) {
        //if (global_db.bit[index] == 0) {
            memcpy(global_db.stable + LINE_SIZE * index, global_db.live + LINE_SIZE * index, LINE_SIZE);
            global_db.bit[index] = 1;
        //}
    } else if (start_state == COMPLETE || start_state == REST) {
        if (global_db.stable[index]) {
        }
    }
    // set each filed data.
    int k = 0;
    int rnd;
    rnd = rand();
    while (k++ < 1024) {
        memcpy(global_db.live + LINE_SIZE * index + 4 * k, &rnd, 4);
    }
}


//   采用两阶段锁操作并发事务
void Execute(int start_state) {
    int i = 0;
    long long int index[3];
    while (i < 3) {
        index[i] = rand() % (global_db.size);   //int value1 = rand();
        ApplyWrite(start_state, index[i]);
        i++;
    }
    int commit_state = global_db.STATE;
    if (start_state == PREPARE) {
        if (commit_state == RESOLVE) {
            i = 0;
            while (i < 3) {
                global_db.bit[index[i]] = 1;
                i++;
            }
        }
    }
    //cout<<start_state;
    ++sec_throughput[timestamp];
}

void *transaction(void *info) {
    while (is_finished == 0) {
        int start_state = global_db.STATE;
        if (start_state == REST || start_state == COMPLETE) {
            __sync_fetch_and_add(&active, 1);
            Execute(start_state);
            __sync_fetch_and_sub(&active, 1);
        } else if (start_state == PREPARE) {
            __sync_fetch_and_add(&prepare, 1);
            Execute(start_state);
            __sync_fetch_and_sub(&prepare, 1);
        } else if (start_state == CAPTURE || start_state == RESOLVE) {
            __sync_fetch_and_add(&complete, 1);
            Execute(start_state);
            __sync_fetch_and_sub(&complete, 1);
        }
    }
}


void *timer(void *info) {
    while (is_finished == 0) {
        sleep(1);
        //printf("%d\n",sec_throughput[timestamp]);
        ++timestamp;
    }
}

void checkpointer(int num) {
    while (num--) {
        global_db.STATE = REST;
        sleep(1);
        global_db.STATE = PREPARE;
        while (active > 0);
        global_db.STATE = RESOLVE;
        while (prepare > 0);
        global_db.STATE = CAPTURE;

        long long int i = 0;
        ofstream ckp_fd("dump.dat", ios::binary);
        while (i < global_db.size) {
            if (global_db.bit[i] == 1) {
                ckp_fd.write((global_db.stable + i * LINE_SIZE), LINE_SIZE);
            } else if (global_db.bit[i] == 0) {
                global_db.bit[i] == 1;
                ckp_fd.write(global_db.live + i * LINE_SIZE, LINE_SIZE);
            }
            i++;
        }
        global_db.STATE = COMPLETE;
        while (complete > 0);
    }
    is_finished = 1;
}

int main(int argc, char const *argv[]) {
    srand((unsigned) time(NULL));
    load_db(atoi(argv[1]));

    sec_throughput = (int *) malloc(3600 * sizeof(int));  // assume running 1000s
    for (int j = 0; j < 1000; ++j) {
        sec_throughput[j] = 0;
    }

    throughput = atoi(argv[2]);

    for (int i = 0; i < throughput; ++i) {
        pthread_t pid_t;
        pthread_create(&pid_t, NULL, transaction, NULL);
    }
    pthread_t time_thread;
    pthread_create(&time_thread, NULL, timer, NULL);
    checkpointer(5);
    long long int sum = 0;
    for (int i = timestamp / 4; i < timestamp / 4 * 3; ++i) {
        sum += sec_throughput[i];
    }
    printf("CALC,%d,%lld,%f\n", atoi(argv[1]), throughput, (float) sum / timestamp * 2);
    return 0;
}
