#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <math.h>

#include "util.h"
#define REST 0
#define PREPARE 1
#define RESOLVE 2
#define CAPTURE 3
#define COMPLETE 4

#define PAGE_SIZE 4096

typedef struct {
    long long int size;
    char* live;
    char* stable;
    int* bit;
    pthread_mutex_t* live_lock;
    pthread_mutex_t* stable_lock;
    int STATE;
} database;

database global_db;
int is_finished = 0;     //程序是否结束
long long int throughput = 0;   //  系统最大并行度
long long int active=0,prepare=0,complete=0;


int* msec_throughput;
long long int timestamp = 0;
int ckp_fd;  // 文件



void load_db(long long int size) {
    global_db.size = size;
    global_db.live =        (char *) malloc(global_db.size * PAGE_SIZE);
    global_db.stable =      (char *) malloc(global_db.size * PAGE_SIZE);
    global_db.bit =         (int *)  malloc(global_db.size * sizeof(int));
    global_db.live_lock =   (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    global_db.stable_lock = (pthread_mutex_t *) malloc(global_db.size * sizeof(pthread_mutex_t));
    global_db.STATE = REST;

    long long int i = 0;
    while(i < global_db.size) {
        pthread_mutex_init(&(global_db.live_lock[i]),NULL);
        pthread_mutex_init(&(global_db.stable_lock[i]),NULL);
        i++;
    }
}

void ApplyWrite(int start_state,long long int index1, int value1)
{
    if(start_state == PREPARE) {
        if (global_db.bit[index1] == 0)
            memcpy(global_db.stable + PAGE_SIZE * index1, global_db.live + PAGE_SIZE * index1, PAGE_SIZE);
    }else if(start_state == RESOLVE || start_state == CAPTURE){
            if(global_db.bit[index1] == 0) {
                memcpy(global_db.stable + PAGE_SIZE * index1,global_db.live + PAGE_SIZE * index1,PAGE_SIZE);
                global_db.bit[index1] = 1;
            }
    }else if(start_state == COMPLETE || start_state == REST){
        if(global_db.stable[index1]){
        }
    }

    //memset(global_db.live + PAGE_SIZE * index1, &value1, PAGE_SIZE);
    int k =0;
    while(k++ < 1024)
    {
        memcpy( global_db.live + PAGE_SIZE * index1 + 4*k , &value1, 4);
    }
    //printf("~");
}


//   采用两阶段锁操作并发事务
void Execute(int start_state)
{
    //long long int start_time = get_ntime();
    long long int index1 = rand() % (global_db.size);   int value1 = rand();
    long long int index2 = rand() % (global_db.size);   int value2 = rand();
    long long int index3 = rand() % (global_db.size);   int value3 = rand();
    //pthread_mutex_lock(&(global_db.live_lock[index1]));    pthread_mutex_lock(&(global_db.stable_lock[index1]));
    //pthread_mutex_lock(&(global_db.live_lock[index2]));    pthread_mutex_lock(&(global_db.stable_lock[index2]));
    //pthread_mutex_lock(&(global_db.live_lock[index3]));    pthread_mutex_lock(&(global_db.stable_lock[index3]));
    ApplyWrite(start_state,index1,value1);
    //pthread_mutex_unlock(&(global_db.live_lock[index1]));    pthread_mutex_unlock(&(global_db.stable_lock[index1]));
    ApplyWrite(start_state,index2,value2);
    //pthread_mutex_unlock(&(global_db.live_lock[index2]));    pthread_mutex_unlock(&(global_db.stable_lock[index2]));
    ApplyWrite(start_state,index3,value3);
    //pthread_mutex_unlock(&(global_db.live_lock[index3]));    pthread_mutex_unlock(&(global_db.stable_lock[index3]));

    //pthread_mutex_lock(&mutex_state);
    int commit_state = global_db.STATE;
    //pthread_mutex_unlock(&mutex_state);
    if(start_state == PREPARE){
        if(commit_state == RESOLVE) {
            global_db.bit[index1]=1;
        }
    }
    ++msec_throughput[timestamp];
    //printf("t");
    //printf("%lld\n", get_ntime()-start_time);
}

void* transaction(void* info) {
    while(is_finished==0)
    {
        int start_state = global_db.STATE;
        if(start_state == REST || start_state == COMPLETE) {
            __sync_fetch_and_add(&active,1);
            Execute(start_state);
            __sync_fetch_and_sub(&active,1);
        } else if(start_state == PREPARE) {
            __sync_fetch_and_add(&prepare,1);
            Execute(start_state);
            __sync_fetch_and_sub(&prepare,1);
        } else if(start_state == CAPTURE || start_state == RESOLVE) {
            __sync_fetch_and_add(&complete,1);
            Execute(start_state);
            __sync_fetch_and_sub(&complete,1);
        }
    }
}


void* run_mtime(void* info){
    while(is_finished==0){
        sleep(1);
		printf("%d\n",msec_throughput[timestamp]);
        ++timestamp;
		//printf(",");
    }
}

void checkpointer(int num) {
    sleep(5);  //第一次检查点直接用5s替代算了
    while(num--) {
        global_db.STATE = REST;
        sleep(30);
        global_db.STATE = PREPARE;
        while(active>0);
        global_db.STATE = RESOLVE;
        while(prepare>0);
        global_db.STATE = CAPTURE;

        long long int i = 0;
        ckp_fd = open("./dump.dat", O_WRONLY | O_TRUNC | O_SYNC | O_CREAT, 666);
        while(i < global_db.size) {
            if(global_db.bit[i] == 1) {
                write(ckp_fd, global_db.stable + i * PAGE_SIZE,PAGE_SIZE);
                lseek(ckp_fd, 0, SEEK_END);
            } else if(global_db.bit[i] == 0) {
                global_db.bit[i] == 1;
                write(ckp_fd, global_db.live + i * PAGE_SIZE,PAGE_SIZE);
                lseek(ckp_fd, 0, SEEK_END);
            }
            i++;
        }
        global_db.STATE = COMPLETE;
        while(complete>0);
    }
    is_finished = 1;
}

int main(int argc, char const *argv[]) {
    srand((unsigned)time(NULL));
    load_db(atoi(argv[1]));

    msec_throughput = (int*) malloc(1000 * sizeof(int));  // assume running 1000s
    for (int j = 0; j < 1000; ++j) {
        msec_throughput[j] = 0;
    }

    throughput = atoi(argv[2]);

    for (int i = 0; i < throughput; ++i)
    {
        pthread_t pid_t;
        pthread_create(&pid_t,NULL,transaction,NULL);
    }
    pthread_t time_thread;
    pthread_create(&time_thread,NULL,run_mtime,NULL);
    checkpointer(5);
	//for(int i=0;i<timestamp;i++)
	//{
		//printf("%d\n",msec_throughput[i]);
	//}
    return 0;
}
