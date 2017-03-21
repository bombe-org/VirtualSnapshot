#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <map>
#include <fcntl.h>
#include <math.h>

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
long long int count = 0;
long long int active=0,prepare=0,complete=0;
pthread_mutex_t mutex_active;
pthread_mutex_t mutex_prepare;
pthread_mutex_t mutex_complete;
pthread_mutex_t mutex_state;


int* sec_throughput;
int run_time;
int ckp_fd;  // 文件描述符


long long get_ntime(void)
{
    struct timespec timeNow;
    clock_gettime(CLOCK_MONOTONIC, &timeNow);
    return timeNow.tv_sec * 1000000000 + timeNow.tv_nsec;
}

long long get_utime(void)
{
    return get_ntime() / 1000;
}

long long get_time(void)
{
     struct timespec timeNow;
    clock_gettime(CLOCK_MONOTONIC, &timeNow);
    return timeNow.tv_sec;
}


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

void work(int start_state)
{
    long long int index1 = rand() % (global_db.size);   int value1 = rand();    
    pthread_mutex_lock(&(global_db.live_lock[index1]));
    pthread_mutex_lock(&(global_db.stable_lock[index1]));
    //printf("*");

    if(start_state == PREPARE)
        if(global_db.bit[index1] == 0)
            memcpy(global_db.stable + PAGE_SIZE * index1,global_db.live + PAGE_SIZE * index1,1024);
        else if(start_state == RESOLVE || start_state == CAPTURE)
            if(global_db.bit[index1] == 0) {
                memcpy(global_db.stable + PAGE_SIZE * index1,global_db.live + PAGE_SIZE * index1,1024);
                global_db.bit[index1] = 1;
            } else if(start_state == COMPLETE || start_state == REST)
                if(global_db.stable[index1]);
    memset(global_db.live + PAGE_SIZE * index1,value1,1024);    
    //pthread_mutex_lock(&mutex_state);
    int commit_state = global_db.STATE;
    //pthread_mutex_unlock(&mutex_state);
    if(start_state == PREPARE){
        if(commit_state == RESOLVE) {
            global_db.bit[index1]=1;    
        }
    }       
    sec_throughput[get_time()-run_time] += 1;
    //printf("!");
    pthread_mutex_unlock(&(global_db.live_lock[index1]));
    pthread_mutex_unlock(&(global_db.stable_lock[index1]));    
}

//   采用两阶段锁操作并发事务
void* transaction(void* info) {
	while(is_finished==0)
    {
        int start_state = global_db.STATE;        
        if(start_state == REST || start_state == COMPLETE) {            
            active++;
            work(start_state);
            active--;            
        } else if(start_state == PREPARE) {            
            prepare++;  
            work(start_state);
            prepare--;          
        } else if(start_state == CAPTURE || start_state == RESOLVE) {            
            complete++;
            work(start_state);            
            complete--;            
        }       
        usleep(1000);
    }
	
}



void checkpointer(int num) {
	while(num--) {
		printf("\nREST\n");
		//pthread_mutex_lock(&mutex_state);
		global_db.STATE = REST;
		//pthread_mutex_unlock(&mutex_state);
        long long int cu = get_utime() + 1000000;
        while(abs(cu- get_utime()) >= 100) {;}
		//pthread_mutex_lock(&mutex_state);
		global_db.STATE = PREPARE;
		//pthread_mutex_unlock(&mutex_state);
		printf("\nPREPARE\n" );
		while(active>0);
		//pthread_mutex_lock(&mutex_state);
		//global_db.STATE = RESOLVE;
		//pthread_mutex_unlock(&mutex_state);
		printf("\nRESOLVE\n");
		while(prepare>0);
		printf("\nCAPTURE\n");
		//pthread_mutex_lock(&mutex_state);
		global_db.STATE = CAPTURE;
		//pthread_mutex_unlock(&mutex_state);

		long long int i = 0;
        ckp_fd = open("./dump.dat", O_WRONLY | O_TRUNC | O_SYNC | O_CREAT, 666);
		while(i < global_db.size) {
			if(global_db.bit[i] == 1) {				
                write(ckp_fd, global_db.stable + i * PAGE_SIZE,PAGE_SIZE);
                //lseek(ckp_fd, 0, SEEK_END);
			} else if(global_db.bit[i] == 0) {
				global_db.bit[i] == 1;
				write(ckp_fd, global_db.live + i * PAGE_SIZE,PAGE_SIZE);
                //lseek(ckp_fd, 0, SEEK_END);
			}				       
			i++;
		}
		printf("\nCOMPLETE\n");
		global_db.STATE = COMPLETE;
		while(complete>0);
	}
	is_finished = 1;
}

int main(int argc, char const *argv[]) {
	srand((unsigned)time(NULL));
	pthread_mutex_init(&mutex_active,NULL);
	pthread_mutex_init(&mutex_prepare,NULL);
	pthread_mutex_init(&mutex_complete,NULL);
	pthread_mutex_init(&mutex_state,NULL);
    
	load_db(atoi(argv[1]));
    sec_throughput = (int*) malloc(10000000 * sizeof(int));
    run_time = get_time();
	throughput = atoi(argv[2]);
    for (int i = 0; i < throughput; ++i)
    {
        pthread_t pid_t;
        pthread_create(&pid_t,NULL,transaction,NULL);
    }


	checkpointer(10);
    int end_time = get_time()-run_time;
    for (int i = 0; i < end_time; ++i)
    {
        printf("%d,%d\n",i,sec_throughput[i]);
    }
	return 0;
}
