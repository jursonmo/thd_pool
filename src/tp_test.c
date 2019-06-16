
#define __DEBUG__

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "thread_pool.h"

#define THD_MIN 3
#define THD_NUM 9 

static pthread_mutex_t lock;
static unsigned exit_cnt;
TpThreadPool *pTp;



void proc_fun(void *arg){
	static unsigned int i=0;
	int idx=*((int*) arg);
	//i = 1000000.0 + (int)(9000000.0 * rand() / RAND_MAX);
    i=i+1000000;
	fprintf(stderr, "Begin: job %d, sleep %d us\n", idx, i);
	usleep(i);
	fprintf(stderr, "End:   job %d\n", idx);
	pthread_mutex_lock(&lock);
	exit_cnt++;
    fprintf(stderr, "exit_cnt ==%d\n",exit_cnt);
	pthread_mutex_unlock(&lock);
	if(exit_cnt == THD_NUM){
        fprintf(stderr, "======================exit_cnt == THD_NUM===============\n");
		tp_exit(pTp);
    }
} 

//gcc -o tp_test tp_test.c thread_pool.c thread_pool.h tsqueue.c tsqueue.h -lpthread
int main(int argc, char **argv){
	pTp= tp_create(THD_MIN, THD_NUM);
	int i;

	exit_cnt = 0;
	pthread_mutex_init(&lock, NULL);
	tp_init(pTp);//对pTp 的队列及其线程初始化(包括管理线程)
	srand((int)time(0));
    int arg[THD_NUM];
	for(i=0; i < THD_NUM; i++){
        arg[i]=i;
		tp_process_job(pTp, proc_fun, &arg[i]);
	}

    fprintf(stderr, "======================wait to close all thread ===============\n");
    //sleep(10);
	tp_run(pTp);//tp_close(pTp, TRUE);-->pTp->stop_flag = TRUE;终止所有线程
	free(pTp);
	fprintf(stderr, "All jobs done!\n");
	return 0;
}

