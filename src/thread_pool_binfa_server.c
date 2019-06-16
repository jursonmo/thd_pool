#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <netinet/in.h>//in_addr结构
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <error.h>
#include <errno.h>
#include <sys/time.h>
#include <time.h>
#include <pthread.h>
#include <net/if.h> 
#include <signal.h>
#include <getopt.h>
#include "thread_pool.h"

#define __DEBUG__

#ifdef __DEBUG__
#define DEBUG(format,...)	printf(format,##__VA_ARGS__)
#else
#define DEBUG(format,...) 
#endif

#define THD_MIN 3
#define THD_NUM 9 

static pthread_mutex_t lock;
static unsigned exit_cnt;
TpThreadPool *pTp;

void proc_fun(void *arg){
	static unsigned int i=0;
	int idx=*((int*)arg);
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
	int i,acc_fd;

	exit_cnt = 0;
	pthread_mutex_init(&lock, NULL);
	tp_init(pTp);//对pTp 的队列及其线程初始化(包括管理线程)
    
    while(1) {
        if((acc_fd=accept(sock_fd,(struct sockaddr *)&peer_addr,&addr_len))<0){
             perror("accept error");
        }
		tp_process_job(pTp, proc_fun, &acc_fd);
    }
    
    fprintf(stderr, "======================wait to close all thread ===============\n");
    //sleep(10);
	tp_run(pTp);//tp_close(pTp, TRUE);-->pTp->stop_flag = TRUE;终止所有线程
	free(pTp);
	fprintf(stderr, "All jobs done!\n");
	return 0;
}

int mysocket(char *proto){
   int sock_fd;
   if(strcmp(proto,"tcp")==0) {
      if((sock_fd=socket(AF_INET,SOCK_STREAM,0))<0) {
          DEBUG("create TCP socket fail\n");
          return 0;
      }
      else{
          DEBUG("create TCP socket %d success\n",sock_fd);

      }
   }
   else{
      if((sock_fd=socket(AF_INET,SOCK_DGRAM,0))<0) {
          DEBUG("create UDP socket fail\n");
          return 0;
      }
      else{
          DEBUG("create UDP socket %d success\n",sock_fd);
      }
   }
    sock_reuse(sock_fd);
    return sock_fd;
}
