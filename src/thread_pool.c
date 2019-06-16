/*
 * @author: boyce
 * @contact: boyce.ywr#gmail.com (# -> @)
 * @version: 1.02
 * @created: 2011-07-25
 * @modified: 2011-08-04
 * @modified: 2012-05-14
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

#include "thread_pool.h"

#define __DEBUG__

#ifdef __DEBUG__
#define DEBUG(format,...)	printf(format,##__VA_ARGS__)
#else
#define DEBUG(format,...) 
#endif

static TpThreadInfo *tp_add_thread(TpThreadPool *pTp, process_job proc_fun, void *job);
static int tp_delete_thread(TpThreadPool *pTp); 
static int tp_get_tp_status(TpThreadPool *pTp); 

static void *tp_work_thread(void *pthread);
static void *tp_manage_thread(void *pthread);

/**
 * user interface. creat thread pool.
 * para:
 * 	num: min thread number to be created in the pool
 * return:
 * 	thread pool struct instance be created successfully
 */
TpThreadPool *tp_create(unsigned min_num, unsigned max_num) {
	TpThreadPool *pTp;
	pTp = (TpThreadPool*) malloc(sizeof(TpThreadPool));

	memset(pTp, 0, sizeof(TpThreadPool));

	//init member var
	pTp->min_th_num = min_num;
	pTp->cur_th_num = min_num;
	pTp->max_th_num = max_num;
	pthread_mutex_init(&pTp->tp_lock, NULL);
	pthread_cond_init(&pTp->tp_cond, NULL);
	pthread_mutex_init(&pTp->loop_lock, NULL);
	pthread_cond_init(&pTp->loop_cond, NULL);

	//malloc mem for num thread info struct
	if (NULL != pTp->thread_info)
		free(pTp->thread_info);
	pTp->thread_info = (TpThreadInfo*) malloc(sizeof(TpThreadInfo) * pTp->max_th_num);
	memset(pTp->thread_info, 0, sizeof(TpThreadInfo) * pTp->max_th_num);

	return pTp;
}

/**
 * member function reality. thread pool init function.
 * para:
 * 	pTp: thread pool struct instance ponter
 * return:
 * 	true: successful; false: failed
 */
int tp_init(TpThreadPool *pTp) {//对pTp 的队列及每个线程初始化
	int i;
	int err;
	TpThreadInfo *pThi;//线程信息

	//init_queue(&pTp->idle_q, NULL);
	pTp->idle_q = ts_queue_create();//创建一个空闲的队列,队列里有TSQItem链表
	pTp->stop_flag = FALSE;
	pTp->busy_threshold = BUSY_THRESHOLD;//busy_num/(pTp->cur_th_num) < pTp->busy_threshold
	pTp->manage_interval = MANAGE_INTERVAL;

	//create work thread and init work thread info
	for (i = 0; i < pTp->min_th_num; i++) {
		pThi = pTp->thread_info + i;
		pThi->tp_pool = pTp;
		pThi->is_busy = FALSE;
		pthread_cond_init(&pThi->thread_cond, NULL);
		pthread_mutex_init(&pThi->thread_lock, NULL);
		pThi->proc_fun = NULL;
		pThi->arg = NULL;
		ts_queue_enq_data(pTp->idle_q, pThi);

		err = pthread_create(&pThi->thread_id, NULL, tp_work_thread, pThi);
		if (0 != err) {
			perror("tp_init: create work thread failed.");
			ts_queue_destroy(pTp->idle_q);//ts_queue_create->ts_queue_init :pthread_mutex_init
			return -1;                    //ts_queue_destroy:pthread_mutex_destroy销除锁
		}
	}

	//create manage thread
	err = pthread_create(&pTp->manage_thread_id, NULL, tp_manage_thread, pTp);
	if (0 != err) {//clear_queue(&pTp->idle_q);
		ts_queue_destroy(pTp->idle_q);
		fprintf(stderr, "tp_init: creat manage thread failed\n");
		return 0;
	}
	
	//wait for all threads are ready
    fprintf(stderr, "tp_init: wait for all threads are ready, i=%d,cur_th_num=%d\n",i,pTp->cur_th_num);
	while(i++ < pTp->cur_th_num){// i++ > min_th_num==cur_th_num tp_create()初始化
		fprintf(stderr, "tp_init: i++ < pTp->cur_th_num\n");
		pthread_mutex_lock(&pTp->tp_lock);
		pthread_cond_wait(&pTp->tp_cond, &pTp->tp_lock);//等待tp_work_thread唤醒?
		pthread_mutex_unlock(&pTp->tp_lock);
	}
	DEBUG("All threads are ready now\n");
	return 0;
}

/**
 * let the thread pool wait until {@link #tp_exit} is called
 * @params:
 *	pTp: pointer of thread pool
 * @return
 *	none
 */
void tp_run(TpThreadPool *pTp){
	pthread_mutex_lock(&pTp->loop_lock);
	pthread_cond_wait(&pTp->loop_cond, &pTp->loop_lock);//等待tp_exit来唤醒退出
	pthread_mutex_unlock(&pTp->loop_lock);
	tp_close(pTp, TRUE);
}

/**
 * let the thread pool exit, this function will wake up {@link #tp_loop}
 * @params:
 *	pTp: pointer of thread pool
 * @return
 *	none
 */
void tp_exit(TpThreadPool *pTp){//执行完计划的任务数就唤醒,以达到退出目的
	pthread_cond_signal(&pTp->loop_cond);//唤醒tp_run的wait()
}

/**
 * member function reality. thread pool entirely close function.
 * para:
 * 	pTp: thread pool struct instance ponter
 * return:
 */
void tp_close(TpThreadPool *pTp, BOOL wait) {//wait=TRUE
	unsigned i;

	pTp->stop_flag = TRUE;
	if (wait) {
		DEBUG("current number of threads: %u\n", pTp->cur_th_num);
        for (i = 0; i < pTp->cur_th_num; i++) {
			DEBUG("all thread i =%d ,thread_id[%d]=%u.\n",i,i,pTp->thread_info[i].thread_id);
        }
		for (i = 0; i < pTp->cur_th_num; i++) {
			DEBUG("pthread_cond_signal i =%d ,thread_id[%d]=%u.\n",i,i,pTp->thread_info[i].thread_id);
            pthread_cancel((pid_t)pTp->thread_info[i].thread_id);//add by mojianwei,会在wait()退出点退出,再join此线程
			//pthread_cond_signal(&pTp->thread_info[i].thread_cond);//不用这个,因为还没进入wait()状态,此信号是没用的
		}
        
		for (i = 0; i < pTp->cur_th_num; i++) {
			DEBUG("joining a thread i =%d ,thread_id=%u.\n",i,pTp->thread_info[i].thread_id);
			if(0 != pthread_join(pTp->thread_info[i].thread_id, NULL)){
				perror("pthread_join");
			}
			DEBUG("join a thread success i =%d.\n",i);
			pthread_mutex_destroy(&pTp->thread_info[i].thread_lock);
			pthread_cond_destroy(&pTp->thread_info[i].thread_cond);
		}
	} else {
		//close work thread
		for (i = 0; i < pTp->cur_th_num; i++) {
			kill((pid_t)pTp->thread_info[i].thread_id, SIGKILL);
			pthread_mutex_destroy(&pTp->thread_info[i].thread_lock);
			pthread_cond_destroy(&pTp->thread_info[i].thread_cond);
		}
	}
	//close manage thread
	kill((pid_t)pTp->manage_thread_id, SIGKILL);
	pthread_mutex_destroy(&pTp->tp_lock);
	pthread_cond_destroy(&pTp->tp_cond);
	pthread_mutex_destroy(&pTp->loop_lock);
	pthread_cond_destroy(&pTp->loop_cond);

	//clear_queue(&pTp->idle_q);
	ts_queue_destroy(pTp->idle_q);
	//free thread struct
	free(pTp->thread_info);
	pTp->thread_info = NULL;
}

/**
 * member function reality. main interface opened.
 * after getting own worker and job, user may use the function to process the task.
 * para:
 * 	pTp: thread pool struct instance ponter
 *	worker: user task reality.
 *	job: user task para
 * return:
 */
int tp_process_job(TpThreadPool *pTp, process_job proc_fun, void *arg) {
	TpThreadInfo *pThi ;
	//fill pTp->thread_info's relative work key
	pThi = (TpThreadInfo *) ts_queue_deq_data(pTp->idle_q);//从空闲队列里获取头节点并从队列中删除此头节点线程
                                                           //线程处理完后会自动添加到空闲队列里tp_work_thread
    //疑问:pThi返回的是节点data指针,data指针指向线程池里的线程A,pThi也就指向线程A,
    //如果此刻有空闲线程B加入,会占用刚才返回的节点,此时data指针就指向线程池里的线程B,
    //跟pThi指向的线程A没有关系,只要确定pThi即将要指向的data指针瞬间data不被指向线程B
    //线程池和队列的关系
    if(pThi){
		DEBUG("Fetch a thread from pool.\n");
		pThi->is_busy = TRUE;
		pThi->proc_fun = proc_fun;
		pThi->arg = arg;
		//let the thread to deal with this job
		DEBUG("wake up thread %u\n", pThi->thread_id);
		pthread_cond_signal(&pThi->thread_cond);
        //线程pThi已经从空闲队列中取出,从tp_work_thread wait()中唤醒它调用proc_fun来处理
	}
	else{//pThi==NULL ,即空闲队列里没有线程了,也就没有空闲线程了,需要创建新的线程,并处理
                       //
		//if all current thread are busy, new thread is created here
		if(!(pThi = tp_add_thread(pTp, proc_fun, arg))){//池已经满了,返回NULL,->tp_work_thread->处理完会加入队列里
			DEBUG("The thread pool is full, no more thread available.\n");
			return -1;
		}
		/* should I wait? */
		//pthread_mutex_lock(&pTp->tp_lock);
		//pthread_cond_wait(&pTp->tp_cond, &pTp->tp_lock);
		//pthread_mutex_unlock(&pTp->tp_lock);
		
		DEBUG("No more idle thread, a new thread is created.\n");
	}
	return 0;
}

/**
 * member function reality. add new thread into the pool and run immediately.
 * para:
 * 	pTp: thread pool struct instance ponter
 * 	proc_fun:
 * 	job:
 * return:
 * 	pointer of TpThreadInfo
 */
static TpThreadInfo *tp_add_thread(TpThreadPool *pTp, process_job proc_fun, void *arg) {
    static int i=0;
    i++;
    DEBUG("====== i=%d: first came in tp_add_thread\n",i);
	int err;
	TpThreadInfo *new_thread;

	pthread_mutex_lock(&pTp->tp_lock);
	if (pTp->max_th_num <= pTp->cur_th_num){//池已经满了
		pthread_mutex_unlock(&pTp->tp_lock);
		return NULL;
	}

	//malloc new thread info struct
	new_thread = pTp->thread_info + pTp->cur_th_num; 
	pTp->cur_th_num++;
	pthread_mutex_unlock(&pTp->tp_lock);

	new_thread->tp_pool = pTp;
	//init new thread's cond & mutex
	pthread_cond_init(&new_thread->thread_cond, NULL);
	pthread_mutex_init(&new_thread->thread_lock, NULL);

	//init status is busy, only new process job will call this function
	new_thread->is_busy = TRUE;
	new_thread->proc_fun = proc_fun;
	new_thread->arg = arg;

	err = pthread_create(&new_thread->thread_id, NULL, tp_work_thread, new_thread);
	if (0 != err) {
		perror("tp_add_thread: pthread_create");
		free(new_thread);
		return NULL;
	}
    DEBUG("====== i=%d: tp_add_thread: pthread_create thread_id=%u\n",i,new_thread->thread_id);
	return new_thread;
}
/*add by mo jianwei*/
int add_one_thread(TpThreadPool *pTp){
    TpThreadInfo *pThi;
    pthread_mutex_lock(&pTp->tp_lock);
	if (pTp->max_th_num <= pTp->cur_th_num){//池已经满了
		pthread_mutex_unlock(&pTp->tp_lock);
		return 0;
	}
        pThi = pTp->thread_info + pTp->cur_th_num;
        pTp->cur_th_num++;
        pthread_mutex_unlock(&pTp->tp_lock);
		pThi->tp_pool = pTp;
		pThi->is_busy = FALSE;
		pthread_cond_init(&pThi->thread_cond, NULL);
		pthread_mutex_init(&pThi->thread_lock, NULL);
		pThi->proc_fun = NULL;
		pThi->arg = NULL;
		ts_queue_enq_data(pTp->idle_q, pThi);

		err = pthread_create(&pThi->thread_id, NULL, tp_work_thread, pThi);
		if (0 != err) {
			perror("tp_init: create work thread failed.");
			ts_queue_destroy(pTp->idle_q);//ts_queue_create->ts_queue_init :pthread_mutex_init
			return -1;                    //ts_queue_destroy:pthread_mutex_destroy销除锁
		}
		return 1;//添加成功
}
/*end by mo*/
/**
 * member function reality. delete idle thread in the pool.
 * only delete last idle thread in the pool.
 * para:
 * 	pTp: thread pool struct instance ponter
 * return:
 * 	true: successful; false: failed
 */
int tp_delete_thread(TpThreadPool *pTp) {//删除的头节点,把最后节点替上头节点
    DEBUG("======: first came in tp_delete_thread\n");
	unsigned idx;
	TpThreadInfo *pThi;
	TpThreadInfo tT;

	//current thread num can't < min thread num
	if (pTp->cur_th_num <= pTp->min_th_num)
		return -1;
	//all threads are busy
	pThi = (TpThreadInfo *) ts_queue_deq_data(pTp->idle_q);//返回需要删除的头节点,也就删除头节点的线程
	if(!pThi)
		return -1;
	
	//after deleting idle thread, current thread num -1
	pthread_mutex_lock(&pTp->tp_lock);
	pTp->cur_th_num--;
	/** swap this thread to the end, and free it! **/
	memcpy(&tT, pThi, sizeof(TpThreadInfo));
	memcpy(pThi, pTp->thread_info + pTp->cur_th_num, sizeof(TpThreadInfo));//跟线程池最后那个线程互换
                                                                           //注:cur_th_num已经先自减了
	memcpy(pTp->thread_info + pTp->cur_th_num, &tT, sizeof(TpThreadInfo));
	pthread_mutex_unlock(&pTp->tp_lock);

	//kill the idle thread and free info struct
	kill((pid_t)tT.thread_id, SIGKILL);//向线程发送SIGKILL 信号
    //pthread_cancel((pid_t)tT.thread_id);//add by mojianwei
    DEBUG("删除线程tT.thread_id=%u\n",tT.thread_id);
	pthread_mutex_destroy(&tT.thread_lock);
	pthread_cond_destroy(&tT.thread_cond);

	return 0;
}

/**
 * internal interface. real work thread.
 * @params:
 * 	arg: args for this method
 * @return:
 *	none
 */
static void *tp_work_thread(void *arg) {//arg 线程信息TpThreadInfo指针
	TpThreadInfo *pTinfo = (TpThreadInfo *) arg;
	TpThreadPool *pTp = pTinfo->tp_pool;

	//wake up waiting thread, notify it I am ready
	pthread_cond_signal(&pTp->tp_cond);
	while (!(pTp->stop_flag)) {//什么时候stop_flag 变为TRUE,跳出循环,还是让其一直循环:tp_close
		//process
		if(pTinfo->proc_fun){
			DEBUG("thread %u is running\n", pTinfo->thread_id);
			pTinfo->is_busy = TRUE;//自己加
			pTinfo->proc_fun(pTinfo->arg);//处理函数的参数
			//thread state shoulde be set idle after work
			pTinfo->is_busy = FALSE;
			pTinfo->proc_fun = NULL;
			//I am idle now
			ts_queue_enq_data(pTp->idle_q, pTinfo);//处理完后加入空闲队列尾部
		}
		//此处是等待工作,上面是处理	
		//wait cond for processing real job.
		DEBUG("thread %u is waiting for a job\n", pTinfo->thread_id);
		pthread_mutex_lock(&pTinfo->thread_lock);//是针对该线程的锁定,与其他线程无关
        /*下面是我自己加的,即锁定后,进入pthread_cond_wait前判断stop_flag值*/
		if(pTinfo->tp_pool->stop_flag){
			DEBUG("thread %u stop\n", pTinfo->thread_id);
            pthread_mutex_unlock(&pTinfo->thread_lock);
			break;
        }
		pthread_cond_wait(&pTinfo->thread_cond, &pTinfo->thread_lock);//此处是一直等待么,直到被唤醒??
		pthread_mutex_unlock(&pTinfo->thread_lock);
		DEBUG("thread %u end a job\n", pTinfo->thread_id);
       
		if(pTinfo->tp_pool->stop_flag){
//在stop_flag=TRUE之前已经执行过了判断了
//而在此线程执行到pthread_cond_wait之前,即还在处理proc_fun时,
//tp_close:pthread_cond_singal已经发送唤醒信号来终止此线程时了,但因为现在不在wait()状态此,信号不起作用
//,proc_fun处理完后此线程执行到pthread_cond_wait(),就一直等待在那,即没有被终止
//所以在pthread_cond_wait之前、proc_fun之后再判断一次stop_flag   如:355行的代码
//也可以不用加,可以把pthread_cond_singal改为pthread_cancel,当线程运行到wait()退出点时自动退出                                    
			DEBUG("thread %u stop\n", pTinfo->thread_id);
			break;
		}
	}
	DEBUG("Job done, thread %u is idle now.\n", pTinfo->thread_id);
}

/**
 * member function reality. get current thread pool status:idle, normal, busy, .etc.
 * para:
 * 	pTp: thread pool struct instance ponter
 * return:
 * 	0: idle; 1: normal or busy(don't process)
 */
int tp_get_tp_status(TpThreadPool *pTp) {
    DEBUG("====tp_get_tp_status: first in the tp_get_tp_status.\n");
	float busy_num = 0.0;
	int i;

	//get busy thread number
	busy_num = pTp->cur_th_num - ts_queue_count(pTp->idle_q);

	DEBUG("Current thread pool status, current num: %u, busy num: %u, idle num: %u\n", pTp->cur_th_num, (unsigned)busy_num, ts_queue_count(pTp->idle_q));
	if(busy_num / (pTp->cur_th_num) < pTp->busy_threshold){
        DEBUG("====tp_get_tp_status: 空闲的线程太多,需要退出.busy_num=%f,pTp->cur_th_num=%d\n",busy_num,pTp->cur_th_num);
		return 0;//idle status 表示空闲的线程太多,需要退出
    }
	else 
		return 1;//busy or normal status
  /*if(busy_num / (pTp->cur_th_num) >= 0.8) //add by mo jianwei
       return 2;//busy
    else
       return 1;//normal status
    */
}

/**
 * internal interface. manage thread pool to delete idle thread.
 * para:
 * 	pthread: thread pool struct ponter
 * return:
 */
//除了删除线程外 ,应该也要根据线程忙碌状态来事先创建线程,不需要等到发现线程不够用了再创建
static void *tp_manage_thread(void *arg) {
	TpThreadPool *pTp = (TpThreadPool*) arg;//main thread pool struct instance
    int s;
	//1?
	sleep(pTp->manage_interval);//30

	do {
        s=tp_get_tp_status(pTp);
		if ( s == 0) {//表示空闲的线程太多,需要退出
			do {
                DEBUG("====tp_manage_thread: tp_delete_thread.\n");
				if (!tp_delete_thread(pTp))//返回0表示成功结束空闲队列里的一个线程
					break;                 //就退出当前循环
			} while (TRUE);
		}//end for if
        //add by mo jianwei
        /*
        if(s == 2) { //表示忙碌状态,事先创建一个线程
            add_one_thread(pTp);
        }          
        */
		                
        DEBUG("====tp_manage_thread: break and sleep=====.\n");
		sleep(pTp->manage_interval);//每30秒才检测一次并每次只删除一个线程,cur_th_num--会递减
	} while (!pTp->stop_flag);// tp_run-->tp_close->pTp->stop_flag = TRUE;退出循环
	return NULL;
}

float tp_get_busy_threshold(TpThreadPool *pTp){
	return pTp->busy_threshold;
}

int tp_set_busy_threshold(TpThreadPool *pTp, float bt){
	if(bt <= 1.0 && bt > 0.)
		pTp->busy_threshold = bt;
}

unsigned tp_get_manage_interval(TpThreadPool *pTp){
	return pTp->manage_interval;
}

int tp_set_manage_interval(TpThreadPool *pTp, unsigned mi){
	pTp->manage_interval = mi;
}