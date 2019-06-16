/*
 * =====================================================================================
 *
 *       Filename:  tsqueue.c
 *
 *    Description:  it's a thread safe queue
 *
 *        Version:  1.0
 *        Created:  05/08/2012 09:53:42 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  boyce
 *   Organization:  gw
 *
 * =====================================================================================
 */
 #include <stdio.h>
 #include <stdlib.h>
 #include <pthread.h>
 #include "tsqueue.h"

#define ITEMS_PER_ALLOC 32 //队列的节点数,当节点数不足以保存已有的线程数,每次增加的节点数

TSQueue *ts_queue_create(){
	TSQueue *cq = (TSQueue *) malloc(sizeof(TSQueue));
	ts_queue_init(cq);
	return cq;
}

void ts_queue_destroy(TSQueue *cq){
	if(!cq)
		return;
	pthread_mutex_destroy(&cq->lock);
	pthread_mutex_destroy(&cq->cqi_freelist_lock);
	free(cq);
}

void ts_queue_init(TSQueue *cq){
	if(!cq)
		return;
	pthread_mutex_init(&cq->lock, NULL);
	cq->head = NULL;
	cq->tail = NULL;
	cq->cqi_freelist = NULL;
	pthread_mutex_init(&cq->cqi_freelist_lock, NULL);
	cq->count = 0;
}

//只有分配或增加节点空间,没有删除节点空间??要看cqi_freelist,即空闲线程总数与总节点数
//节点空间的大小是曾经空闲线程最大时 差不多,如果全部线程空闲过
TSQItem *ts_queue_item_new(TSQueue *cq) {//队列开辟32个节点,节点data成员指向线程
	TSQItem *item = NULL;
	if(!cq)
		return NULL;
	pthread_mutex_lock(&cq->cqi_freelist_lock);
	if (cq->cqi_freelist) {//再次调用本函数,cqi_freelist !=NULL条件成立
        fprintf(stderr,"ts_queue_item_new: cq->cqi_freelist\n");
		item = cq->cqi_freelist;//cqi_freelist指向的是第二个节点,并返回
		cq->cqi_freelist = item->next;//更新cqi_freelist,指向下一个节点
	}
	pthread_mutex_unlock(&cq->cqi_freelist_lock);

	if (NULL == item) {//cqi_freelist为空的情况,比如第一次调用该函数时,或者已经没有空闲节点
        fprintf(stderr,"ts_queue_item_new: NULL == item\n");
		int i;
		item = (TSQItem *) malloc(sizeof(TSQItem) * ITEMS_PER_ALLOC);
		if (NULL == item){
			//perror("error to malloc cq item");
			return NULL;
		}
		for (i = 2; i < ITEMS_PER_ALLOC; i++)//开辟32个节点的链表
			item[i - 1].next = &item[i];

		pthread_mutex_lock(&cq->cqi_freelist_lock);
		item[ITEMS_PER_ALLOC - 1].next = cq->cqi_freelist;
		cq->cqi_freelist = &item[1];//cqi_freelist 指向item第二个,这没被占用的节点
                                    //返回item,是第一个节点
		pthread_mutex_unlock(&cq->cqi_freelist_lock);
	}

	return item;
}

void ts_queue_item_free(TSQueue *cq, TSQItem *item){
    fprintf(stderr,"first in ts_queue_item_free fun\n");
	if(!cq || !item)
		return;
	pthread_mutex_lock(&cq->cqi_freelist_lock);
	item->next = cq->cqi_freelist;
	cq->cqi_freelist = item;
	pthread_mutex_unlock(&cq->cqi_freelist_lock);
}

TSQItem *ts_queue_head(TSQueue *cq){
	TSQItem *item;
	if(!cq)
		return NULL;
	return cq->head;
}

TSQItem *ts_queue_tail(TSQueue *cq){
	TSQItem *item;
	if(!cq)
		return NULL;
	return cq->tail;
}

TSQItem *ts_queue_peek(TSQueue *cq){
	return ts_queue_head(cq);
}

TSQItem *ts_queue_deq(TSQueue *cq){
    fprintf(stderr,"first in ts_queue_deq fun\n");
	TSQItem *item;
	if(!cq)
		return NULL;

	pthread_mutex_lock(&cq->lock);
	item = cq->head;
	if(NULL != item){
		cq->head = item->next;//空闲队列里从头节点开始删
		if(NULL == cq->head)//head已经到了队列的最后的空闲节点
			cq->tail = NULL;//也把tail置NULL，
		cq->count--;
	}
	pthread_mutex_unlock(&cq->lock);

	return item;
}

//调用线程时tp_process_job,或只想删除空闲线程tp_delete_thread(空闲线程太多)
//删除的节点变成可占用的空闲节点freelist,没有把节点空间删除
void *ts_queue_deq_data(TSQueue *cq){//先从空闲队列里删除此线程
                                     //一般是调用头节点的线程
    fprintf(stderr,"first in ts_queue_deq_data fun\n");
    void *data;                      
	TSQItem *item;
	if(!cq){
        fprintf(stderr,"ts_queue_deq_data: !cq \n");
		return NULL;
    }
	item = ts_queue_deq(cq);//找到头节点,其他并没有真正删除节点,只是把
       //要删除的节点变成freelist,以后要增加空闲线程时,把freelist节点内容覆盖
	if(!item){//队列里没有空闲线程可供调用
		fprintf(stderr,"ts_queue_deq_data: !item\n");
		return NULL;
	}

	data = item->data;//必须在此节点变成freelist之前获得成员data,即返回指针指向的线程池中的某线程
	ts_queue_item_free(cq, item);//把要删除的节点变成freelist,
	return data;//返回线程指针,
}

void ts_queue_enq(TSQueue *cq, TSQItem *item) {
	if(!cq || !item)
		return;
	item->next = NULL;//空闲队列的最后一个节点指向NULL,item是被加到tail后面的
	pthread_mutex_lock(&cq->lock);//对队列的操作,要先锁定
	if (NULL == cq->tail)//第一次成立,此item节点就是头或者head和tail已经到最后个
		cq->head = item;//将head重置到刚刚加入到空闲队列的节点,也就是保存空闲线程的第一个节点
	else
		cq->tail->next = item;//
	cq->tail = item;
	cq->count++;
	pthread_mutex_unlock(&cq->lock);
}
//把队列节点分成三部分,1.head-tail空闲线程可被调用,判断有没有空闲线程
//,freelist指添加空闲线程的节点
//2.被调用时或删除线程时(对象是head节点),item=head,head指向下一个,freelist=item
//3.freelist空闲线程加入的位置节点,tail=freelist
//
//初始化时或处理完事件后,把空闲的线程加入到空闲队列cq
int ts_queue_enq_data(TSQueue *cq, void *data){//data指针指向线程TpThreadInfo
    fprintf(stderr,"first in ts_queue_enq_data fun\n");
	TSQItem *item;
	if(!cq || !data)
		return -1;
	item = ts_queue_item_new(cq);//开辟节点或获取没有被占用的节点cq->cqi_freelist
	if(!item){
		//perror("ts_queue_push_data");
		return -1;
	}
	item->data = data;
	ts_queue_enq(cq, item);//把线程加入到链表节点尾部,head不变,tail=item
	return 0;
}

unsigned ts_queue_count(TSQueue *cq){
	return cq->count;
}

BOOL ts_queue_is_empty(TSQueue *cq){
	return cq->count? TRUE : FALSE;
}
