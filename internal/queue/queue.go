package queue

import (
	"container/heap"
	"sync"
	"time"
)

type JobType int

const (
	JobDownload JobType = iota + 1
	JobConvert
)

type Job struct {
	ID         string
	Type       JobType
	SessionID  string
	Quality    string
	StartTime  string
	EndTime    string
	EnqueuedAt time.Time
	Priority   int
}

type priorityJob struct {
	job   Job
	index int
}

type jobPQ []*priorityJob

func (pq jobPQ) Len() int { return len(pq) }
func (pq jobPQ) Less(i, j int) bool { return pq[i].job.Priority > pq[j].job.Priority }
func (pq jobPQ) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i]; pq[i].index = i; pq[j].index = j }
func (pq *jobPQ) Push(x interface{}) { *pq = append(*pq, x.(*priorityJob)) }
func (pq *jobPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type Queue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	pq       jobPQ
	capacity int
}

func NewQueue(capacity int) *Queue {
	q := &Queue{capacity: capacity}
	q.notEmpty = sync.NewCond(&q.mu)
    heap.Init(&q.pq)
	return q
}

func (q *Queue) Len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.pq) }

func (q *Queue) Enqueue(j Job) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pq) >= q.capacity { return false }
	heap.Push(&q.pq, &priorityJob{job: j})
	q.notEmpty.Signal()
	return true
}

func (q *Queue) Dequeue() Job {
	q.mu.Lock()
	for len(q.pq) == 0 { q.notEmpty.Wait() }
	item := heap.Pop(&q.pq).(*priorityJob)
	q.mu.Unlock()
	return item.job
}

type WorkerPool struct {
	workers int
	queue   *Queue
	stopCh  chan struct{}
	wg      sync.WaitGroup
	handler func(Job)
}

func NewWorkerPool(workers int, queue *Queue, handler func(Job)) *WorkerPool {
	return &WorkerPool{workers: workers, queue: queue, stopCh: make(chan struct{}), handler: handler}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			for {
				select {
				case <-wp.stopCh:
					return
				default:
					job := wp.queue.Dequeue()
					wp.handler(job)
				}
			}
		}()
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.stopCh)
	wp.wg.Wait()
}
