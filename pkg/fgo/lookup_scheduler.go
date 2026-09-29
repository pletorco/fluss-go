package fgo

import (
	"context"
	"fmt"
	"time"
)

func (c *Lookuper) runScheduler() {
	scheduler := &lookupScheduler{
		client: c, groups: make(map[lookupGroup][]*lookupTask),
		timer: time.NewTimer(time.Hour),
	}
	if !scheduler.timer.Stop() {
		<-scheduler.timer.C
	}
	for range c.config.MaxInFlightRequests {
		scheduler.workers.Add(1)
		go func() {
			defer scheduler.workers.Done()
			c.runLookupWorker()
		}()
	}
	scheduler.run()
}

func (s *lookupScheduler) run() {
	for {
		select {
		case task := <-s.client.queue:
			if err := task.ctx.Err(); err != nil {
				task.complete(rawLookupResult{err: err})
				continue
			}
			group := lookupGroup{mode: task.mode, bucket: task.bucket}
			s.groups[group] = append(s.groups[group], task)
			if len(s.groups[group]) >= s.client.config.MaxBatchKeys ||
				s.client.config.BatchTimeout == 0 {
				if !s.dispatchGroup(group) {
					s.shutdown()
					return
				}
			} else {
				s.armTimer()
			}
		case <-s.timerC:
			s.timerC = nil
			for group := range s.groups {
				if !s.dispatchGroup(group) {
					s.shutdown()
					return
				}
			}
		case <-s.client.life.Done():
			s.shutdown()
			return
		}
	}
}

func (s *lookupScheduler) armTimer() {
	if s.timerC == nil {
		s.timer.Reset(s.client.config.BatchTimeout)
		s.timerC = s.timer.C
	}
}

func (s *lookupScheduler) stopTimer() {
	if s.timerC != nil && !s.timer.Stop() {
		select {
		case <-s.timer.C:
		default:
		}
	}
	s.timerC = nil
}

func (s *lookupScheduler) dispatchGroup(group lookupGroup) bool {
	tasks := s.groups[group]
	for len(tasks) != 0 {
		count := s.client.config.MaxBatchKeys
		if len(tasks) < count {
			count = len(tasks)
		}
		batchTasks := append([]*lookupTask(nil), tasks[:count]...)
		select {
		case s.client.jobs <- lookupBatch{group: group, tasks: batchTasks}:
			tasks = tasks[count:]
		case <-s.client.life.Done():
			completeLookupTasks(batchTasks, rawLookupResult{err: ErrClosed})
			return false
		}
	}
	delete(s.groups, group)
	return true
}

func (s *lookupScheduler) shutdown() {
	s.stopTimer()
	for _, tasks := range s.groups {
		completeLookupTasks(tasks, rawLookupResult{err: ErrClosed})
	}
	for {
		select {
		case task := <-s.client.queue:
			task.complete(rawLookupResult{err: ErrClosed})
		default:
			close(s.client.jobs)
			s.workers.Wait()
			close(s.client.done)
			return
		}
	}
}

func (c *Lookuper) runLookupWorker() {
	for batch := range c.jobs {
		active := batch.tasks[:0]
		for _, task := range batch.tasks {
			if err := task.ctx.Err(); err != nil {
				task.complete(rawLookupResult{err: err})
			} else {
				active = append(active, task)
			}
		}
		if len(active) == 0 {
			continue
		}
		requestCtx, cancel := context.WithTimeout(c.life, c.config.RequestTimeout)
		if batch.group.mode == pointLookupMode {
			values, err := c.runPointAttempts(requestCtx, batch.group.bucket, active)
			completePointLookupBatch(active, values, err)
		} else {
			values, err := c.runPrefixAttempts(requestCtx, batch.group.bucket, active)
			completePrefixLookupBatch(active, values, err)
		}
		cancel()
	}
}

func (c *Lookuper) runPointAttempts(
	ctx context.Context,
	bucket int32,
	tasks []*lookupTask,
) ([][]byte, error) {
	return retryLookup(ctx, c.config.RetryPolicy, c.observer, c.config.InsertIfNotExists, func() ([][]byte, error) {
		return c.backend.lookup(ctx, lookupRequest{
			path: c.path, bucket: bucket, tableID: c.tableID, partitionID: c.partitionID,
			keys: encodedLookupTasks(tasks), insertIfNotExist: c.config.InsertIfNotExists,
			timeout: c.config.RequestTimeout, acks: c.config.Acks,
		})
	})
}

func (c *Lookuper) runPrefixAttempts(
	ctx context.Context,
	bucket int32,
	tasks []*lookupTask,
) ([][][]byte, error) {
	return retryLookup(ctx, c.config.RetryPolicy, c.observer, false, func() ([][][]byte, error) {
		return c.backend.prefixLookup(
			ctx, c.path, bucket, c.tableID, c.partitionID, encodedLookupTasks(tasks),
		)
	})
}

func retryLookup[T any](
	ctx context.Context,
	policy RetryPolicy,
	observer MetricsObserver,
	mutation bool,
	call func() (T, error),
) (T, error) {
	var zero T
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		value, err := call()
		if err == nil {
			return value, nil
		}
		if mutation || attempt == policy.MaxAttempts || !writerRetryable(err) || ctx.Err() != nil {
			return zero, err
		}
		observeMetric(observer, MetricEvent{
			Kind: MetricRetry, Operation: MetricOperationLookup, Attempt: attempt + 1,
			Failed: true, ErrorClass: metricErrorClass(err),
		})
		delay := time.Duration(0)
		if policy.Backoff != nil {
			delay = policy.Backoff(attempt + 1)
		}
		if err := waitContext(ctx, delay); err != nil {
			return zero, err
		}
	}
	return zero, fmt.Errorf("%w: unreachable lookup retry loop", ErrValidation)
}

func encodedLookupTasks(tasks []*lookupTask) [][]byte {
	encoded := make([][]byte, len(tasks))
	for index, task := range tasks {
		encoded[index] = task.encoded
	}
	return encoded
}

func completeLookupTasks(tasks []*lookupTask, result rawLookupResult) {
	for _, task := range tasks {
		task.complete(result)
	}
}

func completePointLookupBatch(tasks []*lookupTask, values [][]byte, err error) {
	if err != nil {
		completeLookupTasks(tasks, rawLookupResult{err: err})
		return
	}
	if len(values) != len(tasks) {
		completeLookupTasks(tasks, rawLookupResult{
			err: fmt.Errorf(
				"%w: lookup returned %d values for %d keys",
				ErrValidation, len(values), len(tasks),
			),
		})
		return
	}
	for index, task := range tasks {
		task.complete(rawLookupResult{value: values[index]})
	}
}

func completePrefixLookupBatch(tasks []*lookupTask, values [][][]byte, err error) {
	if err != nil {
		completeLookupTasks(tasks, rawLookupResult{err: err})
		return
	}
	if len(values) != len(tasks) {
		completeLookupTasks(tasks, rawLookupResult{
			err: fmt.Errorf(
				"%w: prefix lookup returned %d lists for %d keys",
				ErrValidation, len(values), len(tasks),
			),
		})
		return
	}
	for index, task := range tasks {
		task.complete(rawLookupResult{rows: values[index]})
	}
}
