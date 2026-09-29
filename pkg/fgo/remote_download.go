package fgo

import (
	"context"
	"fmt"
)

type remoteDownloadJob struct {
	size int64
	run  func(context.Context) error
}

type remoteDownloadResult struct {
	size int64
	err  error
}

type remoteDownloadScheduler struct {
	ctx         context.Context
	runCtx      context.Context
	cancel      context.CancelFunc
	config      RemoteFileReadConfig
	jobs        []remoteDownloadJob
	completed   chan remoteDownloadResult
	contextDone <-chan struct{}
	next        int
	active      int
	activeBytes int64
	firstErr    error
}

func runRemoteDownloads(
	ctx context.Context,
	config RemoteFileReadConfig,
	jobs []remoteDownloadJob,
) error {
	if len(jobs) == 0 {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	scheduler := remoteDownloadScheduler{
		ctx: ctx, runCtx: runCtx, cancel: cancel, config: config, jobs: jobs,
		completed:   make(chan remoteDownloadResult, config.MaxConcurrentReads),
		contextDone: ctx.Done(),
	}
	for scheduler.hasWork() {
		scheduler.startReady()
		if scheduler.active == 0 {
			return scheduler.idleError()
		}
		scheduler.awaitCompletion()
	}
	return scheduler.firstErr
}

func (s *remoteDownloadScheduler) hasWork() bool {
	return s.next < len(s.jobs) || s.active != 0
}

func (s *remoteDownloadScheduler) startReady() {
	for s.firstErr == nil && s.next < len(s.jobs) &&
		s.active < s.config.MaxConcurrentReads &&
		s.jobs[s.next].size <= s.config.MaxConcurrentBytes-s.activeBytes {
		job := s.jobs[s.next]
		s.next++
		s.active++
		s.activeBytes += job.size
		go func() {
			s.completed <- remoteDownloadResult{size: job.size, err: job.run(s.runCtx)}
		}()
	}
}

func (s *remoteDownloadScheduler) idleError() error {
	if s.firstErr != nil {
		return s.firstErr
	}
	return fmt.Errorf(
		"%w: remote object exceeds concurrent byte budget",
		ErrValidation,
	)
}

func (s *remoteDownloadScheduler) awaitCompletion() {
	select {
	case result := <-s.completed:
		s.active--
		s.activeBytes -= result.size
		if result.err != nil && s.firstErr == nil {
			s.firstErr = result.err
			s.cancel()
		}
	case <-s.contextDone:
		if s.firstErr == nil {
			s.firstErr = s.ctx.Err()
			s.cancel()
		}
		s.contextDone = nil
	}
}
