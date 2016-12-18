package bio

import (
	"sync"

	"github.com/timtadh/data-structures/exc"
)

// Pipeline is a set of goroutines connected by channels, that may throw exceptions
type Pipeline struct {
	done chan struct{}
	wait sync.WaitGroup
	mut  sync.Mutex
	err  exc.Throwable
}

// NewPipeline creates a new pipeline
func NewPipeline() *Pipeline {
	return &Pipeline{
		make(chan struct{}),
		sync.WaitGroup{},
		sync.Mutex{},
		nil,
	}
}

// Done yields a channel that will be closed when goroutines should stop
func (p *Pipeline) Done() <-chan struct{} {
	return p.done
}

// Wait waits for all goroutines to stop
func (p *Pipeline) Wait() error {
	p.Stop()
	p.wait.Wait()
	return p.err
}

// WaitThrow waits for all goroutines to stop, and rethrows any error
func (p *Pipeline) WaitThrow() {
	p.Stop()
	p.wait.Wait()
	if p.err != nil {
		exc.Rethrow(p.err, exc.Errorf("pipeline failure"))
	}
}

// Stop requests that all the goroutines stop
func (p *Pipeline) Stop() {
	p.mut.Lock()
	defer p.mut.Unlock()
	if p.err == nil {
		close(p.done)
	}
}

// Go spawns a new goroutine
func (p *Pipeline) Go(f func()) {
	p.wait.Add(1)
	go func() {
		defer p.wait.Done()
		exc.Try(f).Catch(&exc.Exception{}, func(e exc.Throwable) {
			p.mut.Lock()
			defer p.mut.Unlock()
			if p.err == nil {
				p.err = e
				close(p.done)
			}
		}).Error()
	}()
}
