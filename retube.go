package beanstalk

import (
	"context"
	"time"
)

// Retube represents tube Name on the server connected to by Reconn.
// It has methods for commands that operate on a single tube.
type Retube struct {
	rc   *Reconn
	tube *Tube
}

// NewTube returns a new Tube representing the given name.
func NewRetube(c *Reconn, name string) *Retube {
	return &Retube{
		rc:   c,
		tube: &Tube{Name: name},
	}
}

func (t *Retube) Close() error {
	return t.rc.Close()
}

// Put puts a job into tube t with priority pri and TTR ttr, and returns
// the id of the newly-created job. If delay is nonzero, the server will
// wait the given amount of time after returning to the client and before
// putting the job into the ready queue.
func (t *Retube) Put(ctx context.Context, body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		id, err = t.tube.Put(body, pri, delay, ttr)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}

// PeekReady gets a copy of the job at the front of t's ready queue.
func (t *Retube) PeekReady(ctx context.Context) (id uint64, body []byte, err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		id, body, err = t.tube.PeekReady()
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}

// PeekDelayed gets a copy of the delayed job that is next to be
// put in t's ready queue.
func (t *Retube) PeekDelayed(ctx context.Context) (id uint64, body []byte, err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		id, body, err = t.tube.PeekDelayed()
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}

// PeekBuried gets a copy of the job in the holding area that would
// be kicked next by Kick.
func (t *Retube) PeekBuried(ctx context.Context) (id uint64, body []byte, err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		id, body, err = t.tube.PeekBuried()
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}

// Kick takes up to bound jobs from the holding area and moves them into
// the ready queue, then returns the number of jobs moved. Jobs will be
// taken in the order in which they were last buried.
func (t *Retube) Kick(ctx context.Context, bound int) (n int, err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		n, err = t.tube.Kick(bound)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}

// Stats retrieves statistics about tube t.
func (t *Retube) Stats(ctx context.Context) (res map[string]string, err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		res, err = t.tube.Stats()
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}

// Pause pauses new reservations in t for time d.
func (t *Retube) Pause(ctx context.Context, d time.Duration) (err error) {
	for {
		if err = t.rc.tryConn(ctx); err != nil {
			return
		}
		t.tube.Conn = t.rc.Conn
		err = t.tube.Pause(d)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			t.rc.Conn.Close()
			t.rc.Conn = nil
			continue
		}
		return
	}
}
