package beanstalk

import (
	"context"
	"time"
)

// TubeSet represents a set of tubes on the server connected to by Conn.
// Name names the tubes represented.
type Retubeset struct {
	Conn *Reconn
	Name map[string]bool
	ts   *TubeSet
}

// NewTubeSet returns a new TubeSet representing the given names.
func NewRetubeset(c *Reconn, name ...string) *Retubeset {
	t := &Retubeset{
		Conn: c,
		ts:   NewTubeSet(c.Conn, name...),
	}
	t.Name = t.ts.Name
	return t
}

var (
	canceled context.Context
)

func init() {
	var cancel context.CancelFunc
	canceled, cancel = context.WithCancel(context.TODO())
	cancel()
}

// Reserve reserves and returns a job from one of the tubes in t. If no
// job is available before time timeout has passed, Reserve returns a
// ConnError recording ErrTimeout.
//
// Typically, a client will reserve a job, perform some work, then delete
// the job with Conn.Delete.
func (t *Retubeset) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	if err = t.Conn.tryConn(canceled); err != nil {
		return
	}
	t.ts.Conn = t.Conn.Conn
	id, body, err = t.ts.Reserve(timeout)
	return
}
