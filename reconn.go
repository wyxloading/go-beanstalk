package beanstalk

import (
	"context"
	"net"
	"time"
)

func DialTimeoutReconn(network, addr string, timeout time.Duration) (*Reconn, error) {
	conn, err := DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Reconn{
		network: network,
		addr:    addr,
		timeout: timeout,
		Conn:    conn,
	}, nil
}

type Reconn struct {
	*Conn
	network string
	addr    string
	timeout time.Duration
}

func (c *Reconn) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

// Delete deletes the given job.
func (c *Reconn) Delete(ctx context.Context, id uint64) (err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return err
		}
		err = c.Conn.Delete(id)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return err
	}
}

// Release tells the server to perform the following actions:
// set the priority of the given job to pri, remove it from the list of
// jobs reserved by c, wait delay seconds, then place the job in the
// ready queue, which makes it available for reservation by any client.
func (c *Reconn) Release(ctx context.Context, id uint64, pri uint32, delay time.Duration) (err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return err
		}
		err = c.Conn.Release(id, pri, delay)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return err
	}
}

// Bury places the given job in a holding area in the job's tube and
// sets its priority to pri. The job will not be scheduled again until it
// has been kicked; see also the documentation of Kick.
func (c *Reconn) Bury(ctx context.Context, id uint64, pri uint32) (err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return err
		}
		err = c.Conn.Bury(id, pri)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return err
	}
}

// KickJob places the given job to the ready queue of the same tube where it currently belongs
// when the given job id exists and is in a buried or delayed state.
func (c *Reconn) KickJob(ctx context.Context, id uint64) (err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return err
		}
		err = c.Conn.KickJob(id)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return err
	}
}

// Touch resets the reservation timer for the given job.
// It is an error if the job isn't currently reserved by c.
// See the documentation of Reserve for more details.
func (c *Reconn) Touch(ctx context.Context, id uint64) (err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return err
		}
		err = c.Conn.Touch(id)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return err
	}
}

// Peek gets a copy of the specified job from the server.
func (c *Reconn) Peek(ctx context.Context, id uint64) (body []byte, err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return
		}
		body, err = c.Conn.Peek(id)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return
	}
}

// Stats retrieves global statistics from the server.
func (c *Reconn) Stats(ctx context.Context) (res map[string]string, err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return
		}
		res, err = c.Conn.Stats()
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return
	}
}

// StatsJob retrieves statistics about the given job.
func (c *Reconn) StatsJob(ctx context.Context, id uint64) (res map[string]string, err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return
		}
		res, err = c.Conn.StatsJob(id)
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return
	}
}

// ListTubes returns the names of the tubes that currently
// exist on the server.
func (c *Reconn) ListTubes(ctx context.Context) (res []string, err error) {
	for {
		if err = c.tryConn(ctx); err != nil {
			return
		}
		res, err = c.Conn.ListTubes()
		if err == nil {
			return
		}
		if _, ok := err.(netError); ok {
			c.Conn.Close()
			c.Conn = nil
			continue
		}
		return
	}
}

func (c *Reconn) tryConn(ctx context.Context) error {
	if c.Conn != nil {
		return nil
	}
	for {
		if err := c.redial(); err != nil {
			select {
			case <-ctx.Done():
				return err
			default:
			}
		} else {
			return nil
		}
	}
}

func (c *Reconn) redial() error {
	dialer := &net.Dialer{
		Timeout:   c.timeout,
		KeepAlive: DefaultKeepAlivePeriod,
	}
	conn, err := dialer.Dial(c.network, c.addr)
	if err != nil {
		return err
	}
	c.Conn = NewConn(conn)
	return nil
}
