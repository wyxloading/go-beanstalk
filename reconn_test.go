package beanstalk

import (
	"context"
	"testing"
)

func TestReconn_DeleteMissing(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("delete 1\r\n", "NOT_FOUND\r\n")),
	}

	err := c.Delete(context.TODO(), 1)
	if e, ok := err.(ConnError); !ok || e.Err != ErrNotFound {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}
