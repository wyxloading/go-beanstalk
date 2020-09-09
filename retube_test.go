package beanstalk

import (
	"context"
	"testing"
	"time"
)

func TestRetubePut(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("put 0 0 0 3\r\nfoo\r\n", "INSERTED 1\r\n")),
	}
	rt := NewRetube(c, "default")

	id, err := rt.Put(context.TODO(), []byte("foo"), 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal("expected 1, got", id)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetubePeekReady(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("peek-ready\r\n", "FOUND 1 1\r\nx\r\n")),
	}
	rt := NewRetube(c, "default")

	id, body, err := rt.PeekReady(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal("expected 1, got", id)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetubePeekDelayed(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("peek-delayed\r\n", "FOUND 1 1\r\nx\r\n")),
	}
	rt := NewRetube(c, "default")

	id, body, err := rt.PeekDelayed(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal("expected 1, got", id)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetubePeekBuried(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("peek-buried\r\n", "FOUND 1 1\r\nx\r\n")),
	}
	rt := NewRetube(c, "default")

	id, body, err := rt.PeekBuried(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal("expected 1, got", id)
	}
	if len(body) != 1 || body[0] != 'x' {
		t.Fatalf("bad body, expected %#v, got %#v", "x", string(body))
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetubeKick(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("kick 2\r\n", "KICKED 1\r\n")),
	}
	rt := NewRetube(c, "default")

	n, err := rt.Kick(context.TODO(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("expected 1, got", n)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetubeStats(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("stats-tube default\r\n", "OK 10\r\n---\na: ok\n\r\n")),
	}
	rt := NewRetube(c, "default")

	m, err := rt.Stats(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || m["a"] != "ok" {
		t.Fatalf("expected %#v, got %#v", map[string]string{"a": "ok"}, m)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetubePause(t *testing.T) {
	c := &Reconn{
		Conn: NewConn(mock("pause-tube default 5\r\n", "PAUSED\r\n")),
	}
	rt := NewRetube(c, "default")

	err := rt.Pause(context.TODO(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}
