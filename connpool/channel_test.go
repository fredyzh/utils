package connpool

import (
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	InitialCap = 5
	MaxIdleCap = 10
	MaximumCap = 100
	network    = "tcp"
	address    = "127.0.0.1:16777"

	factory = func() (interface{}, error) {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		maxSizeOption := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(20*1024*1024),
			grpc.MaxCallSendMsgSize(20*1024*1024))
		opts = append(opts, maxSizeOption)
		return grpc.Dial(address, opts...)
	}
	closeFac = func(v interface{}) error {
		nc := v.(*grpc.ClientConn)
		return nc.Close()
	}
)

func TestNew(t *testing.T) {
	p, err := newChannelPool()
	defer p.Release()
	if err != nil {
		t.Errorf("New error: %s", err)
	}
}


func TestPoolGet(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Release()

	_, err := p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}

	// after one get, current capacity should be lowered by one.
	if p.Len() != (InitialCap - 1) {
		t.Errorf("Get error. Expecting %d, got %d",
			(InitialCap - 1), p.Len())
	}

	// get them all
	var wg sync.WaitGroup
	for i := 0; i < (MaximumCap - 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Get()
			if err != nil {
				t.Errorf("Get error: %s", err)
			}
		}()
	}
	wg.Wait()

	if p.Len() != 0 {
		t.Errorf("Get error. Expecting %d, got %d",
			(InitialCap - 1), p.Len())
	}

	_, err = p.Get()
	if err != ErrMaxActiveConnReached {
		t.Errorf("Get error: %s", err)
	}
}

func TestPoolGetImpl(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Release()

	conn, err := p.Get()
	if err != nil {
		t.Errorf("Get error: %s", err)
	}
	_, ok := conn.(*grpc.ClientConn)
	if !ok {
		t.Errorf("Conn is not of type poolConn")
	}
	p.Put(conn)
}

func TestPoolUsedCapacity(t *testing.T) {
	p, _ := newChannelPool()
	defer p.Release()

	if p.Len() != InitialCap {
		t.Errorf("InitialCap error. Expecting %d, got %d",
			InitialCap, p.Len())
	}
}

func TestPoolClose(t *testing.T) {
	p, _ := newChannelPool()

	// now close it and test all cases we are expecting.
	p.Release()

	c := p.(*channelPool)

	if c.conns != nil {
		t.Errorf("Close error, conns channel should be nil")
	}

	if c.factory != nil {
		t.Errorf("Close error, factory should be nil")
	}

	_, err := p.Get()
	if err == nil {
		t.Errorf("Close error, get conn should return an error")
	}

	if p.Len() != 0 {
		t.Errorf("Close error used capacity. Expecting 0, got %d", p.Len())
	}
}

func TestPoolConcurrent(t *testing.T) {
	p, _ := newChannelPool()
	pipe := make(chan interface{}, 0)

	go func() {
		p.Release()
	}()

	for i := 0; i < MaximumCap; i++ {
		go func() {
			conn, _ := p.Get()

			pipe <- conn
		}()

		go func() {
			conn := <-pipe
			if conn == nil {
				return
			}
			p.Put(conn)
		}()
	}
}

func newChannelPool() (Pool, error) {
	pconf := Config{InitialCap: InitialCap, MaxCap: MaximumCap, Factory: factory, Close: closeFac, IdleTimeout: time.Second * 20,
		MaxIdle: MaxIdleCap}
	return NewChannelPool(&pconf)
}
