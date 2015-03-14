/*
Package rediss provides a client for Sentinel managed Redis servers

One possible use is as follows:

        package main

        import (
                "fmt"
                "github.com/richard-lyman/rediss"
                "os"
                "time"
        )

        func main() {
                t := 0
                sentinelHostPort := "localhost:26379"
                masterName := "mymaster"
                size := 10
                retryDelay := 5 * time.Second
                resyncDelay := 100 * time.Millisecond
                logEnabled = false
                s := rediss.New(sentinelHostPort, masterName, size, retryDelay, resyncDelay, logEnabled)
                go func() {
                        previousState := s.State
                        fmt.Println(s.State)
                        for {
                                if previousState != s.State {
                                        previousState = s.State
                                        fmt.Println(s.State)
                                }
                                time.Sleep(100 * time.Millisecond)
                        }
                }()
                for i := 0; i < 100000; i++ { // In the middle of this process you could trigger a failover
                        v, err := s.PDo("GET", "a")
                        if err != nil {
                                fmt.Println("Failed in test call to GET a", err)
                                os.Exit(-1)
                        }
                        if v.(string) == "b" {
                                t += 1
                        }
                }
                fmt.Println(t)
        }

*/
package rediss

import (
	"fmt"
	"github.com/richard-lyman/redisb"
	"github.com/richard-lyman/redisn"
	"github.com/richard-lyman/redisp"
	"net"
	"strings"
	"time"
)

type state string

const (
	Creating      state = "Creating"
	Bootstrapping state = "Bootstrapping"
	Resetting     state = "Resetting"
	Healthy       state = "Healthy"
)

func New(hostPort string, masterName string, size int, retryDelay time.Duration, resyncDelay time.Duration, logEnabled bool) *SPool {
	s := &SPool{
		State:      Creating,
		masterName: masterName,
		hps:        []string{hostPort},
		size:       size,
		retryDelay: retryDelay,
		p:          hostPort,
		Up:         true,
		n:          map[string][]redisn.Handler{},
		LogEnabled: logEnabled,
	}
	s.bootstrap()
	s.creator = func() net.Conn {
		c, err := net.Dial("tcp", s.master)
		if err != nil {
			if strings.HasSuffix(err.Error(), "connection refused") {
				s.log("Connection error with master at addr: '%s'", s.master)
				s.reset()
				c, err = net.Dial("tcp", s.master)
				if err != nil {
					panic(fmt.Sprintf("failed to reset: '%s'", err))
				}
			} else {
				panic(err)
			}
		}
		return c
	}
	s.reset()
	s.pubSub()
	return s
}

type SPool struct {
	State       state
	master      string
	masterName  string
	hps         []string
	size        int
	creator     redisp.Creator
	retryDelay  time.Duration
	resyncDelay time.Duration
	p           string
	Up          bool
	pool        *redisn.NPool
	LogEnabled  bool
	n           map[string][]redisn.Handler
}

func (s *SPool) log(msgs ...interface{}) {
	if s.LogEnabled {
		fmt.Println(msgs...)
	}
}

func (s *SPool) bootstrap() {
	s.State = Bootstrapping
	defer func() {
		if err := recover(); err != nil {
			panic(fmt.Sprintf("failed to bootstrap: %s", err))
		}
	}()
	c, err := net.Dial("tcp", s.hps[0])
	if err != nil {
		panic(err)
	}
	defer c.Close()
	tmpr, err := redisb.Do(c, "ROLE")
	if err != nil {
		panic(err)
	}
	r := tmpr.([]interface{})
	if strings.ToUpper(r[0].(string)) != "SENTINEL" {
		panic(fmt.Sprintf("the given host:port, '%s', failed to respond correctly to a ROLE request. The given host:port must identify itself as having the sentinel role", s.hps[0]))
	}
	s.findPreferred()
}

func (s *SPool) findPreferred() {
	if len(s.p) == 0 {
		s.p = s.hps[0]
	}
	c, err := net.Dial("tcp", s.p)
	if err != nil {
		panic(err)
	}
	tmpr, err := redisb.Do(c, "SENTINEL", "sentinels", s.masterName)
	c.Close()
	if err != nil {
		panic(fmt.Sprintf("Unable to get list of sentinels: %s", err))
	}
	tmpa := tmpr.([]interface{})
	for _, tmpv := range tmpa {
		v := tmpv.([]interface{})
		h := v[3].(string)
		p := v[5].(string)
		hp := h + ":" + p
		exists := false
		for _, existing := range s.hps {
			if existing == hp {
				exists = true
				break
			}
		}
		if !exists {
			s.log("Adding sentinel:", hp)
			s.hps = append(s.hps, hp)
		}
	}
	fastest := 1 * time.Second
	for _, fhp := range s.hps {
		start := time.Now()
		c, err := net.DialTimeout("tcp", fhp, 100*time.Millisecond)
		if err != nil {
			continue
		}
		d := time.Since(start)
		c.Close()
		if d < fastest {
			s.p = fhp
			fastest = d
		}
	}
}

func (s *SPool) reset() {
	if s.State == Resetting {
		return
	}
	s.State = Resetting
	if s.pool == nil && len(s.master) > 0 {
		s.pool = redisn.New(redisp.New(s.size, s.creator, s.retryDelay))
	}
	if s.pool != nil {
		s.pool.Empty()
	}
	s.master = ""
	for {
		time.Sleep(s.resyncDelay)
		c, err := net.Dial("tcp", s.p)
		if err != nil {
			s.log("Failed to dial sentinel")
			s.findPreferred()
			continue
		}
		tmpr, err := redisb.Do(c, "SENTINEL", "get-master-addr-by-name", s.masterName)
		if err != nil {
			s.log("error getting master-addr:", err)
			c.Close()
			s.findPreferred()
			continue
		}
		c.Close()
		r := tmpr.([]interface{})
		host := r[0].(string)
		port := r[1].(string)
		if net.ParseIP(host).To4() == nil {
			host = "[" + host + "]"
		}
		maddr := fmt.Sprintf("%s:%s", host, port)
		c, err = net.Dial("tcp", maddr)
		if err != nil {
			s.log("Failed to dial master:", maddr, err)
			continue
		}
		tmpr, err = redisb.Do(c, "ROLE")
		if err != nil {
			s.log("Failed to get maddr ROLE:", err)
			c.Close()
			continue
		}
		c.Close()
		r = tmpr.([]interface{})
		if strings.ToUpper(r[0].(string)) != "MASTER" {
			s.log("maddr ROLE is not MASTER:", r[0].(string))
			continue
		}
		s.log("Master found:", maddr)
		s.master = maddr
		break
	}
	s.pool = redisn.New(redisp.New(s.size, s.creator, s.retryDelay))
	s.pool.Fill()
	s.State = Healthy
	s.resubscribe()
}

func (s *SPool) pubSub() {
	c := s.creator()
	isMasterName := func(msg string) bool {
		msga := strings.SplitN(msg, " ", 3)
		if len(msga) < 3 {
			s.log("Incorrectly formatted Sentinel pubsub message:", msg)
			return false
		}
		return msga[1] == s.masterName
	}
	redisn.NDo(c, "SUBSCRIBE", func(full string, k string, msg string, err error) {
		if err != nil {
			return
		}
		if isMasterName(msg) {
			s.Up = false
		}
	}, "+odown")
	redisn.NDo(c, "SUBSCRIBE", func(full string, k string, msg string, err error) {
		if err != nil {
			return
		}
		if isMasterName(msg) {
			s.Up = true
		}
	}, "-odown")
	redisn.NDo(c, "SUBSCRIBE", func(full string, k string, msg string, err error) {
		if err != nil {
			return
		}
		if isMasterName(msg) {
			s.Up = true
		}
	}, "switch-master")
}

func (s *SPool) Do(c net.Conn, args ...string) (interface{}, error) {
	return redisb.Do(s.Get(), args...)
}

func (s *SPool) DoN(c net.Conn, args ...string) (interface{}, error) {
	return redisb.DoN(s.Get(), args...)
}

func (s *SPool) Out(c net.Conn, args ...string) {
	redisb.Out(s.Get(), args...)
}

func (s *SPool) Get() net.Conn {
	return s.pool.Get()
}

func (s *SPool) Put(c net.Conn) {
	if s.Up {
		s.pool.Put(c)
	} else {
		s.pool.Bad(c)
	}
}

func (s *SPool) Bad(c net.Conn) {
	s.pool.Bad(c)
}

func (s *SPool) PDo(args ...string) (interface{}, error) {
	return s.pool.PDo(args...)
}

func (s *SPool) NDo(command string, handler redisn.Handler, keys ...string) error {
	err := s.pool.NDo(command, handler, keys...)
	if err == nil {
		for _, k := range keys {
			ck := command + ":" + k
			_, exists := s.n[ck]
			if !exists {
				s.n[ck] = []redisn.Handler{}
			}
			s.n[ck] = append(s.n[ck], handler)
		}
	}
	return err
}

func (s *SPool) resubscribe() {
	for ck, hs := range s.n {
		tmp := strings.SplitN(ck, ":", 2)
		c := tmp[0]
		k := tmp[1]
		for _, h := range hs {
			s.NDo(c, h, k) // Ignoring errors - there's no useful caller to handle any
		}
	}
}

func (s *SPool) NUnDo(command string, keys ...string) error {
	err := s.pool.NUnDo(command, keys...)
	for _, k := range keys {
		ck := command + ":" + k
		delete(s.n, ck)
	}
	return err
}
