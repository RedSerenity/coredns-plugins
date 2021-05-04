package redis

import (
	"errors"
	"strconv"
	"time"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/mediocregopher/radix/v3"
	"github.com/miekg/dns"
)

// Redis is plugin that looks up responses in a cache and caches replies.
// It has a success and a denial of existence cache.
type Redis struct {
	Next  plugin.Handler
	Zones []string

	pool *radix.Pool
	nttl time.Duration
	pttl time.Duration

	addr string
	idle int
	// Testing.
	now func() time.Time
}

// New returns an new initialized Redis.
func New() *Redis {
	return &Redis{
		Zones: []string{"."},
		addr:  "127.0.0.1:6379",
		idle:  10,
		pool:  &radix.Pool{},
		pttl:  maxTTL,
		nttl:  maxNTTL,
		now:   time.Now,
	}
}

// Add adds the message m under k in Redis.
func Add(p *radix.Pool, key int, m *dns.Msg, duration time.Duration) error {
	// SETEX key duration m
	return p.Do(radix.FlatCmd(nil, "SETEX", strconv.Itoa(key), int(duration.Seconds()), ToString(m)))
}

// Get returns the message under key from Redis.
func Get(p *radix.Pool, key int) (*dns.Msg, error) {
	s := ""
	ttl := 0

	pipe := radix.Pipeline(
		radix.Cmd(&s, "GET", strconv.Itoa(key)),
		radix.Cmd(&ttl, "TTL", strconv.Itoa(key)),
	)

	if err := p.Do(pipe); err != nil {
		return nil, err
	}

	if s == "" {
		return nil, errors.New("not found")
	}

	return FromString(s, ttl), nil
}

func (r *Redis) get(now time.Time, state request.Request, server string) *dns.Msg {
	k := hash(state.Name(), state.QType(), state.Do())

	m, err := Get(r.pool, k)
	if err != nil {
		log.Debugf("Failed to get response from Redis cache: %s", err)
		cacheMisses.WithLabelValues(server).Inc()
		return nil
	}
	log.Debugf("Returning response from Redis cache: %s for %s", m.Question[0].Name, state.Name())
	cacheHits.WithLabelValues(server).Inc()
	return m
}

func (r *Redis) connect() (err error) {
	r.pool, err = radix.NewPool("tcp", r.addr, r.idle)
	return err
}
