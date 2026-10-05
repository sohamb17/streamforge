package console

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Chaos actions available from the dashboard. Each one is a real fault on
// the real system (process kill, process freeze, dropped Raft traffic) and
// heals itself after a fixed time, so a public visitor can never leave the
// demo broken. Only one action runs at a time, with a cooldown after it.
var chaosCatalog = map[string]struct {
	heal time.Duration
	desc string
}{
	"kill-leader":   {20 * time.Second, "SIGKILL the Raft leader; restart it 20 s later"},
	"kill-follower": {20 * time.Second, "SIGKILL a follower; restart it 20 s later"},
	"partition":     {20 * time.Second, "cut the leader and one follower off from the other three for 20 s"},
	"pause-leader":  {10 * time.Second, "freeze the leader process (like a long GC pause) for 10 s"},
	"kill-worker":   {10 * time.Second, "SIGKILL stream worker 1; restart it 10 s later"},
	"redeliver":     {3 * time.Second, "make every worker re-send its last batch and re-read 2,000 Kafka offsets"},
	"stop-redis":    {15 * time.Second, "kill the Redis cache for 15 s"},
}

// ErrBusy is returned when another action is running or cooling down.
var ErrBusy = errors.New("another fault is running or cooling down; try again in a few seconds")

type chaosState struct {
	l             *Live
	mu            sync.Mutex
	active        *ChaosAction
	cooldownUntil time.Time
	healFn        func(context.Context)
	timer         *time.Timer
	perIP         map[string][]time.Time

	waitSince      time.Time
	oldLeader      uint64
	lastFailoverMs int64
}

func newChaos(l *Live) *chaosState { return &chaosState{l: l, perIP: map[string][]time.Time{}} }

func (c *chaosState) view() Chaos {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := Chaos{Enabled: c.l.cfg.Chaos, CooldownUntil: c.cooldownUntil.UnixMilli(), LastFailoverMs: c.lastFailoverMs}
	if c.active != nil {
		a := *c.active
		v.Active = &a
	}
	return v
}

// observeLeader measures failover time after leader faults.
func (c *chaosState) observeLeader(leader uint64, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waitSince.IsZero() || leader == 0 || leader == c.oldLeader {
		return
	}
	ms := now.Sub(c.waitSince).Milliseconds()
	c.lastFailoverMs = ms
	c.waitSince = time.Time{}
	go c.l.addLog("raft", "new leader: node %d, %d ms after the fault (observed by polling every 250 ms)", leader, ms)
}

func (c *chaosState) allowIP(ip string) bool {
	now := time.Now()
	var keep []time.Time
	for _, t := range c.perIP[ip] {
		if now.Sub(t) < time.Minute {
			keep = append(keep, t)
		}
	}
	if len(keep) >= 6 {
		c.perIP[ip] = keep
		return false
	}
	c.perIP[ip] = append(keep, now)
	return true
}

// Do runs an action. Heal ("heal") ends the active one early.
func (c *chaosState) Do(ctx context.Context, action, ip string) error {
	if !c.l.cfg.Chaos {
		return errors.New("chaos is disabled on this deployment")
	}
	if action == "heal" {
		c.healNow()
		return nil
	}
	spec, ok := chaosCatalog[action]
	if !ok {
		return fmt.Errorf("unknown action %q", action)
	}
	c.mu.Lock()
	if c.active != nil || time.Now().Before(c.cooldownUntil) {
		c.mu.Unlock()
		return ErrBusy
	}
	if !c.allowIP(ip) {
		c.mu.Unlock()
		return errors.New("rate limit: at most 6 faults per minute per visitor")
	}
	// Reserve the slot before doing slow work.
	now := time.Now()
	c.active = &ChaosAction{Name: action, Started: now.UnixMilli(), HealAt: now.Add(spec.heal).UnixMilli()}
	c.mu.Unlock()

	target, heal, err := c.inject(ctx, action)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.active = nil
		return err
	}
	c.active.Target = target
	c.healFn = heal
	c.timer = time.AfterFunc(spec.heal, c.healNow)
	go c.l.addLog("chaos", "%s: %s (%s)", action, target, spec.desc)
	return nil
}

func (c *chaosState) healNow() {
	c.mu.Lock()
	h := c.healFn
	a := c.active
	c.healFn, c.active = nil, nil
	if c.timer != nil {
		c.timer.Stop()
	}
	if a != nil {
		c.cooldownUntil = time.Now().Add(8 * time.Second)
	}
	c.mu.Unlock()
	if h != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		h(ctx)
	}
	if a != nil {
		c.l.addLog("chaos", "healed: %s", a.Name)
	}
}

func (c *chaosState) containerID(ctx context.Context, service string) (string, error) {
	cs, err := c.l.docker.List(ctx, c.l.cfg.Project)
	if err != nil {
		return "", err
	}
	for _, ct := range cs {
		if ct.Service() == service {
			return ct.ID, nil
		}
	}
	return "", fmt.Errorf("no container for service %s", service)
}

func (c *chaosState) leaderAndFollowers() (uint64, []uint64) {
	s := c.l.Snapshot()
	var fs []uint64
	for _, n := range s.Nodes {
		if n.ID != s.Leader && n.State == "up" {
			fs = append(fs, n.ID)
		}
	}
	return s.Leader, fs
}

func (c *chaosState) startLeaderWait(old uint64) {
	c.mu.Lock()
	c.waitSince, c.oldLeader = time.Now(), old
	c.mu.Unlock()
}

func (c *chaosState) inject(ctx context.Context, action string) (string, func(context.Context), error) {
	d := c.l.docker
	switch action {
	case "kill-leader", "pause-leader":
		leader, _ := c.leaderAndFollowers()
		if leader == 0 {
			return "", nil, errors.New("no leader right now")
		}
		id, err := c.containerID(ctx, fmt.Sprintf("storenode%d", leader))
		if err != nil {
			return "", nil, err
		}
		c.startLeaderWait(leader)
		if action == "kill-leader" {
			if err := d.Kill(ctx, id); err != nil {
				return "", nil, err
			}
			return fmt.Sprintf("node %d", leader), func(ctx context.Context) { _ = d.Start(ctx, id) }, nil
		}
		if err := d.Pause(ctx, id); err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("node %d", leader), func(ctx context.Context) { _ = d.Unpause(ctx, id) }, nil
	case "kill-follower":
		_, fs := c.leaderAndFollowers()
		if len(fs) == 0 {
			return "", nil, errors.New("no follower available")
		}
		f := fs[rand.Intn(len(fs))]
		id, err := c.containerID(ctx, fmt.Sprintf("storenode%d", f))
		if err != nil {
			return "", nil, err
		}
		if err := d.Kill(ctx, id); err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("node %d", f), func(ctx context.Context) { _ = d.Start(ctx, id) }, nil
	case "partition":
		leader, fs := c.leaderAndFollowers()
		if leader == 0 || len(fs) < 4 {
			return "", nil, errors.New("partition needs a leader and all four followers up")
		}
		minority := []uint64{leader, fs[0]}
		majority := fs[1:]
		set := func(ctx context.Context, ids []uint64, peers []uint64) error {
			var ps []string
			for _, p := range peers {
				ps = append(ps, fmt.Sprint(p))
			}
			for _, id := range ids {
				addr := c.l.cfg.AdminAddrs[id]
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/admin/block?peers="+strings.Join(ps, ","), nil)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}
				resp.Body.Close()
				c.l.mu.Lock()
				c.l.blocked[id] = append([]uint64(nil), peers...)
				c.l.mu.Unlock()
			}
			return nil
		}
		c.startLeaderWait(leader)
		if err := set(ctx, minority, majority); err != nil {
			return "", nil, err
		}
		if err := set(ctx, majority, minority); err != nil {
			return "", nil, err
		}
		heal := func(ctx context.Context) {
			for id := range c.l.cfg.AdminAddrs {
				_ = set(ctx, []uint64{id}, nil)
			}
		}
		return fmt.Sprintf("{%d,%d} | {%d,%d,%d}", minority[0], minority[1], majority[0], majority[1], majority[2]), heal, nil
	case "kill-worker", "stop-redis":
		svc := "worker1"
		if action == "stop-redis" {
			svc = "redis"
		}
		id, err := c.containerID(ctx, svc)
		if err != nil {
			return "", nil, err
		}
		if err := d.Kill(ctx, id); err != nil {
			return "", nil, err
		}
		return svc, func(ctx context.Context) { _ = d.Start(ctx, id) }, nil
	case "redeliver":
		var done []string
		for _, w := range c.l.cfg.WorkerAdmin {
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+w+"/admin/redeliver?n=2000", nil)
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
				done = append(done, strings.Split(w, ":")[0])
			}
		}
		if len(done) == 0 {
			return "", nil, errors.New("no worker reachable")
		}
		return strings.Join(done, ", "), func(context.Context) {}, nil
	}
	return "", nil, fmt.Errorf("unknown action %q", action)
}
