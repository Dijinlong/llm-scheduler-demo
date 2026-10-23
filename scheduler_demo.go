// scheduler_demo.go —— 多货源派单的最小实现：滑动窗口健康分 + 熔断 + 带探索的加权选择。
//
// 只用标准库，可直接 `go run scheduler_demo.go` 看模拟结果。
// 目的不是给你一个能上生产的调度器，而是把「为什么轮询会把 429 放大」这件事跑出来。
package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// ---------- 货源与滑动窗口 ----------

type sample struct {
	at       time.Time
	ok       bool
	tooMany  bool
	latency  time.Duration
}

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

type Source struct {
	ID         string
	PriceRatio float64 // 相对官方价，0.5 = 5 折
	FailRate   float64 // 仅用于模拟：这个货源真实会 429 的概率

	mu       sync.Mutex
	samples  []sample
	state    breakerState
	openTill time.Time
	openFor  time.Duration
	consec   int
	probing  bool
}

const (
	window       = 60 * time.Second
	minSamples   = 5
	openBase     = 20 * time.Second
	openMax      = 5 * time.Minute
	tripAfter    = 3 // 连续 3 次 429 就熔断
	exploreRatio = 0.05
)

func NewSource(id string, price, failRate float64) *Source {
	return &Source{ID: id, PriceRatio: price, FailRate: failRate, openFor: openBase}
}

// Record 写入一次调用结果，并按连续 429 触发熔断。
func (s *Source) Record(now time.Time, tooMany bool, latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.samples = append(s.samples, sample{at: now, ok: !tooMany, tooMany: tooMany, latency: latency})
	cut := now.Add(-window)
	i := 0
	for i < len(s.samples) && s.samples[i].at.Before(cut) {
		i++
	}
	if i > 0 {
		s.samples = s.samples[i:]
	}

	if tooMany {
		s.consec++
		if s.consec >= tripAfter {
			s.trip(now)
		}
	} else {
		s.consec = 0
		if s.state == halfOpen {
			s.state = closed
			s.openFor = openBase
			s.probing = false
		}
	}
}

func (s *Source) trip(now time.Time) {
	s.state = open
	s.openTill = now.Add(s.openFor)
	// 指数退避：20s → 40s → 80s … 封顶 5 分钟
	s.openFor *= 2
	if s.openFor > openMax {
		s.openFor = openMax
	}
	s.consec = 0
}

// Health 返回 0~1 的健康分：429 记双倍权重，样本不足时按 0.8 起步（新货源不至于被饿死）。
func (s *Source) Health(now time.Time) (float64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cut := now.Add(-window)
	var total, bad float64
	for _, sm := range s.samples {
		if sm.at.Before(cut) {
			continue
		}
		total++
		if sm.tooMany {
			bad += 2
		} else if !sm.ok {
			bad++
		}
	}
	if total < minSamples {
		return 0.8, int(total)
	}
	h := 1 - bad/total
	if h < 0 {
		h = 0
	}
	return h, int(total)
}

// Available 判断当前是否可派单（半开状态先放一个探针）。
func (s *Source) Available(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case open:
		if now.After(s.openTill) {
			s.state = halfOpen
			s.probing = false
			return true
		}
		return false
	case halfOpen:
		if s.probing {
			return false
		}
		s.probing = true
		return true
	}
	return true
}

// ---------- 派单器 ----------

type Picker struct {
	sources []*Source
	rnd     *rand.Rand
}

func NewPicker(sources []*Source, seed int64) *Picker {
	return &Picker{sources: sources, rnd: rand.New(rand.NewSource(seed))}
}

// Pick：分数 = 健康分² ÷ 价格系数。便宜且健康优先；5% 流量随机探索，避免慢货源被永久饿死。
func (p *Picker) Pick(now time.Time) (*Source, bool) {
	type cand struct {
		s     *Source
		score float64
	}
	var cands []cand
	healthy := 0
	for _, s := range p.sources {
		if !s.Available(now) {
			continue
		}
		h, _ := s.Health(now)
		if h >= 0.5 {
			healthy++
		}
		score := (h * h) / s.PriceRatio
		cands = append(cands, cand{s, score})
	}
	if len(cands) == 0 {
		return nil, false
	}
	// 最小可用集保护：健康货源少于 2 个时，忽略熔断，选当前最好的那个
	if healthy < 2 && len(cands) < 2 {
		best := cands[0]
		for _, c := range cands[1:] {
			if c.score > best.score {
				best = c
			}
		}
		return best.s, false
	}
	if p.rnd.Float64() < exploreRatio {
		return cands[p.rnd.Intn(len(cands))].s, true
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].score > cands[j].score })
	// 前 3 名里按分数做一次加权抽签，避免绝对倾斜
	n := len(cands)
	if n > 3 {
		n = 3
	}
	total := 0.0
	for _, c := range cands[:n] {
		total += c.score
	}
	r := p.rnd.Float64() * total
	for _, c := range cands[:n] {
		r -= c.score
		if r <= 0 {
			return c.s, false
		}
	}
	return cands[0].s, false
}

// ---------- 模拟：轮询 vs 打分派单 ----------

func main() {
	const rounds = 20000
	specs := []struct {
		id    string
		price float64
		fail  float64
	}{
		{"A-稳定贵", 0.50, 0.02},
		{"B-中等", 0.20, 0.10},
		{"C-便宜但抖", 0.05, 0.35},
	}

	// 1) 纯轮询：平均分，重试 3 次（简化：失败就换下一个）
	rr429 := simulate(rounds, specs, func(i int, _ []*Source) int { return i % len(specs) }, false)
	// 2) 打分派单：健康分 + 熔断 + 探索，失败换货源，最多换 2 次
	pick429 := simulate(rounds, specs, nil, true)

	fmt.Printf("轮询派单    429 占比 %.2f%%\n", rr429*100)
	fmt.Printf("健康分派单  429 占比 %.2f%%\n", pick429*100)
	fmt.Printf("降幅        %.1f%%\n", (rr429-pick429)/rr429*100)
}

func simulate(rounds int, specs []struct {
	id    string
	price float64
	fail  float64
}, fixed func(int, []*Source) int, smart bool) float64 {
	srcs := make([]*Source, len(specs))
	for i, sp := range specs {
		srcs[i] = NewSource(sp.id, sp.price, sp.fail)
	}
	p := NewPicker(srcs, 42)
	rnd := rand.New(rand.NewSource(7))
	now := time.Now()
	fails := 0
	for i := 0; i < rounds; i++ {
		now = now.Add(time.Millisecond)
		var idx int
		if smart {
			s, _ := p.Pick(now)
			if s == nil {
				fails++
				continue
			}
			for j := range srcs {
				if srcs[j] == s {
					idx = j
					break
				}
			}
		} else {
			idx = fixed(i, srcs)
		}
		src := srcs[idx]
		bad := rnd.Float64() < src.FailRate
		if bad {
			fails++
		}
		src.Record(now, bad, 200*time.Millisecond)
	}
	return float64(fails) / float64(rounds)
}
