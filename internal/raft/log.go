package raft

import "fmt"

// raftLog is the in-memory log: a compacted prefix described by the latest
// snapshot (snapIndex, snapTerm) followed by entries[0..].
// entries[i].Index == snapIndex + 1 + i.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry

	committed uint64
	applied   uint64 // highest index handed to the driver for applying
	// stable is the highest index the driver has been asked to persist.
	// Entries after it are handed out in the next Ready.
	stable uint64
}

func (l *raftLog) lastIndex() uint64 {
	return l.snapIndex + uint64(len(l.entries))
}

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index i. ok is false if i is
// compacted away (below the snapshot) or beyond the end of the log.
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapIndex {
		return l.snapTerm, true
	}
	if i < l.snapIndex || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.snapIndex-1].Term, true
}

func (l *raftLog) matchTerm(i, t uint64) bool {
	tt, ok := l.term(i)
	return ok && tt == t
}

// slice returns entries in [lo, hi), at most max entries (0 = no limit).
func (l *raftLog) slice(lo, hi uint64, max int) []Entry {
	if lo <= l.snapIndex {
		panic(fmt.Sprintf("raft: slice(%d) below snapshot %d", lo, l.snapIndex))
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	if max > 0 && hi-lo > uint64(max) {
		hi = lo + uint64(max)
	}
	src := l.entries[lo-l.snapIndex-1 : hi-l.snapIndex-1]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// isUpToDate implements the election restriction (section 5.4.1): a voter
// grants its vote only if the candidate's log is at least as up to date.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	my := l.lastTerm()
	return lastTerm > my || (lastTerm == my && lastIndex >= l.lastIndex())
}

// append appends entries that directly follow the current last index.
func (l *raftLog) append(es ...Entry) {
	if len(es) == 0 {
		return
	}
	if es[0].Index != l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: append gap: last %d, got %d", l.lastIndex(), es[0].Index))
	}
	l.entries = append(l.entries, es...)
}

// maybeAppend handles an AppendEntries request whose prev entry matched.
// It skips entries already present, truncates at the first conflict and
// appends the rest. It returns the index of the last new entry.
func (l *raftLog) maybeAppend(prevIndex uint64, es []Entry) uint64 {
	lastNew := prevIndex + uint64(len(es))
	for i, e := range es {
		if e.Index <= l.snapIndex {
			continue // already compacted, hence committed and identical
		}
		if l.matchTerm(e.Index, e.Term) {
			continue
		}
		if e.Index <= l.committed {
			panic(fmt.Sprintf("raft: conflict at committed index %d", e.Index))
		}
		// Truncate everything from the conflict onward and append the rest.
		l.entries = l.entries[:e.Index-l.snapIndex-1]
		if l.stable >= e.Index {
			l.stable = e.Index - 1
		}
		l.append(es[i:]...)
		break
	}
	return lastNew
}

func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		if i > l.lastIndex() {
			panic(fmt.Sprintf("raft: commit %d beyond last index %d", i, l.lastIndex()))
		}
		l.committed = i
	}
}

// compact discards entries up to and including i, recording (i, term(i)).
func (l *raftLog) compact(i uint64) {
	if i <= l.snapIndex {
		return
	}
	if i > l.applied {
		panic(fmt.Sprintf("raft: compact %d beyond applied %d", i, l.applied))
	}
	t, ok := l.term(i)
	if !ok {
		panic(fmt.Sprintf("raft: compact unknown index %d", i))
	}
	l.entries = append([]Entry(nil), l.entries[i-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = i, t
}

// restore replaces the whole log with a snapshot.
func (l *raftLog) restore(s *Snapshot) {
	l.entries = nil
	l.snapIndex, l.snapTerm = s.Index, s.Term
	l.committed = s.Index
	l.applied = s.Index
	l.stable = s.Index
}

func (l *raftLog) unstable() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	return l.slice(l.stable+1, l.lastIndex()+1, 0)
}

func (l *raftLog) nextCommitted(max int) []Entry {
	if l.applied >= l.committed {
		return nil
	}
	return l.slice(l.applied+1, l.committed+1, max)
}
