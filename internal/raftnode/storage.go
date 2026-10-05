package raftnode

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/sohamb17/streamforge/internal/raft"
)

// DiskStorage persists one node's Raft state in a directory:
//
//	wal       append-only records: hard state, log entries, truncations
//	snapshot  the latest state machine snapshot (index, term, data)
//
// Every record is framed as [len u32][crc32c u32][payload] so a torn write
// at the tail after a crash is detected and cut off on recovery. Files are
// replaced atomically (write temp, fsync, rename, fsync dir).
type DiskStorage struct {
	dir   string
	wal   *os.File
	w     *bufio.Writer
	fsync bool

	snapIndex uint64
	lastIndex uint64
	// entries is a recovery-time copy only; the Core holds the live log.
}

const (
	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3 // drop all entries with index >= payload
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenDiskStorage opens (or creates) the storage and recovers its content.
func OpenDiskStorage(dir string, fsync bool) (*DiskStorage, raft.Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, raft.Storage{}, err
	}
	d := &DiskStorage{dir: dir, fsync: fsync}
	var st raft.Storage
	snap, err := readSnapshot(filepath.Join(dir, "snapshot"))
	if err != nil {
		return nil, st, err
	}
	st.Snapshot = snap
	if snap != nil {
		d.snapIndex = snap.Index
	}
	hs, entries, goodLen, err := replayWAL(filepath.Join(dir, "wal"))
	if err != nil {
		return nil, st, err
	}
	st.HardState = hs
	for _, e := range entries {
		if e.Index > d.snapIndex {
			st.Entries = append(st.Entries, e)
		}
	}
	// Entries must be contiguous after the snapshot.
	for i, e := range st.Entries {
		if e.Index != d.snapIndex+1+uint64(i) {
			return nil, st, fmt.Errorf("raftnode: wal gap at index %d (snapshot %d)", e.Index, d.snapIndex)
		}
	}
	d.lastIndex = d.snapIndex + uint64(len(st.Entries))
	if hs.Commit > d.lastIndex {
		st.HardState.Commit = d.lastIndex
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, st, err
	}
	// Cut a torn tail record, if any.
	if err := f.Truncate(goodLen); err != nil {
		return nil, st, err
	}
	if _, err := f.Seek(goodLen, io.SeekStart); err != nil {
		return nil, st, err
	}
	d.wal = f
	d.w = bufio.NewWriterSize(f, 1<<20)
	return d, st, nil
}

func replayWAL(path string) (hs raft.HardState, entries []raft.Entry, goodLen int64, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return hs, nil, 0, nil
	}
	if err != nil {
		return hs, nil, 0, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return hs, entries, goodLen, nil // clean EOF or torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if n > 64<<20 {
			return hs, entries, goodLen, nil
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return hs, entries, goodLen, nil // torn payload
		}
		if crc32.Checksum(buf, crcTable) != sum || len(buf) == 0 {
			return hs, entries, goodLen, nil
		}
		switch buf[0] {
		case recHardState:
			if len(buf) != 25 {
				return hs, nil, 0, fmt.Errorf("raftnode: bad hardstate record")
			}
			hs = raft.HardState{
				Term:   binary.LittleEndian.Uint64(buf[1:]),
				Vote:   binary.LittleEndian.Uint64(buf[9:]),
				Commit: binary.LittleEndian.Uint64(buf[17:]),
			}
		case recEntry:
			if len(buf) < 21 {
				return hs, nil, 0, fmt.Errorf("raftnode: bad entry record")
			}
			e := raft.Entry{
				Term:  binary.LittleEndian.Uint64(buf[1:]),
				Index: binary.LittleEndian.Uint64(buf[9:]),
				Type:  raft.EntryType(binary.LittleEndian.Uint32(buf[17:])),
				Data:  append([]byte(nil), buf[21:]...),
			}
			for len(entries) > 0 && entries[len(entries)-1].Index >= e.Index {
				entries = entries[:len(entries)-1]
			}
			entries = append(entries, e)
		case recTruncate:
			from := binary.LittleEndian.Uint64(buf[1:])
			for len(entries) > 0 && entries[len(entries)-1].Index >= from {
				entries = entries[:len(entries)-1]
			}
		default:
			return hs, nil, 0, fmt.Errorf("raftnode: unknown wal record %d", buf[0])
		}
		goodLen += int64(8 + n)
	}
}

func (d *DiskStorage) writeRecord(payload []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, crcTable))
	if _, err := d.w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := d.w.Write(payload)
	return err
}

func encodeHardState(hs raft.HardState) []byte {
	b := make([]byte, 25)
	b[0] = recHardState
	binary.LittleEndian.PutUint64(b[1:], hs.Term)
	binary.LittleEndian.PutUint64(b[9:], hs.Vote)
	binary.LittleEndian.PutUint64(b[17:], hs.Commit)
	return b
}

func encodeEntry(e raft.Entry) []byte {
	b := make([]byte, 21+len(e.Data))
	b[0] = recEntry
	binary.LittleEndian.PutUint64(b[1:], e.Term)
	binary.LittleEndian.PutUint64(b[9:], e.Index)
	binary.LittleEndian.PutUint32(b[17:], uint32(e.Type))
	copy(b[21:], e.Data)
	return b
}

// Save durably appends a Ready's hard state and entries. It must complete
// before the Ready's messages are sent. needSync is false when only the
// commit index moved, which is safe to lose (it is re-learned from the
// leader), so no fsync is paid for it.
func (d *DiskStorage) Save(hs *raft.HardState, entries []raft.Entry, termOrVoteChanged bool) error {
	if len(entries) > 0 {
		first := entries[0].Index
		if first <= d.lastIndex {
			var b [9]byte
			b[0] = recTruncate
			binary.LittleEndian.PutUint64(b[1:], first)
			if err := d.writeRecord(b[:]); err != nil {
				return err
			}
		}
		for _, e := range entries {
			if err := d.writeRecord(encodeEntry(e)); err != nil {
				return err
			}
		}
		d.lastIndex = entries[len(entries)-1].Index
	}
	if hs != nil {
		if err := d.writeRecord(encodeHardState(*hs)); err != nil {
			return err
		}
	}
	if err := d.w.Flush(); err != nil {
		return err
	}
	if d.fsync && (len(entries) > 0 || termOrVoteChanged) {
		return d.wal.Sync()
	}
	return nil
}

// SaveSnapshot atomically writes a snapshot and rewrites the WAL to keep
// only entries after it (plus the current hard state).
func (d *DiskStorage) SaveSnapshot(s *raft.Snapshot, hs raft.HardState, remaining []raft.Entry) error {
	if err := writeSnapshot(filepath.Join(d.dir, "snapshot"), s); err != nil {
		return err
	}
	tmp := filepath.Join(d.dir, "wal.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	nd := &DiskStorage{wal: f, w: bufio.NewWriterSize(f, 1<<20)}
	for _, e := range remaining {
		if e.Index > s.Index {
			if err := nd.writeRecord(encodeEntry(e)); err != nil {
				f.Close()
				return err
			}
		}
	}
	if err := nd.writeRecord(encodeHardState(hs)); err != nil {
		f.Close()
		return err
	}
	if err := nd.w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := os.Rename(tmp, filepath.Join(d.dir, "wal")); err != nil {
		f.Close()
		return err
	}
	syncDir(d.dir)
	d.wal.Close()
	d.wal, d.w = f, nd.w
	d.snapIndex = s.Index
	d.lastIndex = s.Index
	if n := len(remaining); n > 0 && remaining[n-1].Index > s.Index {
		d.lastIndex = remaining[n-1].Index
	}
	return nil
}

// Close flushes and closes the WAL.
func (d *DiskStorage) Close() error {
	if d.w != nil {
		d.w.Flush()
	}
	return d.wal.Close()
}

func writeSnapshot(path string, s *raft.Snapshot) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	var hdr [20]byte
	binary.LittleEndian.PutUint64(hdr[0:], s.Index)
	binary.LittleEndian.PutUint64(hdr[8:], s.Term)
	binary.LittleEndian.PutUint32(hdr[16:], crc32.Checksum(s.Data, crcTable))
	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(s.Data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

func readSnapshot(path string) (*raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) < 20 {
		return nil, fmt.Errorf("raftnode: snapshot too short")
	}
	s := &raft.Snapshot{
		Index: binary.LittleEndian.Uint64(b[0:]),
		Term:  binary.LittleEndian.Uint64(b[8:]),
		Data:  b[20:],
	}
	if crc32.Checksum(s.Data, crcTable) != binary.LittleEndian.Uint32(b[16:]) {
		return nil, fmt.Errorf("raftnode: snapshot checksum mismatch")
	}
	return s, nil
}

func syncDir(dir string) {
	if f, err := os.Open(dir); err == nil {
		f.Sync()
		f.Close()
	}
}
