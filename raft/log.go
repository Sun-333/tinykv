// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sort"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstIdx uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := RaftLog{
		storage:         storage,

	}
	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Fatal("init newLog failed")
	}
	//raftLog.pendingSnapshot = &snapshot
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Fatal("init newLog failed")
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Fatal("init newLog failed")
	}

	raftLog.stabled = lastIndex
	raftLog.firstIdx = firstIndex
	raftLog.applied = firstIndex - 1
	raftLog.committed = hardState.Commit
	if lastIndex != 0 {
		raftLog.entries, err = storage.Entries(firstIndex, lastIndex + 1)
		if err != nil {
			log.Fatal("init newLog failed")
		}
	}
	return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).

}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	offset := l.stabled - l.firstIdx + 1
	if offset > uint64(len(l.entries) - 1) {
		return []pb.Entry{}
	}
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := max(l.applied+1, l.firstIndex())
	if l.committed + 1 > off {
		ents, err := l.slice(off, l.committed+1, 10000)
		if err != nil {
			log.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if idx, ok := l.unstableMaybeLastIndex(); ok {
		return idx
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	lo, hi := l.firstIndex(), l.LastIndex()
	if i < lo -1 || i > hi {
		return  0, nil
	}
	if term, ok := l.unstableMaybeTerm(i); ok {
		return term, nil
	}
	term, err := l.storage.Term(i)

	if err == nil {
		return term, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err) // TODO(bdarnell)
}

func (l *RaftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.LastIndex())
}

func (l *RaftLog) append(entries ...*pb.Entry ) uint64 {
	ens := make([]pb.Entry, 0)
	for _, entry := range entries {
		ens = append(ens,*entry)
	}
	l.entries = append(l.entries, ens...)
	return  l.LastIndex()
}

//
func (l *RaftLog) mayCommit(maxIndex uint64, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// firstIndex return the first Index of the entries
// if there is a snapshot returned snapshot.metadata.index
// otherwise return the first Index form storage
func (l *RaftLog) firstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// unstableMaybeTerm if unstable entries find an entry.Index == index return the entry.Term, true
// otherwise return 0, false.
// find the entry by binary search
func (l *RaftLog) unstableMaybeTerm(index uint64) (uint64, bool) {
	if len(l.entries) == 0 {
		return 0, false
	}
	//find the entry by binary search
	i := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].Index >= index
	})
	if i < len(l.entries) && l.entries[i].Index == index {
		return l.entries[i].Term, true
	}
	return 0, false
}

func (l *RaftLog) unstableMaybeLastIndex() (uint64, bool) {
	if len(l.entries) == 0 {
		return 0, false
	}
	return l.entries[len(l.entries) - 1].Index, true
}

func (l *RaftLog) lastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return term
}

func (l *RaftLog) commitTo(toCommit uint64) {
	if l.committed < toCommit {
		if l.LastIndex() < toCommit {
			log.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", toCommit, l.LastIndex())
		}
		l.committed = toCommit
	}
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	log.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) entriesWithBegin(idx uint64, maxsize uint64) ([]pb.Entry, error) {
	if idx > l.LastIndex() {
		return nil, nil
	}
	return l.slice(idx, l.LastIndex() + 1, maxsize)
}

func (l *RaftLog) slice(lo uint64, hi uint64, maxsize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var left, right int
	left, right = int(lo - l.firstIdx), int(hi - l.firstIdx)
	return l.entries[left : right] , nil
}

// must l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo uint64, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if hi > fi+length {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}


// Snapshot returns a snapshot
func (l *RaftLog) Snapshot()  (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(index uint64, term uint64, commit uint64, entries ...*pb.Entry) (lastnewi uint64, ok bool) {
	if l.mathTerm(index, term) {
		lastnewi = uint64(len(entries)) + index
		ci :=l.findConflict(entries...)
		switch  {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			if ci <= l.LastIndex() {
				l.entries = l.entries[0:ci - l.firstIdx]
				l.stabled = min(ci - 1, l.stabled)
			}
			offset := index + 1
			l.append(entries[ci - offset:]...)
		}
		l.commitTo(min(commit, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) mathTerm(i, term uint64) bool {
	u, err := l.Term(i)
	if err != nil {
		return false
	}
	return term == u
}

func (l *RaftLog) findConflict(entries ...*pb.Entry) uint64 {
	for _, en := range entries {
		if !l.mathTerm(en.Index, en.Term) {
			if en.Index < l.LastIndex() {
				log.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					en.Index, l.zeroTermOnErrCompacted(l.Term(en.Index)), en.Term)
			}
			return en.Index
		}
	}
	return 0
}

// findConflictByTerm takes an (index, term) pair (indicating a conflicting log
// entry on a leader/follower during an append) and finds the largest index in
// log l with a term <= `term` and an index <= `index`. If no such index exists
// in the log, the log's first index is returned.
//
// The index provided MUST be equal to or less than l.lastIndex(). Invalid
// inputs log a warning and the input index is returned.
func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index != 0
}

func (l *RaftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) stableTo(i uint64, t uint64) {
	gt, err := l.Term(i)
	if err != nil {
		log.Panic(err)
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i > l.stabled {
		//l.entries = l.entries[i-l.stabled:]
		l.stabled = i
	}
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

