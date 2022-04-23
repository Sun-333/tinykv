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
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// MaxReplicasSize is the max size of logs to replicate by one times
const MaxReplicasSize uint64 = 100

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type VoteResult uint64
const (
	VoteWin VoteResult = iota
	VoteLoss
	VoteProcess
)

// ProgressState represents the state of follower Progress
type ProgressState uint64
const (
	StateReplicate ProgressState = iota
	StateProbe
	StateSnapshot
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

type Tick func()

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
	state ProgressState
	inflight uint64
}

func (pr *Progress) BecomeReplicate() {
	pr.state = StateReplicate
	pr.inflight = MaxReplicasSize
}

func (pr Progress) BecomeSnapshot() {
	pr.state = StateSnapshot
	pr.inflight = 0
}

func (pr *Progress) BecomeProbe() {
	pr.state = StateProbe
	pr.inflight = 1
}
// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
func (pr *Progress) MaybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	pr.Next = max(pr.Next, n+1)
	return updated
}

func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if rejected <= pr.Match {
		return false
	}
	pr.Next = pr.Match + 1
	return true
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	//random election timeout interval
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	//the state of the leader,candidate,follower do the different tk function to advance its logic time
	tk Tick
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	prs := make(map[uint64]*Progress)
	for _, peer := range c.peers {
		prs[peer] = new(Progress)
	}
	 raft := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		//msgs:             make([]pb.Message,0),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
		PendingConfIndex: 0,
		votes: make(map[uint64]bool),
	}
	raft.becomeFollower(0, None)
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		return nil
	}
	raft.Term = hardState.Term
	raft.Vote = hardState.Vote
	return &raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// when r.Prs[to].Next-1 is min snapshot index need to send snapshotRequest msg
func (r *Raft) sendAppend(to uint64, ifCommit bool) bool {
	// Your Code Here (2A).
	m := pb.Message{}
	m.To = to
	m.Term = r.Term
	m.From = r.id

	idx := r.Prs[to].Next
	term, err1 := r.RaftLog.Term(idx - 1)
	ents, err2 := r.RaftLog.entriesWithBegin(idx, 1000)
	if  len(ents) == 0 && !ifCommit {
		return false
	}

	if err1 != nil || err2 !=nil {
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.Snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return false
			}
			panic(err) // TODO(bdarnell)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		// TODO something
	}else {
		m.MsgType = pb.MessageType_MsgAppend
		m.Index = idx - 1
		m.LogTerm = term
		m.Commit = r.RaftLog.committed
		m.Entries = make([]*pb.Entry, 0)
		for _, ent := range ents {
			e := ent
			m.Entries = append(m.Entries,&e)
		}
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	term, err := r.RaftLog.Term(r.RaftLog.committed)
	if err != nil {
		log.Fatal(err)
	}
	r.send(pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeat,
		To:                   to,
		From:                 r.id,
		Term:                 r.Term,
		Commit:               r.RaftLog.committed,
		LogTerm: 			  term,
	})
}


// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.tk()
}

// tickElection is run by follower and candidate after electionTimeout
func (r *Raft) tickElection()  {
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		// If an election timeout happened, the node should pass 'MessageType_MsgHup'
		// to its Step method and start a new election.
		r.Step(pb.Message{
			MsgType:              pb.MessageType_MsgHup,
			From:                 r.id,
		})
	}
}

// tickHeartbeat is run by leader to send `MessageType_MsgBeat` after heartbeatTimeout
func (r *Raft) tickHeartbeat()  {
	if !r.isLeader() {
		log.Info("error occurred during checking sending heartbeat,the node is not leader")
		return
	}
	r.heartbeatElapsed++
	if r.pastHeartbeatTimeout() {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Debugf("error occurred during checking sending heartbeat: %v", err)
		}
	}
}

//pastElectionTimeout return if past the election timeout
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

//pastHeartbeatTimeout return if past the heartbeat timeout
func (r *Raft) pastHeartbeatTimeout() bool {
	return r.heartbeatElapsed >= r.heartbeatTimeout
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.State = StateFollower
	r.Lead = lead
	r.tk = r.tickElection
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.reset(r.Term + 1)
	r.tk = r.tickElection
	r.Vote = r.id
	r.poll(r.id, true)
	r.State = StateCandidate
	//log.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// TODO(xiangli) remove the panic when the raft implementation is stable
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	r.tk = r.tickHeartbeat
	r.Lead = r.id
	r.State = StateLeader
	r.appendEntry([]*pb.Entry{{}}...)
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.
	//r.Prs[r.id].BecomeReplicate()

	//log.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) reset(term uint64)  {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.resetVote()

	r.resetProgress()

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if (m.MsgType == pb.MessageType_MsgAppend) || (m.MsgType == pb.MessageType_MsgAppendResponse) ||
		(m.MsgType == pb.MessageType_MsgRequestVote) || (m.MsgType ==pb.MessageType_MsgRequestVoteResponse) ||
		(m.MsgType == pb.MessageType_MsgSnapshot) || (m.MsgType == pb.MessageType_MsgHeartbeat) ||
		(m.MsgType == pb.MessageType_MsgHeartbeatResponse) || (m.MsgType == pb.MessageType_MsgTransferLeader) ||
		(m.MsgType == pb.MessageType_MsgTimeoutNow) {
		r.preLogic(m)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

//preLogic if m.Term > r.Term change state to follower and set r.Term = m.Term
func (r *Raft) preLogic(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)	
	}
}


func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for i, _ := range r.Prs {
			if i != r.id {
				r.sendRequestVote(i)
			}
		}
		if len(r.Prs) == 1 {
			r.becomeLeader()
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		// transfer msg to leader
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return ErrProposalDropped
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		for i, _ := range r.Prs {
			if i != r.id {
				r.sendRequestVote(i)
			}
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Term == r.Term {
			res :=r.poll(m.From, !m.Reject)
			switch res {
			case VoteWin:
				r.becomeLeader()
				r.bcastAppend(true)
			case VoteLoss:
				r.becomeFollower(r.Term, None)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		// transfer msg to leader
		log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return ErrProposalDropped
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for u, _ := range r.Prs {
			if u != r.id {
				r.sendHeartbeat(u)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Reject {
			r.sendAppend(m.From, false)
		}
	case pb.MessageType_MsgPropose:
		r.appendEntry(m.Entries...)
		r.bcastAppend(false)
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			/*nextProbeIdx := m.Index
			if m.LogTerm > 0 {
				nextProbeIdx = r.RaftLog.findConflictByTerm(m.Index, m.LogTerm)
			}
			if r.Prs[m.From].MaybeDecrTo(m.Index, nextProbeIdx) {
				r.sendAppend(m.From, false)
			}*/
			r.Prs[m.From].Next = m.Index
			r.sendAppend(m.From, false)
		} else {
			if r.Prs[m.From].MaybeUpdate(m.Index) {
				if r.maybeCommit() {
					r.bcastAppend(true)
				}
			}
		}
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgRequestVoteResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			Reject:               true,
		})
	}
	return nil
}


//sendRequestVote make the leader to send request vote to a node
func (r *Raft) sendRequestVote(i uint64) error {
	r.send(pb.Message{
		MsgType:              pb.MessageType_MsgRequestVote,
		To:                   i,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              r.RaftLog.lastTerm(),
		Index:                r.RaftLog.LastIndex(),
	})
	return nil
}

func (r *Raft) send(m pb.Message) error {
	r.msgs = append(r.msgs, m)
	return nil
}


// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.becomeFollower(m.Term, m.From)
	//the m.committed needs >= r.RaftLog.committed
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			Term: 				  r.Term,
			To:                   m.From,
			From:                 r.id,
			Index:                r.RaftLog.committed,
		})
		return
	}
	if lastnewi, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgAppendResponse,
			Term: 				  r.Term,
			To:                   m.From,
			From:                 r.id,
			Index:                lastnewi,
		})
	} else {
		//hintIndex := min(m.Index, r.RaftLog.LastIndex())
		//hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		//hintTerm, err := r.RaftLog.Term(hintIndex)

		//if err != nil {
			//panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		//}
		r.send(pb.Message{
			To:         m.From,
			Term: 		r.Term,
			From:       r.id,
			MsgType:    pb.MessageType_MsgAppendResponse,
			Index:      m.Index,
			Reject:     true,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term || m.Commit > r.RaftLog.LastIndex() || !r.RaftLog.mathTerm(m.Commit, m.LogTerm) {
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeatResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			Reject:               true,
		})
	} else {
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeatResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
		})
		r.becomeFollower(r.Term, m.From)
		r.RaftLog.commitTo(m.Commit)
	}
}

func (r *Raft) handleRequestVote(m pb.Message)  {
	canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
	canVote = canVote && m.Term == r.Term

	if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm){
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgRequestVoteResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
		})
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	} else {
		r.send(pb.Message{
			MsgType:              pb.MessageType_MsgRequestVoteResponse,
			To:                   m.From,
			From:                 r.id,
			Term:                 r.Term,
			Reject:               true,
		})
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

func (r *Raft) resetRandomizedElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

//abort leadership transfer
func (r *Raft) abortLeaderTransfer() {
	r.Lead = None
}

//reset the votes
func (r *Raft) resetVote() {
	r.votes = make(map[uint64]bool)
}

//reset the Progress
func (r *Raft) resetProgress() {
	for k, _ := range r.Prs {
		if k != r.id {
			r.Prs[k] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		} else {
			r.Prs[k] = &Progress{
				Match: r.RaftLog.LastIndex(),
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
}

//quorum turns if the number of peers is >= n/2 + 1
func (r *Raft) quorum() bool {
	var count int
	for _, b := range r.votes {
		if b {
			count++
		}
	}
	return count >= len(r.votes)/2 + 1
}

//bcastAppend do send APP message to other peers 
func (r *Raft) bcastAppend(ifCommit bool) {
	for i, _ := range r.Prs {
		if i != r.id {
			r.sendAppend(i,ifCommit)
		}
	}
}

// poll add vote result to votes
func (r *Raft) poll(from uint64, b bool) VoteResult {
/*	if b {
		log.Infof("%d received %d from %x at term %d", r.id, from, r.Term)
	} else {
		log.Infof("%x received reject %s from %x at term %d", r.id, from, r.Term)
	}*/
	_, ok := r.votes[from]
	if !ok {
		r.votes[from] = b
	}
	var (
		grant, reject int
		res VoteResult
	)
	n := len(r.Prs)/2 + 1
	for _, b := range r.votes {
		if b {
			grant ++
		}else {
			reject ++
		}
	}
	if grant >= n {
		res = VoteWin
	} else {
		if reject >= n {
			res = VoteLoss
		} else {
			if grant + reject == len(r.Prs)  {
				res = VoteLoss
			} else {
				res = VoteProcess
			}
		}
	}
	return res
}

func (r *Raft) appendEntry(es ...*pb.Entry) bool {
	li := r.RaftLog.LastIndex()
	for i, e := range es{
		e.Index = li + 1 + uint64(i)
		e.Term = r.Term
	}
	li = r.RaftLog.append(es...)
	r.Prs[r.id].MaybeUpdate(li)
	r.maybeCommit()
	return true
}

func (r *Raft) maybeCommit() bool {
	mci := r.committed()
	return r.RaftLog.mayCommit(mci, r.Term)
}


// committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged.
func (r *Raft) committed() uint64 {
	var list  sortUint64
	list = make([]uint64, 0)
	for _, progress := range r.Prs {
		list = append(list, progress.Match)
	}
	sort.Sort(list)
	i := len(list) / 2
	return list[i]
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:                 r.Term,
		Vote:                 r.Vote,
		Commit:               r.RaftLog.committed,
	}
}

func (r *Raft) advance(rd Ready) {
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		r.RaftLog.appliedTo(newApplied)
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(e.Index, e.Term)
	}

	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

type sortUint64 []uint64

func (s sortUint64) Len() int {
	return len(s)
}

func (s sortUint64) Less(i, j int) bool {
	return s[i] > s[j]
}

func (s sortUint64) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}






