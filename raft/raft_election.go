package raft

import (
	"math/rand"
	"time"
)

// 重置选举超时计时器
func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

// 判断选举超时
func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// align the term 如果请求投票的节点任期比自己低, 直接忽略投票请求
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// check for votedFor 如果还没投过票, 进行投票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted to S%d", args.CandidateId, rf.votedFor)
		return
	}

	// check if candidate's last log is more up to date
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject voted, Candidate less up-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked() //重置自己的选举计时器
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Vote granted", args.CandidateId)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// check whether my last log is more up to date than the candidate's last log
func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term

	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

func (rf *Raft) askVoteFromPeer(term, peer int, args *RequestVoteArgs, votes chan bool) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, reply)

	// handle the response
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask vote, Lost or error", peer)
		votes <- false
		return
	}

	// align term
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		votes <- false
		return
	}

	// check the context
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Lost context, abort RequestVoteReply", peer)
		votes <- false
		return
	}

	// count the votes
	if reply.VoteGranted {
		votes <- true
	} else {
		votes <- false
	}
}

func (rf *Raft) startElection(term int) {
	votes := make(chan bool, len(rf.peers)) // channel to collect votes
	count := 0                              // count the votes

	rf.mu.Lock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate[T%d] to %s[T%d], abort RequestVote", rf.role, term, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	l := len(rf.log) //记录自己最新的日志条目
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			count++
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
		}

		go rf.askVoteFromPeer(term, peer, args, votes)
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers)-1; i++ {
		if <-votes {
			count++
		}
		if count > len(rf.peers)/2 {
			rf.mu.Lock()
			rf.becomeLeaderLocked()
			rf.mu.Unlock()
			go rf.replicationTicker(term)
			return
		}
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {

		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
