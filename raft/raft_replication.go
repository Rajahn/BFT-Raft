package raft

import (
	"time"
)

type LogEntry struct {
	Term         int         // the log entry's term
	CommandValid bool        // if it should be applied
	Command      interface{} // the command should be applied to the state machine
	Signature    []byte
	AcceptCount  int //当前日志项在所有节点中, 已经被多少节点确认持有
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the match point
	// leader在尝试分发日志时, 要带上自己上一条已经提交的日志的索引和任期, 供follower检查
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	// leader在分发日志时, 带上自己的commitIndex, 供follower执行apply
	LeaderCommit int
}

type AppendEntriesCommitArgs struct {
	Term       int
	PeerId     int    // 确认消息发送者Id
	Signature  []byte // PeerId 的数字签名，用于防止拜占庭节点伪造确认消息
	EntryIndex int
	EntryTerm  int
	EntryHash  string
}

type AppendEntriesCommitReply struct {
	Term    int
	Success bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm //在reply中记录自己当前保存日志的term, 便于leader回退匹配
	reply.Success = false

	//发现伪造的日志, 说明leader已经丢失, 立即开启选举
	//if len(args.Entries) > 0 && !verifySignature(GetBytes(args.Entries[0].Command), args.Entries[0].Signature) {
	//	rf.becomeCandidateLocked()
	//	fmt.Printf("FOLLOWER %d becomes CANDIDATE..., Current Time: %v\n", rf.me, time.Now().UnixNano()/1000000)
	//	return
	//}

	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	//在收到leader的日志分发请求(心跳)后, 重置自己的选举计时器, 无论是否接受
	defer rf.resetElectionTimerLocked()
	// return failure if prevLog not matched
	if args.PrevLogIndex >= len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short, Len:%d < Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	// append the leader log entries to local
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	//追加日志后, 不去立即更新commitIndex以推进apply工作流, 而是发起一次广播, 告知其他节点某条记录已被我收到
	for index := args.PrevLogIndex + 1; index < len(rf.log); index++ {
		broadcastAppendEntriesCommit(rf, index, rf.log[index].Term)
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// leader分发日志的安全性保证, 只能分发本term的日志, 不能重发之前的, 否则可能造成覆盖
func (rf *Raft) replicateToPeer(peer, term int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
		return
	}

	// align the term
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	//如果在某次执行分发的rpc后,leader身份已经丢失, 则不再处理
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
		return
	}

	// hanle the reply
	// probe the lower index if the prevLog not matched
	if !reply.Success {
		// go back a term
		idx, term := args.PrevLogIndex, args.PrevLogTerm
		for idx > 0 && rf.log[idx].Term == term {
			idx--
		}
		rf.nextIndex[peer] = idx + 1
		LOG(rf.me, rf.currentTerm, DLog, "Not match with S%d in %d, try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
		return
	}

	// update match/next index if log appended successfully
	rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
}

func broadcastAppendEntriesCommit(rf *Raft, index int, term int) {

	for i := range rf.peers {
		if i != rf.me && rf.role != Candidate {
			args := AppendEntriesCommitArgs{Term: rf.currentTerm, PeerId: rf.me, EntryIndex: index, EntryTerm: term}
			reply := AppendEntriesCommitReply{}
			go func(server int) {
				rf.sendAppendEntriesCommit(server, args, &reply)
			}(i)
		}
	}
}
func (rf *Raft) sendAppendEntriesCommit(server int, args AppendEntriesCommitArgs, reply *AppendEntriesCommitReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesCommit", args, reply)
	return ok
}

func (rf *Raft) AppendEntriesCommit(args AppendEntriesCommitArgs, reply *AppendEntriesCommitReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	key := AppendEntriesCommitKey{Term: args.EntryTerm, Index: args.EntryIndex}
	_, ok := rf.m[key]
	if ok {
		rf.m[key] += 1
	} else {
		rf.m[key] = 1
	}

	rf.persistLocked()

	if rf.m[key] >= len(rf.peers)/2 && key.Index > rf.commitIndex {
		rf.commitIndex = key.Index
		rf.applyCond.Signal()
		//fmt.Println("Follower ", rf.me, " m[key] ", rf.m[key], " key ", key)
		LOG(rf.me, rf.currentTerm, DWarn, "Node update the commit index %d->%d", rf.commitIndex, key.Index)
	}
	//fmt.Println("Follower ", rf.me, " m[key] ", rf.m[key], " key ", key)
	reply.Success = true
}

func (rf *Raft) startReplication(term int) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = len(rf.log) - 1 // leader自己的日志第一条是哨兵, 因此-1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIdx].Term
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:], //从leader自身的日志列表里, 找到对应peer应该添加的部分
			LeaderCommit: rf.commitIndex,
		}
		go rf.replicateToPeer(peer, term, args)
	}

	return true
}

// could only replcate in the given term
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicateInterval)
	}
}
