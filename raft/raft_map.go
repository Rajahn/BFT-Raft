// raft_map.go
package raft

type AppendEntriesCommitKey struct {
	Term  int
	Index int
	Hash  string
}

type CommitKeySlice []AppendEntriesCommitKey

func (s CommitKeySlice) Len() int {
	return len(s)
}

func (s CommitKeySlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s CommitKeySlice) Less(i, j int) bool {
	if s[i].Term < s[j].Term {
		return true
	}
	if s[i].Index < s[j].Index {
		return true
	}

	return s[i].Hash < s[j].Hash
}
