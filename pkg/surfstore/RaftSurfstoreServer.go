package surfstore

import (
	context "context"
	"errors"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	isLeader bool
	term     int64
	log      []*UpdateOperation

	commitIndex    int64
	pendingCommits []chan bool
	lastApplied    int64
	metaStore      *MetaStore
	rpcClients     []RaftSurfstoreClient

	/*-------------- Server state ---------------*/
	serverId int64
	ip       string
	ipList   []string

	/*------------ Leader state ---------------*/
	nextIndex     []int64
	isLeaderMutex *sync.RWMutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for {
		if s.getCount(ctx) > len(s.ipList)/2 {
			m, err := s.metaStore.GetFileInfoMap(ctx, empty)
			if err != nil {
				return nil, err
			}
			return m, nil
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for {
		if s.getCount(ctx) > len(s.ipList)/2 {
			blkAdr, err := s.metaStore.GetBlockStoreAddr(ctx, empty)
			if err != nil {
				return nil, err
			}
			return blkAdr, nil
		}
	}
}

func (s *RaftSurfstore) getCount(ctx context.Context) int {
	c := 1
	for i, addr := range s.ipList {
		if int64(i) != s.serverId {
			appendIp := &AppendEntryInput{
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
				Term:         s.term,
				PrevLogTerm:  -1,
				PrevLogIndex: -1,
			}
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatal("Error connecting to clients ", err)
			}
			surfStrClient := NewRaftSurfstoreClient(conn)
			appendOp, _ := surfStrClient.AppendEntries(ctx, appendIp)
			if appendOp != nil {
				c += 1
			}
		}
	}
	return c
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &op)
	for idx := range s.nextIndex {
		s.nextIndex[idx] = int64(len(s.log))
	}

	replicated := make(chan bool)
	s.pendingCommits = make([]chan bool, s.commitIndex+1)
	s.pendingCommits = append(s.pendingCommits, replicated)

	go s.attemptReplication()

	success := <-replicated
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		var prevLogIndex int64 = -1
		var prevLogTerm int64 = -1
		entries := make([]*UpdateOperation, 0)
		client := NewRaftSurfstoreClient(conn)

		if len(s.log) > 0 && int64(len(s.log)) >= s.nextIndex[serverIdx] {
			if int(s.nextIndex[serverIdx]) > 0 {
				prevLogTerm = s.log[s.nextIndex[serverIdx]-1].GetTerm()
				prevLogIndex = s.nextIndex[serverIdx] - 1
			}
			entries = s.log[s.nextIndex[serverIdx]:]
		}

		appendEntryInput := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if s.isCrashed {
			return
		}

		appendEntryOutput, _ := client.AppendEntries(ctx, appendEntryInput)
		if appendEntryOutput == nil || err != nil {
			if errors.Is(err, ERR_NOT_LEADER) {
				s.isLeaderMutex.Lock()
				defer s.isLeaderMutex.Unlock()
				if s.isLeader {
					s.isLeader = false
					s.nextIndex = make([]int64, len(s.ipList))
				}
				break
			} else {
				continue
			}
		}

		for !appendEntryOutput.Success && s.nextIndex[serverIdx] > 0 {
			s.nextIndex[serverIdx] -= 1
			if len(s.log) > 0 {
				prevLogIndex = s.nextIndex[serverIdx] - 1
				if prevLogIndex >= 0 {
					prevLogTerm = s.log[prevLogIndex].GetTerm()
				} else {
					prevLogTerm = -1
				}
				entries = s.log[s.nextIndex[serverIdx]:]
			}
			appendEntryInput = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			if s.isCrashed {
				return
			}

			appendEntryOutput, err = client.AppendEntries(ctx, appendEntryInput)

			if appendEntryOutput == nil || err != nil {
				if errors.Is(err, ERR_NOT_LEADER) {
					s.isLeaderMutex.Lock()
					defer s.isLeaderMutex.Unlock()
					if s.isLeader {
						s.isLeader = false
						s.nextIndex = make([]int64, len(s.ipList))
					}
				}
				break
			}
		}

		if appendEntryOutput == nil || err != nil {
			if !errors.Is(err, ERR_NOT_LEADER) {
				continue
			} else {
				s.isLeaderMutex.Lock()
				defer s.isLeaderMutex.Unlock()
				if s.isLeader {
					s.isLeader = false
					s.nextIndex = make([]int64, len(s.ipList))
				}
				break
			}
		}
		if !appendEntryOutput.Success {
			break
		}
		if appendEntryOutput.Success {
			s.nextIndex[serverIdx] = appendEntryOutput.MatchedIndex + 1
			commitChan <- appendEntryOutput
			return
		}
	}
}

func (s *RaftSurfstore) attemptReplication() {
	newId := s.commitIndex + 1
	chanel := make(chan *AppendEntryOutput, len(s.ipList))
	for idx := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), newId, chanel)
	}

	c := 1
	for {
		commit := <-chanel
		if commit != nil && commit.Success {
			c++
		}
		//Replication on majority is successful, commit on leader
		if c > len(s.ipList)/2 {
			s.pendingCommits[newId] <- true
			s.commitIndex = newId
			break
		}
	}
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	//Check if node has crashed
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	appendEntryOutput := &AppendEntryOutput{
		Success:      false,
		ServerId:     s.serverId,
		MatchedIndex: -1,
	}

	if input.Term < s.term {
		appendEntryOutput.Term = s.term
		return appendEntryOutput, ERR_NOT_LEADER
	}

	if input.PrevLogIndex > -1 {
		if len(s.log) > 0 && int64(len(s.log)) > input.PrevLogIndex {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				appendEntryOutput.Term = input.Term
				appendEntryOutput.MatchedIndex = input.PrevLogIndex - 1
				return appendEntryOutput, nil
			}
		} else {
			appendEntryOutput.Term = input.Term
			appendEntryOutput.MatchedIndex = input.PrevLogIndex - 1
			return appendEntryOutput, nil
		}
	}

	if input.Term > s.term {
		s.term = input.Term
	}

	logEntryIndex := input.PrevLogIndex + 1
	if len(s.log) > 0 {
		s.log = s.log[:logEntryIndex]
	}
	s.log = append(s.log, input.Entries...)

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	for len(s.log) > 0 && s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			return appendEntryOutput, err
		}
	}

	appendEntryOutput.Success = true
	appendEntryOutput.Term = s.term
	appendEntryOutput.MatchedIndex = int64(len(s.log)) - 1

	return appendEntryOutput, nil
}

func (s *RaftSurfstore) appendEntriesRetry(client RaftSurfstoreClient, idx int) error {
	var previousLogIndex int64 = -1
	var previousLogTerm int64 = -1

	entries := make([]*UpdateOperation, 0)

	if len(s.log) > 0 && int64(len(s.log)) >= s.nextIndex[idx] {
		if int(s.nextIndex[idx]) > 0 {
			previousLogTerm = s.log[s.nextIndex[idx]-1].GetTerm()
			previousLogIndex = s.nextIndex[idx] - 1
		}
		entries = s.log[s.nextIndex[idx]:]
	}

	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  previousLogTerm,
		PrevLogIndex: previousLogIndex,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := client.AppendEntries(ctx, input)
	if err != nil && strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		if s.isLeader {
			s.isLeader = false
			s.nextIndex = make([]int64, len(s.ipList))
		}
		return ERR_NOT_LEADER
	}
	if output == nil || err != nil {
		return nil
	}

	for !output.Success && s.nextIndex[idx] > 0 {
		s.nextIndex[idx] -= 1
		if len(s.log) > 0 {
			previousLogIndex = s.nextIndex[idx] - 1
			if previousLogIndex < 0 {
				previousLogTerm = -1
			} else {
				previousLogTerm = s.log[previousLogIndex].GetTerm()
			}
			entries = s.log[s.nextIndex[idx]:]
		}
		input = &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  previousLogTerm,
			PrevLogIndex: previousLogTerm,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}
		s.notCrashedCond.L.Lock()
		for s.isCrashed {
			s.notCrashedCond.Wait()
		}
		s.notCrashedCond.L.Unlock()
		output, err = client.AppendEntries(ctx, input)
		if output == nil || err != nil {
			if errors.Is(err, ERR_NOT_LEADER) {
				s.isLeaderMutex.Lock()
				defer s.isLeaderMutex.Unlock()
				if s.isLeader {
					s.isLeader = false
					s.nextIndex = make([]int64, len(s.ipList))
				}
			}
			break
		}
	}
	if output != nil && output.Success {
		s.nextIndex[idx] = output.MatchedIndex + 1
	}
	return nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	//Check if node has crashed
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	//Initialize leader
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	s.term += 1

	s.rpcClients = make([]RaftSurfstoreClient, len(s.ipList))

	for i, ip := range s.ipList {
		if int64(i) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil
		}
		s.rpcClients[i] = NewRaftSurfstoreClient(conn)
	}

	for i := range s.nextIndex {
		s.nextIndex[i] = int64(len(s.log))
	}

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//Check if node has crashed
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	//Check if node is a leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for i := range s.ipList {
		if int64(i) != s.serverId {
			err := s.appendEntriesRetry(s.rpcClients[i], i)
			if err != nil {
				break
			}
		}
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
