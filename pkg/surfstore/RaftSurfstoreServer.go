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

	opUpdate := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &opUpdate)
	for i := range s.nextIndex {
		s.nextIndex[i] = int64(len(s.log))
	}

	rep := make(chan bool)
	s.pendingCommits = make([]chan bool, s.commitIndex+1)
	s.pendingCommits = append(s.pendingCommits, rep)

	go s.tryReplicate()

	succ := <-rep
	if succ {
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
		var previousLogIndex int64 = -1
		var previousLogTerm int64 = -1
		entries := make([]*UpdateOperation, 0)
		client := NewRaftSurfstoreClient(conn)

		if len(s.log) > 0 && int64(len(s.log)) >= s.nextIndex[serverIdx] {
			if int(s.nextIndex[serverIdx]) > 0 {
				previousLogTerm = s.log[s.nextIndex[serverIdx]-1].GetTerm()
				previousLogIndex = s.nextIndex[serverIdx] - 1
			}
			entries = s.log[s.nextIndex[serverIdx]:]
		}

		appendEntryInput := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  previousLogTerm,
			PrevLogIndex: previousLogIndex,
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
				previousLogIndex = s.nextIndex[serverIdx] - 1
				if previousLogIndex >= 0 {
					previousLogTerm = s.log[previousLogIndex].GetTerm()
				} else {
					previousLogTerm = -1
				}
				entries = s.log[s.nextIndex[serverIdx]:]
			}
			appendEntryInput = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  previousLogTerm,
				PrevLogIndex: previousLogIndex,
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

		if appendEntryOutput.Success {
			s.nextIndex[serverIdx] = appendEntryOutput.MatchedIndex + 1
			commitChan <- appendEntryOutput
			return
		} else {
			break
		}
	}
}

func (s *RaftSurfstore) tryReplicate() {
	n := s.commitIndex + 1
	chanel := make(chan *AppendEntryOutput, len(s.ipList))
	for i := range s.ipList {
		if int64(i) == s.serverId {
			continue
		}
		go s.commitEntry(int64(i), n, chanel)
	}
	c := 1
	for {
		comm := <-chanel
		if comm != nil && comm.Success {
			c++
		}
		if c > len(s.ipList)/2 {
			s.pendingCommits[n] <- true
			s.commitIndex = n
			break
		}
	}
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
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
				appendEntryOutput.MatchedIndex = input.PrevLogIndex - 1
				appendEntryOutput.Term = input.Term
				return appendEntryOutput, nil
			}
		} else {
			appendEntryOutput.MatchedIndex = input.PrevLogIndex - 1
			appendEntryOutput.Term = input.Term
			return appendEntryOutput, nil
		}
	}
	if input.Term > s.term {
		s.term = input.Term
	}
	lInd := input.PrevLogIndex + 1
	if len(s.log) > 0 {
		s.log = s.log[:lInd]
	}
	s.log = append(s.log, input.Entries...)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}
	for len(s.log) > 0 && s.lastApplied < s.commitIndex {
		s.lastApplied++
		e := s.log[s.lastApplied]
		_, err := s.metaStore.UpdateFile(ctx, e.FileMetaData)
		if err != nil {
			return appendEntryOutput, err
		}
	}
	appendEntryOutput.Term = s.term
	appendEntryOutput.Success = true
	appendEntryOutput.MatchedIndex = int64(len(s.log)) - 1
	return appendEntryOutput, nil
}

func (s *RaftSurfstore) appendEntriesRetry(client RaftSurfstoreClient, idx int) error {
	var previousLogIndex int64 = -1
	var previousLogTerm int64 = -1

	ent := make([]*UpdateOperation, 0)

	if len(s.log) > 0 && int64(len(s.log)) >= s.nextIndex[idx] {
		if int(s.nextIndex[idx]) > 0 {
			previousLogTerm = s.log[s.nextIndex[idx]-1].GetTerm()
			previousLogIndex = s.nextIndex[idx] - 1
		}
		ent = s.log[s.nextIndex[idx]:]
	}

	input := &AppendEntryInput{
		Entries:      ent,
		Term:         s.term,
		PrevLogIndex: previousLogIndex,
		LeaderCommit: s.commitIndex,
		PrevLogTerm:  previousLogTerm,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	appendOp, err := client.AppendEntries(ctx, input)
	if err != nil && strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		if s.isLeader {
			s.isLeader = false
			s.nextIndex = make([]int64, len(s.ipList))
		}
		return ERR_NOT_LEADER
	}
	if appendOp == nil || err != nil {
		return nil
	}
	for !appendOp.Success && s.nextIndex[idx] > 0 {
		s.nextIndex[idx] -= 1
		if len(s.log) > 0 {
			previousLogIndex = s.nextIndex[idx] - 1
			if previousLogIndex < 0 {
				previousLogTerm = -1
			} else {
				previousLogTerm = s.log[previousLogIndex].GetTerm()
			}
			ent = s.log[s.nextIndex[idx]:]
		}
		input = &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  previousLogTerm,
			PrevLogIndex: previousLogTerm,
			Entries:      ent,
			LeaderCommit: s.commitIndex,
		}
		s.notCrashedCond.L.Lock()
		for s.isCrashed {
			s.notCrashedCond.Wait()
		}
		s.notCrashedCond.L.Unlock()
		appendOp, err = client.AppendEntries(ctx, input)
		if appendOp == nil || err != nil {
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
	if appendOp != nil && appendOp.Success {
		s.nextIndex[idx] = appendOp.MatchedIndex + 1
	}
	return nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	s.term += 1
	s.rpcClients = make([]RaftSurfstoreClient, len(s.ipList))
	for i, ip := range s.ipList {
		if int64(i) != s.serverId {
			conn, err := grpc.Dial(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, nil
			}
			s.rpcClients[i] = NewRaftSurfstoreClient(conn)
		}
	}
	for i := range s.nextIndex {
		s.nextIndex[i] = int64(len(s.log))
	}
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	serverState, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if serverState.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}
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
