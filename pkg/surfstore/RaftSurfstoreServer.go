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

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool

	lastApplied int64

	rpcClients []RaftSurfstoreClient

	/*------------ Leader state ---------------*/
	isLeaderMutex *sync.RWMutex
	nextIndex     []int64

	/*-------------- Server state ---------------*/
	ip       string
	ipList   []string
	serverId int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	//Check if node has crashed
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	//Check if node is a leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	//2. Talk to majority of the servers
	for {
		count := 1
		for idx, ipAddr := range s.ipList {
			if int64(idx) == s.serverId {
				continue
			}
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  -1,
				PrevLogIndex: -1,
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
			}
			conn, err := grpc.Dial(ipAddr, grpc.WithInsecure())
			if err != nil {
				log.Fatal("Error connecting to clients ", err)
			}
			client := NewRaftSurfstoreClient(conn)
			output, _ := client.AppendEntries(ctx, input)
			if output != nil {
				count += 1
			}
		}
		if count > len(s.ipList)/2 {
			infoMap, err := s.metaStore.GetFileInfoMap(ctx, empty)
			if err != nil {
				return nil, err
			}
			return infoMap, nil
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	//Check if node has crashed
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	//Check if node is a leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	//2. Talk to majority of the servers
	for {
		count := 1
		for idx, ipAddr := range s.ipList {
			if int64(idx) == s.serverId {
				continue
			}
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  -1,
				PrevLogIndex: -1,
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
			}
			conn, err := grpc.Dial(ipAddr, grpc.WithInsecure())
			if err != nil {
				log.Fatal("Error connecting to clients ", err)
			}
			client := NewRaftSurfstoreClient(conn)
			output, _ := client.AppendEntries(ctx, input)
			if output != nil {
				count += 1
			}
		}
		if count > len(s.ipList)/2 {
			blockStoreAddr, err := s.metaStore.GetBlockStoreAddr(ctx, empty)
			if err != nil {
				return nil, err
			}
			return blockStoreAddr, nil
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//Check if node has crashed
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	//Check if node is a leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	//Add to leader's log
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &op)
	for idx, _ := range s.nextIndex {
		s.nextIndex[idx] = int64(len(s.log))
	}
	log.Printf("log length of leader is %d", len(s.log))

	//Replicate to followers
	replicated := make(chan bool)
	s.pendingCommits = make([]chan bool, s.commitIndex+1)
	s.pendingCommits = append(s.pendingCommits, replicated)

	go s.attemptReplication()

	//If successfully replicated on majority, then commit on leader and apply to state machine before replying to client
	success := <-replicated
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) attemptReplication() {
	targetIdx := s.commitIndex + 1
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		//Replication on majority is successful, commit on leader
		if commitCount > len(s.ipList)/2 {
			s.pendingCommits[targetIdx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		var prevLogTerm int64 = -1
		var prevLogIndex int64 = -1
		entries := make([]*UpdateOperation, 0)

		if len(s.log) > 0 && int64(len(s.log)) >= s.nextIndex[serverIdx] {
			if int(s.nextIndex[serverIdx]) > 0 {
				prevLogTerm = s.log[s.nextIndex[serverIdx]-1].GetTerm()
				prevLogIndex = s.nextIndex[serverIdx] - 1
			}
			entries = s.log[s.nextIndex[serverIdx]:]
		}

		input := &AppendEntryInput{
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

		output, _ := client.AppendEntries(ctx, input)
		if output == nil || err != nil {
			if errors.Is(err, ERR_NOT_LEADER) {
				s.convertToFollower()
				break
			} else {
				continue
			}
		}

		for !output.Success && s.nextIndex[serverIdx] > 0 {
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
			input = &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      entries,
				LeaderCommit: s.commitIndex,
			}
			if s.isCrashed {
				return
			}

			output, err = client.AppendEntries(ctx, input)

			if output == nil || err != nil {
				if errors.Is(err, ERR_NOT_LEADER) {
					s.convertToFollower()
				}
				break
			}
		}

		if output == nil || err != nil {
			if errors.Is(err, ERR_NOT_LEADER) {
				s.convertToFollower()
				break
			} else {
				continue
			}
		}
		if output.Success {
			s.nextIndex[serverIdx] = output.MatchedIndex + 1
			commitChan <- output
			return
		}

		//Some failure besides crash
		if !output.Success {
			break
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
//Receiver implementation
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	//Check if node has crashed
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	output := &AppendEntryOutput{
		Success:      false,
		ServerId:     s.serverId,
		MatchedIndex: -1,
	}

	// 1.
	if input.Term < s.term {
		log.Println()

		output.Term = s.term
		return output, ERR_NOT_LEADER
	}

	//2.
	if input.PrevLogIndex > -1 {
		if len(s.log) > 0 && int64(len(s.log)) > input.PrevLogIndex {
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				output.Term = input.Term
				output.MatchedIndex = input.PrevLogIndex - 1
				return output, nil
			}
		} else {
			output.Term = input.Term
			output.MatchedIndex = input.PrevLogIndex - 1
			return output, nil
		}
	}

	//Log matches upto PrevLogIndex
	//3. 4.
	if input.Term > s.term {
		s.term = input.Term
	}

	logEntryIndex := input.PrevLogIndex + 1
	if len(s.log) > 0 {
		s.log = s.log[:logEntryIndex]
	}
	s.log = append(s.log, input.Entries...)

	//5.
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	for len(s.log) > 0 && s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		//Check if retries are needed
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			return output, err
		}
	}

	output.Success = true
	output.Term = s.term
	output.MatchedIndex = int64(len(s.log)) - 1

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	//Check if node has crashed
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	//Initialize leader
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	s.term += 1

	s.rpcClients = make([]RaftSurfstoreClient, len(s.ipList))

	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		//Add for future use
		s.rpcClients[idx] = client
	}

	for idx, _ := range s.nextIndex {
		s.nextIndex[idx] = int64(len(s.log))
	}

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//Check if node has crashed
	state, _ := s.IsCrashed(ctx, &emptypb.Empty{})
	if state.IsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	//Check if node is a leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		client := s.rpcClients[idx]
		err := s.retryAppendEntries(client, idx)
		if err != nil {
			break
		}
	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) convertToFollower() {
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	if s.isLeader {
		s.isLeader = false
		s.nextIndex = make([]int64, len(s.ipList))
	}
}

func (s *RaftSurfstore) retryAppendEntries(client RaftSurfstoreClient, idx int) error {
	var prevLogTerm int64 = -1
	var prevLogIndex int64 = -1
	entries := make([]*UpdateOperation, 0)

	if len(s.log) > 0 && int64(len(s.log)) >= s.nextIndex[idx] {
		if int(s.nextIndex[idx]) > 0 {
			prevLogTerm = s.log[s.nextIndex[idx]-1].GetTerm()
			prevLogIndex = s.nextIndex[idx] - 1
		}
		entries = s.log[s.nextIndex[idx]:]
	}
	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := client.AppendEntries(ctx, input)
	if err != nil && strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
		s.convertToFollower()
		return ERR_NOT_LEADER
	}
	if output == nil || err != nil {
		return nil
	}
	for !output.Success && s.nextIndex[idx] > 0 {
		s.nextIndex[idx] -= 1
		if len(s.log) > 0 {
			prevLogIndex = s.nextIndex[idx] - 1
			if prevLogIndex >= 0 {
				prevLogTerm = s.log[prevLogIndex].GetTerm()
			} else {
				prevLogTerm = -1
			}
			entries = s.log[s.nextIndex[idx]:]
		}
		input = &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}
		s.notCrashedCond.L.Lock()
		for s.isCrashed {
			s.notCrashedCond.Wait()
		}
		s.notCrashedCond.L.Unlock()
		output, err = client.AppendEntries(ctx, input)
		log.Println(output)
		log.Println(err)
		if output == nil || err != nil {
			if errors.Is(err, ERR_NOT_LEADER) {
				s.convertToFollower()
			}
			break
		}
	}
	if output != nil && output.Success {
		s.nextIndex[idx] = output.MatchedIndex + 1
	}
	return nil
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
