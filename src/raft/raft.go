package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"log"
	"time"
	"math/rand"
	"os"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int // current term. persistent
	resetElectionTimer bool
	electionCV *sync.Cond // used for resetting the election timer
	followerCV *sync.Cond // we became a follower as a result of an RPC
	appendEntriesCV *sync.Cond // enable / disable AppendEntries timer
	currentState int // whether I am a follower, candidate or a leader
	leader int // current leader
	votedFor int // who did we vote for? persistent.
}

const (
	RF_FOLLOWER = 0
	RF_CANDIDATE = 1
	RF_LEADER = 2

	ElectionTimeoutMin = 500
	ElectionTimeoutMax = 800
	AppendEntriesTimeout = 100
)

//
// Prints out a log message prefixed with raft id and current term
// Since this function accesses currentTerm, it assumes that the
// caller has held the mutex lock rf.mu
//
func (rf *Raft) logFunc (message string, args interface{}) {

	log.Printf("[Id:%d, cTerm:%d]: "+message+":%v\n", rf.me, rf.currentTerm, args)

}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.currentState == RF_LEADER)
	
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// AppendEntries RPC arguments structure
//
type AppendEntriesArgs struct {

	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommitIndex int
	

}

//
// AppendEntries RPC reply structure
//
type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {

	// Your data here (2A, 2B).
	CandidateTerm int
	SenderId int
	LastLogIndex int
	LastLogTerm int

}

//
//  RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VotedGranted bool
}




type voteReply struct {
	success bool
	discard bool
	peer *RequestVoteReply	
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	
	// reset the election timer to prevent dead
	// servers from firing their election timers
	rf.resetElectionTimer = true
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		
		rf.mu.Lock()
		// only start an election timer if we are in
		// follower or candidate state
		if rf.currentState != RF_LEADER  {
			
			rf.resetElectionTimer = false
			
			resetElectionTimer := make(chan bool, 1)

			go rf.electionTimer(resetElectionTimer)
			
			rf.electionCV.Wait()
			
			// if we got an AppendEntries or we just became
			// a leader, or we just voted, reset/ignore
			// the election timer when it fires
			if rf.resetElectionTimer {
				
				resetElectionTimer <- true
				rf.logFunc("Resetting election timer. Term", rf.currentTerm)
				rf.mu.Unlock()
				continue
			} else {
				
				// Increment Term. Become Candidate
				// Vote for self. Start Election
				rf.currentTerm++
				rf.currentState = RF_CANDIDATE
				rf.votedFor = rf.me
				sendingTerm := rf.currentTerm
				rf.logFunc("Starting Election. Old Term", sendingTerm-1)
				rf.logFunc("Starting Election. New Term", sendingTerm)
				rf.mu.Unlock()
				go rf.startElection(sendingTerm)
			}
		} else {
			// release the lock, and wait.
			// this goroutine will now only
			// be signalled when we lose 
			// leader status and become follower
			rf.logFunc("Leader. Suspending election timer for now", rf.me)
			rf.followerCV.Wait()
			
			rf.logFunc("Changed state from leader to follower",rf.me)
			rf.mu.Unlock()
		}
	}
}

//
// electionTimer(): goroutine sleeps for the determined
// election timeout duration. quietly exits if we
// received an AppendEntries, or voted in an election,
// or became a leader.
//
func (rf *Raft) electionTimer(resetElectionTimer chan bool) {

	var duration time.Duration
	
	x1 := rand.NewSource(time.Now().UnixNano())
	y1 := rand.New(x1)
	min := ElectionTimeoutMin
	max := ElectionTimeoutMax

	z1 := y1.Intn(max - min) + min
	duration = time.Duration(z1) * time.Millisecond
	
	time.Sleep(duration)

	select {
	case <-resetElectionTimer:
		// An append entries was received,
		// or we won the election, or we just
		// voted. do nothing and gracefully exit.
		break

	default:
		// Election timeout fired without receiving an
		// AppendEntries or us becoming the leader
		// Start the election
		rf.mu.Lock()
		rf.logFunc("\nElection timer completed", duration)
		rf.mu.Unlock()
		rf.electionCV.Signal()
		break
	}
	
	return	
}

//
// 
//
func (rf *Raft) startElection(sendingTerm int) {

	if rf.killed() {
		return
	}

	electionSuccess := rf.dispatchRequestVotes(sendingTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if electionSuccess {
		rf.logFunc("Election completed successfully. Term", sendingTerm)

	} else {

		rf.logFunc("Election failed. Term", sendingTerm)
	}
}


//
// dispatchRequestVotes(): send RequestVote RPCs to all peers
//
func (rf *Raft) dispatchRequestVotes (sendingTerm int) bool {

	var voteCount int = 1 // always vote for oneself
	
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	pendingVotes := len(rf.peers) - 1
	totalPeers := len(rf.peers)
	voteResults := make (chan voteReply, pendingVotes)
	
	for i := 0; i < len(rf.peers); i++ {
		
		if i == rf.me {
			continue
		}
		
		args := RequestVoteArgs{}
		reply := RequestVoteReply{}
		args.CandidateTerm = rf.currentTerm
		args.SenderId = rf.me
		go rf.sendRequestvote(i, sendingTerm, &args, &reply, voteResults)
	}

	rf.mu.Unlock()

	var staleReply bool = false
	var stopRaft bool = false

	// loop to wait for the replies to come in. check rf.killed()
	// to prevent dead servers from processing replies
	for (pendingVotes > 0) && (voteCount < (totalPeers/2 + 1)) {

		if rf.killed() {
			stopRaft = true
			break
		}

		select {
			
		case vote := <- voteResults:

			if vote.discard {

				rf.mu.Lock()
				rf.logFunc("Stale response. Term changed since request. Old term", sendingTerm)
				rf.mu.Unlock()
				staleReply = true
				break
			}
			
			if vote.success {
				if vote.peer.VotedGranted {
					voteCount++
				}

				rf.mu.Lock()
				rf.logFunc("Received vote reply", vote)
				rf.mu.Unlock()
				
				log.Printf("Current vote count: %d\n", voteCount)
			} else {
				rf.mu.Lock()
				rf.logFunc("Vote reply invalid", vote)
				rf.mu.Unlock()
			}
			
			pendingVotes--
			log.Printf("[Id:%d, cTerm:%d]: Pending votes:%d\n", me, currentTerm, pendingVotes)
			
		default:
			continue
		}
	}
		
	if staleReply || stopRaft {
		// the term changed while we were collecting
		// votes, or testing killed us. either way,
		// this election has to be discarded.
		return false
	}

	var electionSuccess bool = false

	rf.mu.Lock()
	
	// election completed. check if we won.
	// but first check again to see if the term
	// is still valid. it could've changed
	// after the last vote was received. 
	if  (sendingTerm == rf.currentTerm) {
		if (voteCount > totalPeers/2) {
		
			// we won the leader election!
			rf.currentState = RF_LEADER
			rf.leader = rf.me
			electionSuccess = true
			rf.logFunc("* * * * * I won the election. Becoming leader * * * * *", rf.me)			
			// Suspend election Timer
			rf.resetElectionTimer = true
			rf.electionCV.Signal()

			// start sending append entries
			go rf.startAppendEntries()
			
		} else {
			// we didn't win the election.
			// or there was a split vote.
			// if it's the latter, another
			// one will start soon enough.
			// become a follower.
			rf.currentState = RF_FOLLOWER
			rf.logFunc("Lost the election. Switching back to follower", rf.me)
		}
	}

	rf.mu.Unlock()

	return electionSuccess	
}


//
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
//
func (rf *Raft) sendRequestvote(server int, sendingTerm int, args *RequestVoteArgs, reply *RequestVoteReply, voteResults chan voteReply) {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if rf.killed() {
		return
	}

	vote := &voteReply{}
	vote.success = ok
	vote.peer = reply

	
	rf.mu.Lock()

	rf.logFunc("RequestVote reply received from peer", server)
	rf.logFunc("Vote Reply",*reply)

	// check if this is a stale reply.
	// this could be a super-delayed reply
	// or we could've lost candidate status
	// on account of an updated appendEntries
	// received from another peer or a
	// RequestVote RPC with a higher term
	if sendingTerm != rf.currentTerm {
		vote.discard = true
	} else {
		vote.discard = false
	}
	rf.mu.Unlock()	

	voteResults <- *vote
}


//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	if rf.killed() {
		return
	}
	
	rf.mu.Lock()

	rf.logFunc("RequestVote RPC recived", args)

	// stale request. reject it.
	if args.CandidateTerm < rf.currentTerm {

		rf.logFunc("We have a higher term number. Reject vote request", args)
		reply.Term = rf.currentTerm
		reply.VotedGranted = false
		rf.mu.Unlock()
		return
	}

	var wasLeader bool = false
	if args.CandidateTerm > rf.currentTerm {

		// if you're a candidate or leader,
		// immediately become a follower 
		if rf.currentState != RF_FOLLOWER {
			wasLeader = (rf.currentState == RF_LEADER)
			rf.currentState = RF_FOLLOWER
			rf.leader = -1
			rf.logFunc("Switching to follower state", args)
		}

		// adopt the requester's term
		rf.currentTerm = args.CandidateTerm
		rf.votedFor = -1
	}

	
	if (rf.votedFor == -1) || (rf.votedFor == args.SenderId) {
		rf.votedFor = args.SenderId
		reply.VotedGranted = true
		rf.resetElectionTimer = true
		rf.logFunc("Accepted vote request", args)
	} else {
		reply.VotedGranted = false
		rf.logFunc("Rejected vote request. Already voted for", rf.votedFor)
	}

	reply.Term = rf.currentTerm

	rf.mu.Unlock()

	if wasLeader {
		// if we have just stepped down 
		// from the leader state, our
		// election timer was paused and
		// we need to restart it
		rf.followerCV.Signal()

		// Note: we don't need to explicitly stop
		// the AppendEntries timer because it
		// checks for leader state before sending
		// out the AppendEntries RPCs
	} else {

		// if we were in candidate or follower state
		// and granted the vote, then reset
		// our election timer
		if reply.VotedGranted {
			rf.electionCV.Signal()
		}
	}
}


//
// appendEntriesTimer()
//
func (rf *Raft) appendEntriesTimer() {

	duration := time.Duration(AppendEntriesTimeout) * time.Millisecond
	time.Sleep(duration)
	rf.appendEntriesCV.Signal()
}


//
// startAppendEntries(): main loop to handle AppendEntries stuff
//
func (rf *Raft) startAppendEntries () {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.me
	
	for rf.currentState == RF_LEADER {

		if rf.killed() {
			break
		}
		
		// start the AppendEntries timer
		go rf.appendEntriesTimer()

		// dispatch AppendEntries to all peers
		go rf.dispatchAppendEntries()

		// wait until the timeout fires
		rf.appendEntriesCV.Wait()
	}

	rf.logFunc("Lost leader status. Stopping AppendEntries.", me)
}



//
// dispatchAppendEntries(): send AppendEntries to all peers
// in parallel
//
func (rf *Raft) dispatchAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	numPeers := len(rf.peers)
	sendingTerm := rf.currentTerm

	for i := 0; i < numPeers; i++ {

		if i == rf.me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}

		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		//TODO: add 2B stuff

		rf.logFunc("Sending Append Entries to Server", i)
		go rf.sendAppendEntries(i, sendingTerm, &args, &reply)
	}	
}


//
// sendAppendEntries(): send AppendEntriesRPC to a single peer
//
func (rf *Raft) sendAppendEntries(server int, sendingTerm int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me

	if sendingTerm != rf.currentTerm {
		log.Printf("[Id:%d, cTerm:%d]: Received stale AppendEntries response from peer:%d, stale term:%d\n", me, currentTerm, server, sendingTerm)
		rf.mu.Unlock()
		return
	}
	
	log.Printf("[Id:%d, cTerm:%d]: AppendEntries reply received from peer:%d, success:%v, reply:%v\n", me, currentTerm, server, ok, *reply)

	var wasLeader bool = false
	if ok {
		if !reply.Success {

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentState = RF_FOLLOWER
				wasLeader = true
			}
		}

		// TODO: count the number of sendAppendEntries
		// to determine when it's safe to commit the log
		
	}

	rf.mu.Unlock()

	// if we just lost leader status, then we need
	// to restart the election timer
	if wasLeader {
		rf.followerCV.Signal()
	}

	// Check if we've lost leader status. If yes, then
	// some of the prevLogIndex stuff should not happen
	// But for 2A, not much to do when we receive an 
	// AppendEntries reply.
	// TODO: More stuff to be added in 2B
}


//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.killed() {
		return
	}

	rf.mu.Lock()
	rf.logFunc("AppendEntries received", args)
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		rf.logFunc("Sender has a lower term number. Ignoring", args)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	var wasLeader bool = false
	
	if args.Term > rf.currentTerm {
		rf.logFunc("Sender has a higher term number", args)
		rf.currentTerm = args.Term
		wasLeader = (rf.currentState == RF_LEADER)
		rf.currentState = RF_FOLLOWER
	}

	
	if rf.currentState == RF_CANDIDATE {
		rf.currentState = RF_FOLLOWER
	}
	
	// ASSERT args.Term == rf.currentTerm
	rf.resetElectionTimer = true
	rf.logFunc("Received a valid AppendEntries RPC", args)

	rf.leader = args.LeaderId
	reply.Term = rf.currentTerm
	reply.Success = true

	if wasLeader {
		rf.logFunc("Switching to follower", args)
			
		// restart the election timeouts
		rf.followerCV.Signal()

		// Note: we don't need to explicitly stop
		// the AppendEntries timer because the
		// we check for leader state before
		// sending cd out the AppendEntries RPCs
	} else {
		
		// reset the election timer. we got a valid AppendEntries
		rf.electionCV.Signal()
	}

	// There will be logic for 2B added here, but
	// we don't need to worry about that right now
	
	return
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.electionCV = sync.NewCond(&rf.mu)
	rf.followerCV = sync.NewCond(&rf.mu)
	rf.appendEntriesCV = sync.NewCond(&rf.mu)

	rf.mu.Lock()
	rf.resetElectionTimer = false
	rf.currentState = RF_FOLLOWER
	rf.leader = -1
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.mu.Unlock()

	// set up the log stuff
	LOG_FILE := "/tmp/raft.log"
	logfile, err := os.OpenFile(LOG_FILE, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(logfile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Printf("Logging Raft to custom file: %s\n\n", LOG_FILE)
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
