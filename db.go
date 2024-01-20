package volatile

import (
	"sync"

	"github.com/ermes-labs/core-go/commands"
	"github.com/ermes-labs/core-go/infrastructure"
	"github.com/wangjia184/sortedset"
)

type SessionRuntimeState struct {
	RwLocks       uint
	RoLocks       uint
	IsOffloading  bool
	OffloadTarget *infrastructure.SessionLocation
}

type SessionRuntimeMetadata struct {
	commands.SessionMetadata
	SessionRuntimeState
}

type DB struct {
	// The commands
	commands.SessionCommands
	// A Mutex
	Mu sync.Mutex
	// The map
	Sessions map[string]*SessionRuntimeMetadata
	// The sorted set
	SortedByExpirationSessions sortedset.SortedSet
	// The sorted set
	SortedByOffloadableScoreSessions sortedset.SortedSet
}

func NewDB() *DB {
	return &DB{
		Sessions:                         map[string]*SessionRuntimeMetadata{},
		SortedByExpirationSessions:       *sortedset.New(),
		SortedByOffloadableScoreSessions: *sortedset.New(),
	}
}

func NewSessionRuntimeMetadata(
	metadata commands.SessionMetadata,
	sessionState SessionRuntimeState,
) SessionRuntimeMetadata {
	return SessionRuntimeMetadata{
		SessionMetadata:     metadata,
		SessionRuntimeState: sessionState,
	}
}

func NewSessionRuntimeState(
	rwLocks uint,
	roLocks uint,
	isOffloading bool,
	offloadedTo *infrastructure.SessionLocation,
) SessionRuntimeState {
	return SessionRuntimeState{
		RwLocks:       rwLocks,
		RoLocks:       roLocks,
		IsOffloading:  isOffloading,
		OffloadTarget: offloadedTo,
	}
}
