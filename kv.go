package volatile

import (
	"context"
	"io"

	"github.com/ermes-labs/core-go/commands"
	"github.com/wangjia184/sortedset"
)

type KVDB struct {
	DB
	KV map[string]string
}

// Sets the session for onload.
func (db *KVDB) Onload(
	ctx context.Context,
	metadata commands.SessionMetadata,
	reader io.Reader,
) (string, error) {
	// Lock the mutex.
	db.Mu.Lock()
	// Unlock the mutex.
	defer db.Mu.Unlock()
	// Create a new session id.
	sessionId := db.get_unique_session_id()

	// Create the session metadata.
	sessionRuntimeMetadata := NewSessionRuntimeMetadata(
		metadata,
		SessionRuntimeState{
			RwLocks:       0,
			RoLocks:       0,
			IsOffloading:  false,
			OffloadTarget: nil,
		},
	)

	// Add the session to the map.
	db.Sessions[sessionId] = &sessionRuntimeMetadata

	// If the session has an expiration date, add it to the sorted set.
	if metadata.ExpiresAt != nil {
		// Add the session to the sorted set.
		db.SortedByExpirationSessions.AddOrUpdate(sessionId, sortedset.SCORE(*metadata.ExpiresAt), nil)
	}

	// Use the reader to retrieve the string content of the session.
	content, err := io.ReadAll(reader)
	// If there is an error, return it.
	if err != nil {
		return "", err
	}

	// Set the content of the session.
	db.KV[sessionId] = string(content)

	// Return the session id.
	return sessionId, nil
}
