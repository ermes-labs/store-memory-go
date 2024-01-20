package volatile

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/ermes-labs/core-go/commands"
	"github.com/ermes-labs/core-go/infrastructure"
	"github.com/google/uuid"
	"github.com/wangjia184/sortedset"
)

func compute_score(metadata *SessionRuntimeMetadata) sortedset.SCORE {
	// Constants.
	decay := 0.1
	currentTime := time.Now().Unix()
	deltaTime := currentTime - metadata.UpdatedAt
	// The score starts from the updatedAt field.
	activityScore := currentTime + int64(float64(metadata.StaticOffloadableScore)*math.Exp(-decay*float64(deltaTime)))
	// Return the score.
	return sortedset.SCORE(activityScore)
}

func (db *DB) get_unique_session_id() string {
	// Loop until a unique session id is found.
	for {
		sessionId := uuid.New().String()
		if _, ok := db.Sessions[sessionId]; !ok {
			// Return the session id.
			return sessionId
		}
	}
}

// Returns the metadata of a session.
func (db *DB) Get_metadata(
	ctx context.Context,
	sessionId string,
) (commands.SessionMetadata, error) {
	return db.Sessions[sessionId].SessionMetadata, nil
}

// Creates a new session and acquires it in read-write mode.
func (db *DB) Create_and_rw_acquire(
	ctx context.Context,
	geoCoords *infrastructure.GeoCoordinates,
	expiresAt *int64,
) (string, error) {
	// Lock the mutex.
	db.Mu.Lock()
	// Unlock the mutex.
	defer db.Mu.Unlock()
	// Create a new session id.
	sessionId := db.get_unique_session_id()
	// Get the current time.
	now := time.Now().Unix()

	// Create the session metadata.
	sessionRuntimeMetadata := NewSessionRuntimeMetadata(
		commands.SessionMetadata{
			ClientGeoCoordinates:   *geoCoords,
			StaticOffloadableScore: 0,
			CreatedIn:              "",
			CreatedAt:              now,
			UpdatedAt:              now,
			ExpiresAt:              expiresAt,
		},
		SessionRuntimeState{
			RwLocks:       1,
			RoLocks:       0,
			IsOffloading:  false,
			OffloadTarget: nil,
		},
	)

	// Add the session to the map.
	db.Sessions[sessionId] = &sessionRuntimeMetadata
	// Add the session to the sorted set.
	db.SortedByOffloadableScoreSessions.AddOrUpdate(sessionId, sortedset.SCORE(now), nil)

	// If the session has an expiration date, add it to the sorted set.
	if expiresAt != nil {
		// Add the session to the sorted set.
		db.SortedByExpirationSessions.AddOrUpdate(sessionId, sortedset.SCORE(*expiresAt), nil)
	}

	// Return the session id.
	return sessionId, nil
}

// Creates a new session and acquires it in read-only mode.
func (db *DB) Create_and_ro_acquire(
	ctx context.Context,
	geoCoords *infrastructure.GeoCoordinates,
	expiresAt *int64,
) (string, error) {
	// Lock the mutex.
	db.Mu.Lock()
	// Unlock the mutex.
	defer db.Mu.Unlock()
	// Create a new session id.
	sessionId := db.get_unique_session_id()
	// Get the current time.
	now := time.Now().Unix()

	// Create the session metadata.
	sessionRuntimeMetadata := NewSessionRuntimeMetadata(
		commands.SessionMetadata{
			ClientGeoCoordinates:   *geoCoords,
			StaticOffloadableScore: 0,
			CreatedIn:              "",
			CreatedAt:              now,
			UpdatedAt:              now,
			ExpiresAt:              expiresAt,
		},
		SessionRuntimeState{
			RwLocks:       0,
			RoLocks:       1,
			IsOffloading:  false,
			OffloadTarget: nil,
		},
	)

	// Add the session to the map.
	db.Sessions[sessionId] = &sessionRuntimeMetadata

	// If the session has an expiration date, add it to the sorted set.
	if expiresAt != nil {
		// Add the session to the sorted set.
		db.SortedByExpirationSessions.AddOrUpdate(sessionId, sortedset.SCORE(*expiresAt), nil)
	}

	// Return the session id.
	return sessionId, nil
}

// Acquires a session in read-write mode. If the session is not
// available for read-write, it returns false and does not acquire the session.
func (db *DB) Rw_acquire(
	ctx context.Context,
	sessionId string,
) (*infrastructure.SessionLocation, bool, error) {
	// Get the session.
	sessionRuntimeMetadata, ok := db.Sessions[sessionId]
	// If the session does not exist, return an error.
	if !ok {
		return nil, false, errors.New("Session not found") //commands.SessionNotFoundError
	}

	// If the session is not active, return false.
	if sessionRuntimeMetadata.SessionRuntimeState.OffloadTarget != nil {
		return nil, false, nil
	}

	// Acquire the session in read-write mode.
	sessionRuntimeMetadata.SessionRuntimeState.RwLocks += 1

	// Update the session.
	db.Sessions[sessionId] = sessionRuntimeMetadata

	// Return the session location.
	return &infrastructure.SessionLocation{
		Host:      "",
		SessionId: sessionId,
	}, true, nil
}
