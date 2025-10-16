package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"encoding/json"

	"github.com/redis/go-redis/v9"

	"ytmp3api/internal/models"
)

type SessionStore interface {
	CreateSession(ctx context.Context, s *models.ConversionSession) error
	UpdateSession(ctx context.Context, s *models.ConversionSession) error
	GetSession(ctx context.Context, id string) (*models.ConversionSession, error)
	DeleteSession(ctx context.Context, id string) error
	FindByURL(ctx context.Context, url string) (string, bool, error)
	SetURLMap(ctx context.Context, url, id string) error
}

var ErrNotFound = errors.New("not found")

// MemoryStore implements in-memory sessions with URL deduplication.
type MemoryStore struct {
	mu        sync.RWMutex
	sessions  map[string]*models.ConversionSession
	urlToID   map[string]string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{sessions: make(map[string]*models.ConversionSession), urlToID: make(map[string]string)}
}

func (m *MemoryStore) CreateSession(ctx context.Context, s *models.ConversionSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[s.ID]; ok {
		return errors.New("session exists")
	}
	s.CreatedAt = time.Now().UTC()
	s.UpdatedAt = s.CreatedAt
	m.sessions[s.ID] = s
	return nil
}

func (m *MemoryStore) UpdateSession(ctx context.Context, s *models.ConversionSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[s.ID]; !ok {
		return ErrNotFound
	}
	s.UpdatedAt = time.Now().UTC()
	m.sessions[s.ID] = s
	return nil
}

func (m *MemoryStore) GetSession(ctx context.Context, id string) (*models.ConversionSession, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	if !ok {
		return nil, ErrNotFound
	}
	copy := *s
	return &copy, nil
}

func (m *MemoryStore) DeleteSession(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, id)
	for u, sid := range m.urlToID {
		if sid == id {
			delete(m.urlToID, u)
		}
	}
	return nil
}

func (m *MemoryStore) FindByURL(ctx context.Context, url string) (string, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sid, ok := m.urlToID[url]
	return sid, ok, nil
}

func (m *MemoryStore) SetURLMap(ctx context.Context, url, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.urlToID[url] = id
	return nil
}

// RedisStore implements SessionStore on Redis.
type RedisStore struct {
	rdb *redis.Client
}

func NewRedisStore(rdb *redis.Client) *RedisStore { return &RedisStore{rdb: rdb} }

func (r *RedisStore) sessionKey(id string) string { return "session:" + id }
func (r *RedisStore) urlKey(url string) string { return "url:" + url }

func (r *RedisStore) CreateSession(ctx context.Context, s *models.ConversionSession) error {
	b, err := json.Marshal(s)
	if err != nil { return err }
	return r.rdb.Set(ctx, r.sessionKey(s.ID), b, 0).Err()
}
func (r *RedisStore) UpdateSession(ctx context.Context, s *models.ConversionSession) error {
	s.UpdatedAt = time.Now().UTC()
	b, err := json.Marshal(s)
	if err != nil { return err }
	return r.rdb.Set(ctx, r.sessionKey(s.ID), b, 0).Err()
}
func (r *RedisStore) GetSession(ctx context.Context, id string) (*models.ConversionSession, error) {
	res, err := r.rdb.Get(ctx, r.sessionKey(id)).Bytes()
	if err != nil {
		if err == redis.Nil { return nil, ErrNotFound }
		return nil, err
	}
	var s models.ConversionSession
	if err := json.Unmarshal(res, &s); err != nil { return nil, err }
	return &s, nil
}
func (r *RedisStore) DeleteSession(ctx context.Context, id string) error {
	return r.rdb.Del(ctx, r.sessionKey(id)).Err()
}
func (r *RedisStore) FindByURL(ctx context.Context, url string) (string, bool, error) {
	id, err := r.rdb.Get(ctx, r.urlKey(url)).Result()
	if err != nil {
		if err == redis.Nil { return "", false, nil }
		return "", false, err
	}
	return id, true, nil
}
func (r *RedisStore) SetURLMap(ctx context.Context, url, id string) error {
	return r.rdb.Set(ctx, r.urlKey(url), id, 24*time.Hour).Err()
}
