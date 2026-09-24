package breakout

import "sync"

type Room struct { ID string; Name string; Participants map[string]string }
type Manager struct { mu sync.RWMutex; rooms map[string]*Room }

func NewManager() *Manager { return &Manager{rooms: map[string]*Room{}} }
func (m *Manager) Create(id,name string) *Room { m.mu.Lock(); defer m.mu.Unlock(); r:=&Room{ID:id,Name:name,Participants:map[string]string{}}; m.rooms[id]=r; return r }
func (m *Manager) Assign(roomID,userID string) bool { m.mu.Lock(); defer m.mu.Unlock(); r:=m.rooms[roomID]; if r==nil{return false}; r.Participants[userID]=roomID; return true }
func (m *Manager) Remove(roomID,userID string) bool { m.mu.Lock(); defer m.mu.Unlock(); r:=m.rooms[roomID]; if r==nil{return false}; if _,ok:=r.Participants[userID];!ok{return false}; delete(r.Participants,userID); return true }
func (m *Manager) Get(roomID string) *Room { m.mu.RLock(); defer m.mu.RUnlock(); return m.rooms[roomID] }
