package ratelimit

import (
	"sync"
	"testing"
)

func TestManager_SetGetRemove(t *testing.T) {
	m := &RateLimitManager{users: sync.Map{}}

	// Set
	ul := m.Set("user1@test", 1_000_000, 2_000_000)
	if ul == nil {
		t.Fatal("Set returned nil")
	}
	if ul.Email != "user1@test" {
		t.Errorf("expected email user1@test, got %s", ul.Email)
	}
	if ul.Egress == nil || ul.Egress.Rate() != 1_000_000 {
		t.Error("egress rate mismatch")
	}
	if ul.Ingress == nil || ul.Ingress.Rate() != 2_000_000 {
		t.Error("ingress rate mismatch")
	}

	// Get
	got := m.Get("user1@test")
	if got != ul {
		t.Error("Get returned different instance")
	}

	// Get non-existent
	if m.Get("nobody@test") != nil {
		t.Error("expected nil for non-existent user")
	}

	// Remove
	m.Remove("user1@test")
	if m.Get("user1@test") != nil {
		t.Error("expected nil after Remove")
	}
}

func TestManager_UpdateExisting(t *testing.T) {
	m := &RateLimitManager{users: sync.Map{}}

	m.Set("user@test", 1_000_000, 1_000_000)
	m.Set("user@test", 5_000_000, 3_000_000)

	ul := m.Get("user@test")
	if ul.Egress.Rate() != 5_000_000 {
		t.Errorf("expected updated egress 5M, got %d", ul.Egress.Rate())
	}
	if ul.Ingress.Rate() != 3_000_000 {
		t.Errorf("expected updated ingress 3M, got %d", ul.Ingress.Rate())
	}
}

func TestManager_SetZeroRemovesLimiter(t *testing.T) {
	m := &RateLimitManager{users: sync.Map{}}

	m.Set("user@test", 1_000_000, 1_000_000)
	// Update to 0 = remove limiter
	ul := m.Set("user@test", 0, 0)
	if ul != nil {
		t.Fatal("expected nil limiter when setting both directions to 0")
	}
	if m.Get("user@test") != nil {
		t.Fatal("expected limiter to be removed from manager")
	}
}

func TestManager_ListAll(t *testing.T) {
	m := &RateLimitManager{users: sync.Map{}}

	m.Set("a@test", 100, 200)
	m.Set("b@test", 300, 400)

	all := m.ListAll()
	if len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}

	emails := map[string]bool{}
	for _, info := range all {
		emails[info.Email] = true
	}
	if !emails["a@test"] || !emails["b@test"] {
		t.Error("missing expected emails in ListAll")
	}
}

func TestManager_ConcurrentSetGet(t *testing.T) {
	m := &RateLimitManager{users: sync.Map{}}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		email := "user" + string(rune('A'+i%26)) + "@test"
		go func() {
			defer wg.Done()
			m.Set(email, 1_000_000, 1_000_000)
		}()
		go func() {
			defer wg.Done()
			m.Get(email) // may or may not find it, just shouldn't panic
		}()
	}
	wg.Wait()
}

func TestGlobalManager_Singleton(t *testing.T) {
	// Global Manager should be non-nil
	if Manager == nil {
		t.Fatal("global Manager is nil")
	}

	// Basic operation on global
	Manager.Set("global-test@test", 1000, 2000)
	ul := Manager.Get("global-test@test")
	if ul == nil {
		t.Fatal("global Manager.Get returned nil after Set")
	}
	Manager.Remove("global-test@test")
}
