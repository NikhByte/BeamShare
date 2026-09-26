package mdns

import (
	"errors"
	"net"
	"testing"
)

type mockRegistrar struct {
	shutdownCalled bool
}

func (m *mockRegistrar) Shutdown() {
	m.shutdownCalled = true
}

func TestBroadcaster_HostnameFormatting(t *testing.T) {
	b := New("My_Test Host", 8080)
	if b.Hostname() != "My_Test Host" {
		t.Fatalf("expected raw hostname 'My_Test Host', got '%s'", b.Hostname())
	}
	if b.LocalName() != "My_Test Host.local" {
		t.Fatalf("expected local name 'My_Test Host.local', got '%s'", b.LocalName())
	}

	bAuto := New("", 8080)
	if bAuto.Hostname() == "" {
		t.Fatalf("expected non-empty hostname for empty input")
	}
	if bAuto.LocalName() != bAuto.Hostname()+".local" {
		t.Fatalf("expected local name '%s.local', got '%s'", bAuto.Hostname(), bAuto.LocalName())
	}
}

func TestFilterInterfaces(t *testing.T) {
	testIfaces := []net.Interface{
		{
			Index: 1,
			Name:  "eth0",
			Flags: net.FlagUp | net.FlagMulticast,
		},
		{
			Index: 2,
			Name:  "lo",
			Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast,
		},
		{
			Index: 3,
			Name:  "eth1_down",
			Flags: net.FlagMulticast, // down (no FlagUp)
		},
		{
			Index: 4,
			Name:  "eth2_nomulticast",
			Flags: net.FlagUp, // no FlagMulticast
		},
		{
			Index: 5,
			Name:  "eth3_noip",
			Flags: net.FlagUp | net.FlagMulticast,
		},
		{
			Index: 6,
			Name:  "eth4_unspecifiedip",
			Flags: net.FlagUp | net.FlagMulticast,
		},
		{
			Index: 7,
			Name:  "wlan0",
			Flags: net.FlagUp | net.FlagMulticast,
		},
	}

	mockAddrs := map[string][]net.Addr{
		"eth0": {
			&net.IPNet{IP: net.ParseIP("192.168.1.50"), Mask: net.CIDRMask(24, 32)},
		},
		"lo": {
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		},
		"eth1_down": {
			&net.IPNet{IP: net.ParseIP("10.0.0.1"), Mask: net.CIDRMask(24, 32)},
		},
		"eth2_nomulticast": {
			&net.IPNet{IP: net.ParseIP("10.0.0.2"), Mask: net.CIDRMask(24, 32)},
		},
		"eth3_noip": {},
		"eth4_unspecifiedip": {
			&net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(0, 32)},
		},
		"wlan0": {
			&net.IPNet{IP: net.ParseIP("10.0.0.100"), Mask: net.CIDRMask(24, 32)},
		},
	}

	mockAddrsFunc := func(iface *net.Interface) ([]net.Addr, error) {
		addrs, ok := mockAddrs[iface.Name]
		if !ok {
			return nil, errors.New("unknown interface")
		}
		return addrs, nil
	}

	active := filterInterfaces(testIfaces, mockAddrsFunc)

	if len(active) != 2 {
		t.Fatalf("expected 2 active interfaces (eth0, wlan0), got %d", len(active))
	}

	if active[0].Name != "eth0" || active[1].Name != "wlan0" {
		t.Errorf("unexpected filtered interfaces: %v, %v", active[0].Name, active[1].Name)
	}
}

func TestBroadcaster_MockStartStop(t *testing.T) {
	mockReg := &mockRegistrar{}
	registered := false

	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		registered = true
		if service != ServiceType {
			t.Errorf("expected service type %s, got %s", ServiceType, service)
		}
		if domain != Domain {
			t.Errorf("expected domain %s, got %s", Domain, domain)
		}
		if port != 9090 {
			t.Errorf("expected port 9090, got %d", port)
		}
		if ifaces == nil {
			t.Errorf("expected non-nil ifaces slice passed to registerFunc")
		} else if len(ifaces) != 1 || ifaces[0].Name != "eth0" {
			t.Errorf("expected ifaces slice with [eth0], got %v", ifaces)
		}
		return mockReg, nil
	}

	b := NewWithRegister("testbeam", 9090, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}
	b.addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.168.1.10"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}

	err := b.Start()
	if err != nil {
		t.Fatalf("unexpected error starting broadcaster: %v", err)
	}
	if !registered {
		t.Fatalf("expected mock register func to be called")
	}

	b.Stop()
	if !mockReg.shutdownCalled {
		t.Fatalf("expected Shutdown to be called on mock registrar")
	}
}

func TestBroadcaster_MockRegisterError(t *testing.T) {
	expectedErr := errors.New("registration failed")
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return nil, expectedErr
	}

	b := NewWithRegister("failbeam", 8080, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}
	b.addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.168.1.10"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on start, got nil")
	}
}

func TestBroadcaster_NoActiveMulticastInterfaces(t *testing.T) {
	registerCalled := false
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		registerCalled = true
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("noifacebeam", 8080, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},
			{Index: 2, Name: "eth0_down", Flags: net.FlagMulticast},
		}, nil
	}
	b.addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		}, nil
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when no active multicast interfaces exist, got nil")
	}
	if !errors.Is(err, ErrNoMulticastInterfaces) {
		t.Errorf("expected ErrNoMulticastInterfaces, got %v", err)
	}
	if registerCalled {
		t.Fatalf("registerFunc should not be called when no active multicast interfaces exist")
	}
}

func TestBroadcaster_InterfacesError(t *testing.T) {
	registerCalled := false
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		registerCalled = true
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("errbeam", 8080, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return nil, errors.New("network failure")
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when net.Interfaces() fails, got nil")
	}
	if registerCalled {
		t.Fatalf("registerFunc should not be called when net.Interfaces() fails")
	}
}
