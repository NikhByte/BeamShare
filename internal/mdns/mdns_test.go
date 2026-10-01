package mdns

import (
	"errors"
	"net"
	"strings"
	"testing"
)

type mockRegistrar struct {
	shutdownCalled bool
}

func (m *mockRegistrar) Shutdown() {
	m.shutdownCalled = true
}

func mockActiveInterface() ([]net.Interface, AddrsProvider) {
	iface := net.Interface{
		Index: 1,
		Name:  "eth0",
		Flags: net.FlagUp | net.FlagMulticast,
	}
	ifaces := []net.Interface{iface}
	addrs := func(i *net.Interface) ([]net.Addr, error) {
		if i.Name == "eth0" {
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.10")}}, nil
		}
		return nil, errors.New("unknown interface")
	}
	return ifaces, addrs
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

func TestBroadcaster_MockStartStop(t *testing.T) {
	mockReg := &mockRegistrar{}
	registered := false
	ifaces, addrsProvider := mockActiveInterface()

	mockRegisterFunc := func(instance, service, domain string, port int, text []string, passedIfaces []net.Interface) (Registrar, error) {
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
		if passedIfaces == nil {
			t.Errorf("expected non-nil passedIfaces slice")
		} else if len(passedIfaces) != 1 || passedIfaces[0].Name != "eth0" {
			t.Errorf("expected active interface eth0, got %v", passedIfaces)
		}
		return mockReg, nil
	}

	b := NewWithInterfaces("testbeam", 9090, mockRegisterFunc, func() ([]net.Interface, error) {
		return ifaces, nil
	}, addrsProvider)

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
	ifaces, addrsProvider := mockActiveInterface()

	mockRegisterFunc := func(instance, service, domain string, port int, text []string, passedIfaces []net.Interface) (Registrar, error) {
		return nil, expectedErr
	}

	b := NewWithInterfaces("failbeam", 8080, mockRegisterFunc, func() ([]net.Interface, error) {
		return ifaces, nil
	}, addrsProvider)

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on start, got nil")
	}
}

func TestBroadcaster_InterfaceFiltering(t *testing.T) {
	mockIfaces := []net.Interface{
		{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},                              // Valid
		{Index: 2, Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},             // Loopback (omit)
		{Index: 3, Name: "eth1", Flags: net.FlagMulticast},                                           // Down (omit)
		{Index: 4, Name: "tun0", Flags: net.FlagUp},                                                 // No Multicast (omit)
		{Index: 5, Name: "eth2", Flags: net.FlagUp | net.FlagMulticast},                              // No Addresses (omit)
		{Index: 6, Name: "eth3", Flags: net.FlagUp | net.FlagMulticast},                              // Addr Error (omit)
		{Index: 7, Name: "wlan0", Flags: net.FlagUp | net.FlagMulticast},                             // Valid
	}

	mockAddrs := func(iface *net.Interface) ([]net.Addr, error) {
		switch iface.Name {
		case "eth0":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.10")}}, nil
		case "lo":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("127.0.0.1")}}, nil
		case "eth1":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.11")}}, nil
		case "tun0":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("10.0.0.1")}}, nil
		case "eth2":
			return []net.Addr{}, nil
		case "eth3":
			return nil, errors.New("failed to read addrs")
		case "wlan0":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.50")}}, nil
		default:
			return nil, errors.New("unknown interface")
		}
	}

	var capturedIfaces []net.Interface
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, passedIfaces []net.Interface) (Registrar, error) {
		capturedIfaces = passedIfaces
		return &mockRegistrar{}, nil
	}

	b := NewWithInterfaces("filtertest", 8080, mockRegisterFunc, func() ([]net.Interface, error) {
		return mockIfaces, nil
	}, mockAddrs)

	err := b.Start()
	if err != nil {
		t.Fatalf("unexpected error starting broadcaster: %v", err)
	}

	if capturedIfaces == nil {
		t.Fatalf("expected non-nil capturedIfaces slice")
	}

	if len(capturedIfaces) != 2 {
		t.Fatalf("expected 2 active interfaces (eth0, wlan0), got %d (%v)", len(capturedIfaces), capturedIfaces)
	}

	expectedNames := map[string]bool{"eth0": true, "wlan0": true}
	for _, iface := range capturedIfaces {
		if !expectedNames[iface.Name] {
			t.Errorf("unexpected interface %s included in registration", iface.Name)
		}
	}
}

func TestBroadcaster_NoActiveInterfacesError(t *testing.T) {
	mockIfaces := []net.Interface{
		{Index: 1, Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},
		{Index: 2, Name: "eth1", Flags: net.FlagMulticast},
		{Index: 3, Name: "eth2", Flags: net.FlagUp | net.FlagMulticast},
	}

	mockAddrs := func(iface *net.Interface) ([]net.Addr, error) {
		if iface.Name == "eth2" {
			return []net.Addr{}, nil
		}
		return []net.Addr{&net.IPNet{IP: net.ParseIP("127.0.0.1")}}, nil
	}

	registerCalled := false
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, passedIfaces []net.Interface) (Registrar, error) {
		registerCalled = true
		return &mockRegistrar{}, nil
	}

	b := NewWithInterfaces("noifaces", 8080, mockRegisterFunc, func() ([]net.Interface, error) {
		return mockIfaces, nil
	}, mockAddrs)

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when no active multicast interfaces exist, got nil")
	}

	if !strings.Contains(err.Error(), "no active multicast network interfaces found") {
		t.Errorf("expected error message to contain 'no active multicast network interfaces found', got '%v'", err)
	}

	if registerCalled {
		t.Fatalf("registerFunc should not have been called when no active interfaces exist")
	}
}

func TestBroadcaster_GetInterfacesError(t *testing.T) {
	expectedErr := errors.New("network error")
	b := NewWithInterfaces("errtest", 8080, nil, func() ([]net.Interface, error) {
		return nil, expectedErr
	}, nil)

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when fetching interfaces fails, got nil")
	}
}
