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
		if ifaces == nil || len(ifaces) == 0 {
			t.Errorf("expected non-nil, non-empty ifaces list, got %v", ifaces)
		}
		return mockReg, nil
	}

	mockIfacesProvider := func() ([]net.Interface, error) {
		return []net.Interface{
			{Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}

	b := NewWithInterfaces("testbeam", 9090, mockIfacesProvider, mockRegisterFunc)
	b.addrsProvider = func(iface *net.Interface) ([]net.Addr, error) {
		return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.50")}}, nil
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

func TestBroadcaster_NoActiveInterfaces(t *testing.T) {
	b := NewWithRegister("testbeam", 9090, func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return &mockRegistrar{}, nil
	})
	b.ifacesProvider = func() ([]net.Interface, error) {
		return nil, errors.New("no active multicast network interfaces found")
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected Start() to fail when no active interfaces exist")
	}
}

func TestFilterInterfaces(t *testing.T) {
	lo := net.Interface{Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast}
	downIface := net.Interface{Name: "eth0", Flags: net.FlagMulticast} // not UP
	noMulticast := net.Interface{Name: "eth1", Flags: net.FlagUp}      // no multicast
	noAddrs := net.Interface{Name: "eth2", Flags: net.FlagUp | net.FlagMulticast}
	validIface := net.Interface{Name: "eth3", Flags: net.FlagUp | net.FlagMulticast}

	getAddrs := func(iface net.Interface) ([]net.Addr, error) {
		switch iface.Name {
		case "lo":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("127.0.0.1")}}, nil
		case "eth0":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.10")}}, nil
		case "eth1":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.11")}}, nil
		case "eth2":
			return nil, nil
		case "eth3":
			return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.13")}}, nil
		default:
			return nil, errors.New("unknown interface")
		}
	}

	ifaces := []net.Interface{lo, downIface, noMulticast, noAddrs, validIface}
	filtered, err := filterInterfaces(ifaces, getAddrs)
	if err != nil {
		t.Fatalf("unexpected error filtering interfaces: %v", err)
	}

	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered interface, got %d", len(filtered))
	}
	if filtered[0].Name != "eth3" {
		t.Fatalf("expected eth3, got %s", filtered[0].Name)
	}
}

func TestFilterInterfaces_NoActiveInterfaces(t *testing.T) {
	lo := net.Interface{Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast}
	getAddrs := func(iface net.Interface) ([]net.Addr, error) {
		return []net.Addr{&net.IPNet{IP: net.ParseIP("127.0.0.1")}}, nil
	}

	_, err := filterInterfaces([]net.Interface{lo}, getAddrs)
	if err == nil {
		t.Fatalf("expected error when no active interfaces exist, got nil")
	}
}

func TestBroadcaster_MockRegisterError(t *testing.T) {
	expectedErr := errors.New("registration failed")
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return nil, expectedErr
	}

	mockIfacesProvider := func() ([]net.Interface, error) {
		return []net.Interface{
			{Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}

	b := NewWithInterfaces("failbeam", 8080, mockIfacesProvider, mockRegisterFunc)
	b.addrsProvider = func(iface *net.Interface) ([]net.Addr, error) {
		return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.50")}}, nil
	}
	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on start, got nil")
	}
}

func TestBroadcaster_FilterInterfaces(t *testing.T) {
	mockReg := &mockRegistrar{}
	var passedIfaces []net.Interface

	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		passedIfaces = ifaces
		return mockReg, nil
	}

	mockIfacesProvider := func() ([]net.Interface, error) {
		return []net.Interface{
			{Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},                              // Valid -> include
			{Name: "eth1", Flags: net.FlagMulticast},                                          // Down -> skip
			{Name: "eth2", Flags: net.FlagUp},                                                 // No Multicast -> skip
			{Name: "lo", Flags: net.FlagUp | net.FlagMulticast | net.FlagLoopback},              // Loopback -> skip
			{Name: "wlan0", Flags: net.FlagUp | net.FlagMulticast | net.FlagBroadcast | net.FlagPointToPoint}, // Valid -> include
		}, nil
	}

	b := NewWithInterfaces("filtertest", 8080, mockIfacesProvider, mockRegisterFunc)
	b.addrsProvider = func(iface *net.Interface) ([]net.Addr, error) {
		return []net.Addr{&net.IPNet{IP: net.ParseIP("192.168.1.50")}}, nil
	}
	err := b.Start()
	if err != nil {
		t.Fatalf("unexpected error on start: %v", err)
	}

	if len(passedIfaces) != 2 {
		t.Fatalf("expected 2 active interfaces, got %d", len(passedIfaces))
	}
	if passedIfaces[0].Name != "eth0" || passedIfaces[1].Name != "wlan0" {
		t.Fatalf("expected interfaces ['eth0', 'wlan0'], got ['%s', '%s']", passedIfaces[0].Name, passedIfaces[1].Name)
	}
}

func TestBroadcaster_NoActiveMulticastInterfaces(t *testing.T) {
	registerCalled := false
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		registerCalled = true
		return &mockRegistrar{}, nil
	}

	mockIfacesProvider := func() ([]net.Interface, error) {
		return []net.Interface{
			{Name: "eth0", Flags: net.FlagUp},                                    // Missing FlagMulticast
			{Name: "eth1", Flags: net.FlagMulticast},                             // Missing FlagUp
			{Name: "lo", Flags: net.FlagUp | net.FlagMulticast | net.FlagLoopback}, // Loopback
		}, nil
	}

	b := NewWithInterfaces("nomanifest", 8080, mockIfacesProvider, mockRegisterFunc)
	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when no active multicast interfaces found, got nil")
	}
	if registerCalled {
		t.Fatalf("registerFunc should not be called when no active multicast interfaces are found")
	}
	if !strings.Contains(err.Error(), "no active multicast network interfaces found") {
		t.Fatalf("expected 'no active multicast network interfaces found' in error, got: %v", err)
	}
}

func TestBroadcaster_GetInterfacesError(t *testing.T) {
	mockIfacesProvider := func() ([]net.Interface, error) {
		return nil, errors.New("network uninitialized")
	}

	b := NewWithInterfaces("errbeam", 8080, mockIfacesProvider, nil)
	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on interface enumeration failure, got nil")
	}
	if !strings.Contains(err.Error(), "failed to get network interfaces") {
		t.Fatalf("expected 'failed to get network interfaces' in error, got: %v", err)
	}
}
