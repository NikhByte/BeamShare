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

func mockActiveInterface(name string) net.Interface {
	return net.Interface{
		Index:        1,
		MTU:          1500,
		Name:         name,
		HardwareAddr: net.HardwareAddr{0x00, 0x11, 0x22, 0x33, 0x44, 0x55},
		Flags:        net.FlagUp | net.FlagMulticast,
	}
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
		if len(ifaces) != 1 || ifaces[0].Name != "eth0" {
			t.Errorf("expected 1 interface eth0, got %v", ifaces)
		}
		return mockReg, nil
	}

	b := NewWithRegister("testbeam", 9090, mockRegisterFunc)
	b.interfacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{mockActiveInterface("eth0")}, nil
	}
	b.addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
		_, ipNet, _ := net.ParseCIDR("192.168.1.100/24")
		return []net.Addr{ipNet}, nil
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
	b.interfacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{mockActiveInterface("eth0")}, nil
	}
	b.addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
		_, ipNet, _ := net.ParseCIDR("192.168.1.100/24")
		return []net.Addr{ipNet}, nil
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on start, got nil")
	}
}

func TestBroadcaster_InterfaceFiltering(t *testing.T) {
	ifaces := []net.Interface{
		{Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},                              // Active, multicast, valid IPv4 -> KEEP
		{Name: "eth1", Flags: net.FlagMulticast},                                           // Down -> SKIP
		{Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},             // Loopback -> SKIP
		{Name: "tun0", Flags: net.FlagUp},                                                 // Non-multicast -> SKIP
		{Name: "eth2", Flags: net.FlagUp | net.FlagMulticast},                              // No IP addresses -> SKIP
		{Name: "eth3", Flags: net.FlagUp | net.FlagMulticast},                              // Unspecified IP -> SKIP
		{Name: "eth4", Flags: net.FlagUp | net.FlagMulticast},                              // Valid IPv6 -> KEEP
	}

	addrsMap := map[string][]net.Addr{
		"eth0": {&net.IPNet{IP: net.ParseIP("192.168.1.50"), Mask: net.CIDRMask(24, 32)}},
		"eth1": {&net.IPNet{IP: net.ParseIP("192.168.1.51"), Mask: net.CIDRMask(24, 32)}},
		"lo":   {&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)}},
		"tun0": {&net.IPNet{IP: net.ParseIP("10.0.0.1"), Mask: net.CIDRMask(24, 32)}},
		"eth2": {},
		"eth3": {&net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(32, 32)}},
		"eth4": {&net.IPNet{IP: net.ParseIP("fe80::1"), Mask: net.CIDRMask(64, 128)}},
	}

	var passedIfaces []net.Interface
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		passedIfaces = ifaces
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("testfilter", 8080, mockRegisterFunc)
	b.interfacesFunc = func() ([]net.Interface, error) {
		return ifaces, nil
	}
	b.addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
		return addrsMap[iface.Name], nil
	}

	err := b.Start()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(passedIfaces) != 2 {
		t.Fatalf("expected 2 filtered interfaces, got %d", len(passedIfaces))
	}
	if passedIfaces[0].Name != "eth0" || passedIfaces[1].Name != "eth4" {
		t.Fatalf("expected interfaces [eth0, eth4], got [%s, %s]", passedIfaces[0].Name, passedIfaces[1].Name)
	}
}

func TestBroadcaster_NoActiveMulticastInterfaces(t *testing.T) {
	ifaces := []net.Interface{
		{Name: "eth1", Flags: net.FlagMulticast},                               // Down
		{Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast}, // Loopback
	}

	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("noiface", 8080, mockRegisterFunc)
	b.interfacesFunc = func() ([]net.Interface, error) {
		return ifaces, nil
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when no active multicast interfaces found, got nil")
	}
	if !strings.Contains(err.Error(), "no active multicast interfaces found") {
		t.Fatalf("expected error message to contain 'no active multicast interfaces found', got '%v'", err)
	}
}

func TestBroadcaster_InterfaceGetError(t *testing.T) {
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("erriface", 8080, mockRegisterFunc)
	b.interfacesFunc = func() ([]net.Interface, error) {
		return nil, errors.New("network down")
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on interface query failure, got nil")
	}
	if !strings.Contains(err.Error(), "get network interfaces") {
		t.Fatalf("expected error to contain 'get network interfaces', got '%v'", err)
	}
}

