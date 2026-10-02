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
		return mockReg, nil
	}

	b := NewWithRegister("testbeam", 9090, mockRegisterFunc)
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
	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on start, got nil")
	}
}

func TestBroadcaster_InterfaceFiltering(t *testing.T) {
	mockReg := &mockRegistrar{}
	var receivedIfaces []net.Interface

	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		receivedIfaces = ifaces
		return mockReg, nil
	}

	mockInterfacesFunc := func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
			{Index: 2, Name: "eth1", Flags: net.FlagMulticast},                    // Down
			{Index: 3, Name: "tun0", Flags: net.FlagUp},                           // No multicast
			{Index: 4, Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast}, // Loopback
			{Index: 5, Name: "wlan0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}

	b := NewWithRegister("filterbeam", 8080, mockRegisterFunc)
	b.SetInterfacesFunc(mockInterfacesFunc)

	err := b.Start()
	if err != nil {
		t.Fatalf("unexpected error starting broadcaster: %v", err)
	}

	if len(receivedIfaces) != 2 {
		t.Fatalf("expected 2 valid interfaces, got %d", len(receivedIfaces))
	}
	if receivedIfaces[0].Name != "eth0" {
		t.Errorf("expected first valid interface eth0, got %s", receivedIfaces[0].Name)
	}
	if receivedIfaces[1].Name != "wlan0" {
		t.Errorf("expected second valid interface wlan0, got %s", receivedIfaces[1].Name)
	}

	b.Stop()
	if !mockReg.shutdownCalled {
		t.Fatalf("expected Shutdown to be called on mock registrar")
	}
}

func TestBroadcaster_NoValidMulticastInterfaces(t *testing.T) {
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return &mockRegistrar{}, nil
	}

	mockInterfacesFunc := func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "eth1", Flags: net.FlagMulticast}, // Down
			{Index: 2, Name: "tun0", Flags: net.FlagUp},        // No multicast
			{Index: 3, Name: "lo", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},
		}, nil
	}

	b := NewWithRegister("nomanbeam", 8080, mockRegisterFunc)
	b.SetInterfacesFunc(mockInterfacesFunc)

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when no valid multicast interface exists, got nil")
	}
}

func TestBroadcaster_InterfacesFuncError(t *testing.T) {
	mockInterfacesFunc := func() ([]net.Interface, error) {
		return nil, errors.New("network interface list failed")
	}

	b := NewWithRegister("errbeam", 8080, nil)
	b.SetInterfacesFunc(mockInterfacesFunc)

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when interfaces function fails, got nil")
	}
}
