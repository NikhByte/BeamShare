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
		if ifaces == nil {
			t.Errorf("expected non-nil active interfaces slice, got nil")
		}
		if len(ifaces) != 1 || ifaces[0].Name != "eth0" {
			t.Errorf("expected [eth0], got %v", ifaces)
		}
		return mockReg, nil
	}

	b := NewWithRegister("testbeam", 9090, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}
	b.addrsFunc = func(iface net.Interface) ([]net.Addr, error) {
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

func TestBroadcaster_NoActiveInterfaces(t *testing.T) {
	b := NewWithRegister("testbeam", 9090, func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		return &mockRegistrar{}, nil
	})
	b.interfacesFunc = func() ([]net.Interface, error) {
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

	b := NewWithRegister("failbeam", 8080, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		}, nil
	}
	b.addrsFunc = func(iface net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.168.1.10"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on start, got nil")
	}
}

func TestBroadcaster_FilterActiveInterfaces(t *testing.T) {
	ifaces := []net.Interface{
		{Index: 1, Name: "down0", Flags: net.FlagMulticast},
		{Index: 2, Name: "lo0", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},
		{Index: 3, Name: "nomulti0", Flags: net.FlagUp},
		{Index: 4, Name: "unaddressed0", Flags: net.FlagUp | net.FlagMulticast},
		{Index: 5, Name: "unspecified0", Flags: net.FlagUp | net.FlagMulticast},
		{Index: 6, Name: "eth0", Flags: net.FlagUp | net.FlagMulticast},
		{Index: 7, Name: "wlan0", Flags: net.FlagUp | net.FlagMulticast},
	}

	addrsMap := map[string][]net.Addr{
		"down0": {
			&net.IPNet{IP: net.ParseIP("192.168.1.10"), Mask: net.CIDRMask(24, 32)},
		},
		"lo0": {
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		},
		"nomulti0": {
			&net.IPNet{IP: net.ParseIP("10.0.0.1"), Mask: net.CIDRMask(24, 32)},
		},
		"unaddressed0": {},
		"unspecified0": {
			&net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.CIDRMask(0, 32)},
		},
		"eth0": {
			&net.IPNet{IP: net.ParseIP("192.168.1.20"), Mask: net.CIDRMask(24, 32)},
		},
		"wlan0": {
			&net.IPAddr{IP: net.ParseIP("fe80::1")},
		},
	}

	mockAddrsFn := func(iface net.Interface) ([]net.Addr, error) {
		return addrsMap[iface.Name], nil
	}

	active := FilterActiveInterfaces(ifaces, mockAddrsFn)
	if len(active) != 2 {
		t.Fatalf("expected 2 active interfaces, got %d", len(active))
	}
	if active[0].Name != "eth0" || active[1].Name != "wlan0" {
		t.Fatalf("expected eth0 and wlan0, got %s and %s", active[0].Name, active[1].Name)
	}
}

func TestBroadcaster_StartNoActiveInterfaces(t *testing.T) {
	mockRegCalled := false
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		mockRegCalled = true
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("testbeam", 8080, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return []net.Interface{
			{Index: 1, Name: "down0", Flags: net.FlagMulticast},
			{Index: 2, Name: "lo0", Flags: net.FlagUp | net.FlagLoopback | net.FlagMulticast},
		}, nil
	}
	b.addrsFunc = func(iface net.Interface) ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
		}, nil
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error when no active multicast interfaces are present, got nil")
	}
	if mockRegCalled {
		t.Fatalf("expected registerFunc to not be called when no active interfaces exist")
	}
}

func TestBroadcaster_StartIfacesError(t *testing.T) {
	mockRegCalled := false
	mockRegisterFunc := func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
		mockRegCalled = true
		return &mockRegistrar{}, nil
	}

	b := NewWithRegister("testbeam", 8080, mockRegisterFunc)
	b.ifacesFunc = func() ([]net.Interface, error) {
		return nil, errors.New("network interface enumeration failed")
	}

	err := b.Start()
	if err == nil {
		t.Fatalf("expected error on interface enumeration failure, got nil")
	}
	if mockRegCalled {
		t.Fatalf("expected registerFunc to not be called on interface error")
	}
}
