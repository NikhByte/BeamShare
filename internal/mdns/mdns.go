// Package mdns implements mDNS (multicast DNS) broadcasting so that any
// device on the same LAN can discover the Beam sender by hostname
// (e.g. "beamshare.local") instead of typing a numeric IP address.
//
// Uses github.com/grandcat/zeroconf under the hood, which speaks the
// standard DNS-SD / Bonjour protocol understood by macOS, iOS, Android,
// and most Linux distros with avahi.
package mdns

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/grandcat/zeroconf"
)

// ServiceType is the DNS-SD service type for the Beam HTTP server.
const ServiceType = "_beam._tcp"

// Domain is the mDNS search domain.
const Domain = "local."

// Registrar abstracts the mDNS server shutdown mechanism.
type Registrar interface {
	Shutdown()
}

// RegisterFunc abstracts the zeroconf registration function.
type RegisterFunc func(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error)

// Broadcaster advertises a Beam session over mDNS / Bonjour.
type Broadcaster struct {
	hostname     string
	port         int
	server       Registrar
	registerFunc RegisterFunc
	ifacesFunc   func() ([]net.Interface, error)
	addrsFunc    func(net.Interface) ([]net.Addr, error)
}

// New creates a Broadcaster. The hostname will become "<hostname>.local"
// on the network. If hostname is empty, the machine's OS hostname is used.
func New(hostname string, port int) *Broadcaster {
	return NewWithRegister(hostname, port, defaultRegister)
}

// NewWithRegister creates a Broadcaster with a custom RegisterFunc (for mocking/testing).
func NewWithRegister(hostname string, port int, registerFunc RegisterFunc) *Broadcaster {
	if hostname == "" {
		h, err := os.Hostname()
		if err != nil {
			h = "beamshare"
		}
		// Sanitise: lowercase, replace spaces/underscores with hyphens.
		h = strings.ToLower(h)
		h = strings.ReplaceAll(h, " ", "-")
		h = strings.ReplaceAll(h, "_", "-")
		hostname = h
	}
	if registerFunc == nil {
		registerFunc = defaultRegister
	}
	return &Broadcaster{
		hostname:     hostname,
		port:         port,
		registerFunc: registerFunc,
		ifacesFunc:   net.Interfaces,
		addrsFunc:    defaultIfaceAddrs,
	}
}

func defaultIfaceAddrs(iface net.Interface) ([]net.Addr, error) {
	return iface.Addrs()
}

// FilterActiveInterfaces filters network interfaces for active status (FlagUp),
// multicast capability (FlagMulticast), non-loopback flags (FlagLoopback == 0),
// and valid IP address assignment.
func FilterActiveInterfaces(ifaces []net.Interface, addrsFn func(net.Interface) ([]net.Addr, error)) []net.Interface {
	if addrsFn == nil {
		addrsFn = defaultIfaceAddrs
	}
	var active []net.Interface
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, err := addrsFn(iface)
		if err != nil || len(addrs) == 0 {
			continue
		}
		hasValidIP := false
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() {
				hasValidIP = true
				break
			}
		}
		if hasValidIP {
			active = append(active, iface)
		}
	}
	return active
}

func defaultRegister(instance, service, domain string, port int, text []string, ifaces []net.Interface) (Registrar, error) {
	srv, err := zeroconf.Register(instance, service, domain, port, text, ifaces)
	if err != nil {
		return nil, err
	}
	return srv, nil
}

// Hostname returns the sanitised hostname that will appear as "<name>.local".
func (b *Broadcaster) Hostname() string { return b.hostname }

// LocalName returns the full mDNS name (e.g. "beamshare.local").
func (b *Broadcaster) LocalName() string { return b.hostname + ".local" }

// Start begins advertising the Beam service via mDNS.
// It is non-blocking; call Stop() to deregister.
func (b *Broadcaster) Start() error {
	ifacesFn := b.ifacesFunc
	if ifacesFn == nil {
		ifacesFn = net.Interfaces
	}
	addrsFn := b.addrsFunc
	if addrsFn == nil {
		addrsFn = defaultIfaceAddrs
	}

	ifaces, err := ifacesFn()
	if err != nil {
		return fmt.Errorf("mdns register: %w", err)
	}

	activeIfaces := FilterActiveInterfaces(ifaces, addrsFn)
	if len(activeIfaces) == 0 {
		return fmt.Errorf("mdns register: no active multicast network interfaces available")
	}

	// TXT record: clients can read the Beam version from it.
	txtRecords := []string{"v=beam/0.2"}

	regFunc := b.registerFunc
	if regFunc == nil {
		regFunc = defaultRegister
	}

	srv, err := regFunc(
		"Beam — "+b.hostname, // Instance name shown in mDNS browsers
		ServiceType,
		Domain,
		b.port,
		txtRecords,
		activeIfaces,
	)
	if err != nil {
		return fmt.Errorf("mdns register: %w", err)
	}
	b.server = srv
	return nil
}

// Stop deregisters the mDNS service.
func (b *Broadcaster) Stop() {
	if b.server != nil {
		b.server.Shutdown()
		b.server = nil
	}
}
