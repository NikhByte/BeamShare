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

// InterfacesProvider abstracts retrieving system network interfaces.
type InterfacesProvider func() ([]net.Interface, error)

// AddrsProvider abstracts retrieving network addresses for an interface.
type AddrsProvider func(iface *net.Interface) ([]net.Addr, error)

// Broadcaster advertises a Beam session over mDNS / Bonjour.
type Broadcaster struct {
	hostname     string
	port         int
	server       Registrar
	registerFunc RegisterFunc
	ifacesFunc   InterfacesProvider
	addrsFunc    AddrsProvider
}

// New creates a Broadcaster. The hostname will become "<hostname>.local"
// on the network. If hostname is empty, the machine's OS hostname is used.
func New(hostname string, port int) *Broadcaster {
	return NewWithRegister(hostname, port, defaultRegister)
}

// NewWithRegister creates a Broadcaster with a custom RegisterFunc (for mocking/testing).
func NewWithRegister(hostname string, port int, registerFunc RegisterFunc) *Broadcaster {
	return NewWithInterfaces(hostname, port, registerFunc, nil, nil)
}

// NewWithInterfaces creates a Broadcaster with custom RegisterFunc and network interface providers (for mocking/testing).
func NewWithInterfaces(hostname string, port int, registerFunc RegisterFunc, ifacesFunc InterfacesProvider, addrsFunc AddrsProvider) *Broadcaster {
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
	if ifacesFunc == nil {
		ifacesFunc = net.Interfaces
	}
	if addrsFunc == nil {
		addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
			return iface.Addrs()
		}
	}
	return &Broadcaster{
		hostname:     hostname,
		port:         port,
		registerFunc: registerFunc,
		ifacesFunc:   ifacesFunc,
		addrsFunc:    addrsFunc,
	}
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
	ifacesFunc := b.ifacesFunc
	if ifacesFunc == nil {
		ifacesFunc = net.Interfaces
	}
	addrsFunc := b.addrsFunc
	if addrsFunc == nil {
		addrsFunc = func(iface *net.Interface) ([]net.Addr, error) {
			return iface.Addrs()
		}
	}

	ifaces, err := ifacesFunc()
	if err != nil {
		return fmt.Errorf("mdns get interfaces: %w", err)
	}

	activeIfaces := make([]net.Interface, 0, len(ifaces))
	for _, iface := range ifaces {
		// Must be active (up)
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		// Must not be loopback
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// Must support multicast
		if iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		// Must have assigned addresses
		addrs, err := addrsFunc(&iface)
		if err != nil || len(addrs) == 0 {
			continue
		}

		activeIfaces = append(activeIfaces, iface)
	}

	if len(activeIfaces) == 0 {
		return fmt.Errorf("no active multicast network interfaces found")
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
