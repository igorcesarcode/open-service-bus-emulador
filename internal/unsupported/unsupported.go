// Package unsupported lists the Azure Service Bus features the emulator does
// not implement, so that each one is said so out loud: in the log when a
// configuration declares it, in the response when a client asks for it and in
// the portal, where its menu entry is marked.
package unsupported

import (
	"fmt"
	"log/slog"
)

// Feature is something Azure Service Bus has and the emulator does not.
type Feature struct {
	// ID is the value of the "unsupported" attribute of the log line.
	ID   string
	Name string
	// Why says what the feature is for and why the emulator leaves it out.
	Why string
}

// The features that are left out.
var (
	PrivateLink = Feature{"private-link", "Private Link (private endpoints)",
		"A private endpoint puts a namespace on a virtual network address. The emulator listens on the address you give it, and there is no network to join."}
	IPFilter = Feature{"ip-filter", "IP firewall rules",
		"IP rules limit which addresses reach a namespace. The emulator serves every client that can reach its port; restrict that with the host's firewall."}
	VNet = Feature{"vnet", "Virtual network rules and service endpoints",
		"Virtual network rules limit a namespace to subnets. The emulator has no virtual networks."}
	MigratePremium = Feature{"migrate-to-premium", "Migrate to premium",
		"A portal flow that copies a Standard namespace to a Premium one. There is one kind of namespace here, with the Premium features, and nothing to migrate."}
	Tasks = Feature{"tasks", "Tasks",
		"A portal list of guided Azure tasks with no effect on the data."}
	Diagnose = Feature{"diagnose-and-solve", "Diagnose and solve problems",
		"An Azure troubleshooting service. The activity log and the metrics pages show what the emulator knows."}
	Support = Feature{"support-request", "New support request",
		"Opens a ticket with Microsoft support, which does not exist for the emulator."}
	JMSClientScoped = Feature{"jms-client-scoped-subscriptions", "JMS client-scoped subscriptions",
		"Subscriptions affine to one JMS client id (IsClientAffine, ClientAffineProperties). No validating SDK uses them, so the emulator refuses to create one instead of pretending."}
)

// All lists the features, in the order of the portal's menu.
func All() []Feature {
	return []Feature{PrivateLink, IPFilter, VNet, MigratePremium, Tasks, Diagnose, Support, JMSClientScoped}
}

// Finding is a feature found in a configuration or a request.
type Finding struct {
	Feature Feature
	// Where says what declared it, such as a Terraform address or the name
	// of an entity.
	Where string
}

func (f Finding) String() string {
	return fmt.Sprintf("%s: %s is not supported and is ignored", f.Where, f.Feature.Name)
}

// Log writes a warning for each finding, with the feature in the
// "unsupported" attribute.
func Log(l *slog.Logger, source string, findings []Finding) {
	for _, f := range findings {
		l.Warn("unsupported feature ignored", "unsupported", f.Feature.ID, "where", f.Where, "source", source, "detail", f.Feature.Why)
	}
}

// LogRefused writes the warning for a request the emulator refused.
func LogRefused(l *slog.Logger, f Finding) {
	l.Warn("unsupported feature refused", "unsupported", f.Feature.ID, "where", f.Where, "detail", f.Feature.Why)
}
