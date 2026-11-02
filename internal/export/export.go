// Package export writes the state of a server as the files that start an
// emulator with the same entities again: a Config.json and Terraform.
package export

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/igorcesarcode/open-service-bus-emulador/internal/config"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/server"
	"github.com/igorcesarcode/open-service-bus-emulador/internal/terraform"
)

// Files returns the Config.json and main.tf of the entities, rules, access
// policies, role assignments, locks, tags, encryption and Geo-DR pairings
// of every namespace of s. Messages are not exported. It is the
// server.Options.Export of the emulator.
func Files(s *server.Server) ([]server.ExportFile, error) {
	c := config.Snapshot(s)
	js, err := config.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("export: Config.json: %w", err)
	}
	return []server.ExportFile{
		{Name: "Config.json", Data: append(js, '\n')},
		{Name: "main.tf", Data: []byte(terraform.Export(c))},
	}, nil
}

// Write writes the files of s into dir, which it creates.
func Write(s *server.Server, dir string) error {
	files, err := Files(s)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		if err := os.WriteFile(filepath.Join(dir, f.Name), f.Data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
