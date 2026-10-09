package sqlite

import (
	"context"
	"fmt"
	"strings"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/config"
)

// SqliteBoundaryProvisioner is the SQLite adapter for the boundary
// provisioning slice. It opens and migrates both files, installs the writer
// queue, then publishes the pools to every adapter through one registry.
type SqliteBoundaryProvisioner struct {
	config   config.SqliteConfig
	admin    config.AdminConfig
	registry *BoundaryRegistry
	saver    *SqliteSaveEvents
}

func NewSqliteBoundaryProvisioner(
	sqliteCfg config.SqliteConfig,
	adminCfg config.AdminConfig,
	registry *BoundaryRegistry,
	saver *SqliteSaveEvents,
) *SqliteBoundaryProvisioner {
	return &SqliteBoundaryProvisioner{config: sqliteCfg, admin: adminCfg, registry: registry, saver: saver}
}

func (p *SqliteBoundaryProvisioner) ProvisionBoundary(ctx context.Context, definition boundarymodel.Definition) error {
	if p == nil || p.registry == nil || p.saver == nil {
		return fmt.Errorf("sqlite boundary provisioner is not configured")
	}
	if err := p.validateDefinition(definition); err != nil {
		return err
	}

	p.registry.provisionMu.Lock()
	defer p.registry.provisionMu.Unlock()
	if _, ok := p.registry.eventPool(definition.Name); ok {
		return nil
	}

	eventPool, err := OpenBoundaryPoolsWithConfig(ctx, p.config, definition.Name)
	if err != nil {
		return fmt.Errorf("provision SQLite boundary %s: %w", definition.Name, err)
	}
	defer eventPool.Close()
	metadataPool, err := OpenMetadataPoolsWithConfig(ctx, p.config, definition.Name)
	if err != nil {
		return fmt.Errorf("provision SQLite metadata for %s: %w", definition.Name, err)
	}
	defer metadataPool.Close()
	return nil
}

func (p *SqliteBoundaryProvisioner) InstallBoundary(ctx context.Context, definition boundarymodel.Definition) error {
	if p == nil || p.registry == nil || p.saver == nil {
		return fmt.Errorf("sqlite boundary installer is not configured")
	}
	if err := p.validateDefinition(definition); err != nil {
		return err
	}

	p.registry.provisionMu.Lock()
	defer p.registry.provisionMu.Unlock()
	if _, ok := p.registry.eventPool(definition.Name); ok {
		return nil
	}

	eventPool, err := OpenBoundaryPoolsWithConfig(ctx, p.config, definition.Name)
	if err != nil {
		return fmt.Errorf("open SQLite boundary %s: %w", definition.Name, err)
	}
	metadataPool, err := OpenMetadataPoolsWithConfig(ctx, p.config, definition.Name)
	if err != nil {
		_ = eventPool.Close()
		return fmt.Errorf("open SQLite metadata for %s: %w", definition.Name, err)
	}

	// Keep readers out until the corresponding group-commit worker exists.
	p.registry.mu.Lock()
	if err := p.saver.addBoundary(definition.Name, eventPool); err != nil {
		p.registry.mu.Unlock()
		_ = metadataPool.Close()
		_ = eventPool.Close()
		return fmt.Errorf("start SQLite writer for %s: %w", definition.Name, err)
	}
	p.registry.pools[definition.Name] = eventPool
	p.registry.metadataPools[definition.Name] = metadataPool
	p.registry.mu.Unlock()
	return nil
}

func (p *SqliteBoundaryProvisioner) validateDefinition(definition boundarymodel.Definition) error {
	if !strings.EqualFold(strings.TrimSpace(definition.Placement.Backend), "sqlite") {
		return fmt.Errorf("boundary %s uses unsupported backend %q", definition.Name, definition.Placement.Backend)
	}
	if err := validateIdentifier(definition.Name); err != nil {
		return fmt.Errorf("invalid boundary name %s: %w", definition.Name, err)
	}
	namespace := strings.TrimSpace(definition.Placement.Namespace)
	if namespace != definition.Name {
		return fmt.Errorf("SQLite boundary %s must use namespace %q", definition.Name, definition.Name)
	}
	return nil
}
