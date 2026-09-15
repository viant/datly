package application

import (
	"context"
	"errors"
	"fmt"
)

var ErrClosed = errors.New("application is shut down")

// Shutdown closes publication and admission, cancels and joins accepted async
// work and HTTP warmups across generations. A caller deadline bounds waiting,
// not cleanup: later calls join the same completion. Protocol servers still own
// transport shutdown and ordinary requests.
func (m *Manager) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("application shutdown context is required")
	}
	m.publication.Lock()
	if m.shutdownDone == nil {
		m.shutdownDone = make(chan struct{})
		m.stopped.Store(true)
		m.async.stop()
		m.observation.Stop()
		active := m.active.Load()
		externalPins := make([]func(), 0, len(m.externalPins))
		for _, release := range m.externalPins {
			externalPins = append(externalPins, release)
		}
		go func() {
			for _, release := range externalPins {
				release()
			}
			// Cancel both producers before joining either. Durable completion and
			// notification callbacks finish before shared exporter cleanup.
			warmErr := m.warmups.Shutdown(context.Background())
			m.async.wait()
			observationErr := m.observation.Shutdown(context.Background())
			m.shutdownErr = errors.Join(warmErr, observationErr)
			if active != nil {
				active.retire()
				<-active.closed
				m.shutdownErr = errors.Join(m.shutdownErr, active.closeErr)
			}
			close(m.shutdownDone)
		}()
	}
	done := m.shutdownDone
	m.publication.Unlock()
	select {
	case <-done:
		return m.shutdownErr
	default:
	}
	select {
	case <-done:
		return m.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}
