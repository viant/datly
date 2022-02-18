package config

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/datly/v0/shared"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v2"
	"path"
	"time"
)

//Notify represent notify function
type Notify func(ctx context.Context, fs afs.Service, URL string) error

//Loader represents URL changes notifier
type Loader struct {
	fs             afs.Service
	baseURL        string
	rules          *Resources
	checkFrequency time.Duration
	nextCheck      time.Time
	onChange       Notify
	onRemove       Notify
}

func (m *Loader) isCheckDue(now time.Time) bool {
	if m.nextCheck.IsZero() || now.After(m.nextCheck) {
		m.nextCheck = now.Add(m.checkFrequency)
		return true
	}
	return false
}

func (m *Loader) notify(ctx context.Context, currentSnapshot []storage.Object) (notified bool, err error) {
	snapshot := indexResources(currentSnapshot)

	for URL, lastModified := range snapshot {
		modTime := m.rules.Get(URL)
		if modTime == nil {
			if e := m.onChange(ctx, m.fs, URL); e != nil {
				err = e
				continue
			}
			notified = true
			m.rules.Add(URL, lastModified)
			continue
		}
		if !modTime.Equal(lastModified) {
			notified = true
			if e := m.onChange(ctx, m.fs, URL); e != nil {
				err = e
			}
		}
	}
	removed := m.rules.GetMissing(snapshot)
	for _, URL := range removed {
		notified = true
		_ = m.onRemove(ctx, m.fs, URL)
		m.rules.Remove(URL)
	}
	return notified, err
}

//Notify notifies any rule changes
func (m *Loader) Notify(ctx context.Context, fs afs.Service) (bool, error) {
	if m.baseURL == "" {
		return false, nil
	}
	if !m.isCheckDue(time.Now()) {
		return false, nil
	}
	rules, err := fs.List(ctx, m.baseURL, option.NewRecursive(true))
	if err != nil {
		return false, err
	}
	return m.notify(ctx, rules)
}

//NewLoader create a loader
func NewLoader(baeURL string, checkFrequency time.Duration, fs afs.Service, onChanged, onRemoved Notify) *Loader {
	if checkFrequency == 0 {
		checkFrequency = time.Minute
	}
	return &Loader{
		fs:             fs,
		onChange:       onChanged,
		onRemove:       onRemoved,
		checkFrequency: checkFrequency,
		baseURL:        baeURL,
		rules:          NewResources(),
	}
}

func indexResources(objects []storage.Object) map[string]time.Time {
	var indexed = make(map[string]time.Time)
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		ext := path.Ext(object.Name())
		if ext == shared.JSONExt || ext == shared.YAMLExt {
			indexed[object.URL()] = object.ModTime()
		}
	}
	return indexed
}

func loadTarget(data []byte, ext string, provider func() interface{}, onLoaded func(target interface{}) error) error {
	target := provider()
	switch ext {
	case shared.YAMLExt:
		ruleMap := map[string]interface{}{}
		err := yaml.Unmarshal(data, &ruleMap)
		if err != nil {
			return err
		}
		if err := toolbox.DefaultConverter.AssignConverted(target, ruleMap); err != nil {
			return err
		}
	default:
		if err := json.Unmarshal(data, target); err != nil {
			return err
		}
	}
	return onLoaded(target)
}
