package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
)

// Test fixture locations use physical platform paths. Production deliberately
// does not EvalSymlinks on a configured path; aliases require an explicit handle.
func staticPhysicalDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}
func staticRootFile(t *testing.T, root, name, body string) {
	t.Helper()
	file := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}
func staticRootConfig(location string) *config.Config {
	return &config.Config{Config: gateway.Config{StaticContent: []*spec.StaticContent{{Path: "/site", ContentURL: location}}}}
}
func staticRootResponse(t *testing.T, s *Server, want string) {
	t.Helper()
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/site/", nil))
	if w.Code != 200 || w.Body.String() != want {
		t.Fatalf("HTTP %d %q, want %q", w.Code, w.Body.String(), want)
	}
}

func TestStaticAuthorityEveryComponent(t *testing.T) {
	ctx := context.Background()
	base, outside := staticPhysicalDir(t), staticPhysicalDir(t)
	staticRootFile(t, base, "normal/site/index.html", "normal")
	staticRootFile(t, outside, "parent/site/index.html", "secret")
	for name, target := range map[string]string{"leaf": filepath.Join(outside, "parent/site"), "ancestor": filepath.Join(outside, "parent"), "inside": filepath.Join(base, "normal")} {
		if err := os.Symlink(target, filepath.Join(base, name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, selected := range []string{"leaf", "leaf/", "ancestor/site", "ancestor/./site", "inside/site", "ancestor/../normal/site"} {
		for _, mode := range []string{"direct", "file", "localhost", "global", "global_file", "route_relative", "root_relative", "loader_relative", "loader_global"} {
			t.Run(selected+"/"+mode, func(t *testing.T) {
				// Preserve dot components: filepath.Join would erase the adversarial input.
				local := base + "/" + selected
				cfg := staticRootConfig(local)
				switch mode {
				case "file", "localhost":
					u := url.URL{Scheme: "file", Path: local}
					if mode == "localhost" {
						u.Host = "localhost"
					}
					cfg.StaticContent[0].ContentURL = u.String()
				case "global":
					cfg.ContentURL = local
					cfg.StaticContent[0].ContentURL = "."
				case "global_file":
					cfg.ContentURL = (&url.URL{Scheme: "file", Path: local}).String()
					cfg.StaticContent[0].ContentURL = "."
				case "route_relative":
					cfg.ContentURL = base
					cfg.StaticContent[0].ContentURL = selected
				case "root_relative":
					cfg.StaticContent[0].ContentURL = base
					cfg.StaticContent[0].Root = selected
				case "loader_relative", "loader_global":
					doc := map[string]any{"StaticContent": []any{map[string]string{"Path": "/site", "ContentURL": selected}}}
					if mode == "loader_global" {
						doc["ContentURL"] = selected
						doc["StaticContent"] = []any{map[string]string{"Path": "/site", "ContentURL": "."}}
					}
					data, err := json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
					file := filepath.Join(base, "config.json")
					if err = os.WriteFile(file, data, 0600); err != nil {
						t.Fatal(err)
					}
					cfg, err = (config.Loader{}).Load(ctx, file)
					if err != nil {
						t.Fatal(err)
					}
				}
				server, err := New(ctx, Options{Config: cfg})
				if err != nil {
					t.Fatal(err)
				}
				defer server.Shutdown(ctx)
				if err = server.Reload(ctx, 1); err == nil {
					t.Fatal("symlink-selected authority was published")
				}
			})
		}
	}
}

func TestStaticLocalNormalAndVolumeRoots(t *testing.T) {
	ctx := context.Background()
	base := staticPhysicalDir(t)
	staticRootFile(t, base, "site with space/index.html", "normal")
	location := filepath.Join(base, "site with space")
	for _, mode := range []string{"direct", "file", "localhost", "global", "global_file", "volume", "file_volume", "localhost_volume", "loader_relative", "cwd_relative", "trusted_root", "loader_trusted_root"} {
		t.Run(mode, func(t *testing.T) {
			cfg := staticRootConfig(location)
			switch mode {
			case "file", "localhost":
				u := url.URL{Scheme: "file", Path: location}
				if mode == "localhost" {
					u.Host = "localhost"
				}
				cfg.StaticContent[0].ContentURL = u.String()
			case "global", "global_file":
				cfg.ContentURL = base
				cfg.StaticContent[0].ContentURL = "site with space"
				if mode == "global_file" {
					cfg.ContentURL = (&url.URL{Scheme: "file", Path: base}).String()
				}
			case "volume", "file_volume", "localhost_volume":
				volume := filepath.VolumeName(base) + string(os.PathSeparator)
				cfg.StaticContent[0].ContentURL = volume
				cfg.StaticContent[0].Root = filepath.ToSlash(strings.TrimPrefix(location, volume))
				if mode != "volume" {
					u := url.URL{Scheme: "file", Path: volume}
					if mode == "localhost_volume" {
						u.Host = "localhost"
					}
					cfg.StaticContent[0].ContentURL = u.String()
				}
			case "cwd_relative":
				t.Chdir(base)
				cfg.StaticContent[0].ContentURL = "./site with space"
			case "trusted_root", "loader_trusted_root":
				root, err := os.OpenRoot(base)
				if err != nil {
					t.Fatal(err)
				}
				defer root.Close()
				cfg.StaticLocalRoot = root
				cfg.StaticContent[0].ContentURL = "site with space"
			}
			if mode == "loader_relative" || mode == "loader_trusted_root" {
				file := filepath.Join(base, "config.json")
				if err := os.WriteFile(file, []byte(`{"StaticContent":[{"Path":"/site","ContentURL":"site with space"}]}`), 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				cfg, err = (config.Loader{StaticLocalRoot: cfg.StaticLocalRoot}).Load(ctx, file)
				if err != nil {
					t.Fatal(err)
				}
			}
			s, err := New(ctx, Options{Config: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Shutdown(ctx)
			if err = s.Reload(ctx, 1); err != nil {
				t.Fatal(err)
			}
			staticRootResponse(t, s, "normal")
		})
	}
}

func TestStaticEmptyFileAuthorityNeverSelectsCWD(t *testing.T) {
	ctx := context.Background()
	cwd := staticPhysicalDir(t)
	staticRootFile(t, cwd, "index.html", "cwd-secret")
	t.Chdir(cwd)
	for _, location := range []string{"file:", "file://", "file://localhost"} {
		for _, global := range []bool{false, true} {
			t.Run(location+map[bool]string{true: "global", false: "direct"}[global], func(t *testing.T) {
				cfg := staticRootConfig(location)
				if global {
					cfg.ContentURL = location
					cfg.StaticContent[0].ContentURL = "."
				}
				s, err := New(ctx, Options{Config: cfg})
				if err != nil {
					t.Fatal(err)
				}
				defer s.Shutdown(ctx)
				if err = s.Reload(ctx, 1); err == nil {
					t.Fatal("empty file URL published cwd")
				}
				routeURL, globalURL := location, ""
				if global {
					routeURL, globalURL = ".", location
				}
				data, err := json.Marshal(map[string]any{"ContentURL": globalURL, "StaticContent": []any{map[string]string{"Path": "/site", "ContentURL": routeURL}}})
				if err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(cwd, "empty-config.json")
				if err = os.WriteFile(file, data, 0600); err != nil {
					t.Fatal(err)
				}
				loaded, err := (config.Loader{}).Load(ctx, file)
				if err != nil {
					t.Fatal(err)
				}
				fromFile, err := New(ctx, Options{Config: loaded})
				if err != nil {
					t.Fatal(err)
				}
				defer fromFile.Shutdown(ctx)
				if err = fromFile.Reload(ctx, 1); err == nil {
					t.Fatal("loaded empty file URL published cwd")
				}
			})
		}
	}
}

func TestStaticTrustedPlatformAuthority(t *testing.T) {
	ctx := context.Background()
	base := staticPhysicalDir(t)
	staticRootFile(t, base, "real/site/index.html", "granted")
	alias := filepath.Join(base, "platform-alias")
	if err := os.Symlink(filepath.Join(base, "real"), alias); err != nil {
		t.Fatal(err)
	}
	// The application, not configuration, independently grants the platform root.
	authority, err := os.OpenRoot(alias)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.Close()
	if err = os.Symlink("site", filepath.Join(base, "real", "redirect")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path            string
		granted, accept bool
	}{{alias + "/site", false, false}, {"site", true, true}, {"redirect", true, false}, {"../real/site", true, false}, {alias + "/site", true, false}} {
		t.Run(tc.path, func(t *testing.T) {
			cfg := staticRootConfig(tc.path)
			if tc.granted {
				cfg.StaticLocalRoot = authority
			}
			s, err := New(ctx, Options{Config: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Shutdown(ctx)
			err = s.Reload(ctx, 1)
			if tc.accept {
				if err != nil {
					t.Fatal(err)
				}
				staticRootResponse(t, s, "granted")
			} else if err == nil {
				t.Fatal("configured path exceeded grant")
			}
		})
	}
	// The caller's root is borrowed, not closed by staging or shutdown.
	if _, err = authority.Stat("site"); err != nil {
		t.Fatal(err)
	}
}

func TestStaticAncestorReplacementReloadRetention(t *testing.T) {
	ctx := context.Background()
	base, outside := staticPhysicalDir(t), staticPhysicalDir(t)
	staticRootFile(t, base, "parent/site/index.html", "old")
	staticRootFile(t, outside, "site/index.html", "secret")
	s, err := New(ctx, Options{Config: staticRootConfig(filepath.Join(base, "parent/site"))})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(base, "parent")
	saved := filepath.Join(base, "saved")
	if err = os.Rename(parent, saved); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, parent); err != nil {
		t.Fatal(err)
	}
	if err = s.Reload(ctx, 2); err == nil {
		t.Fatal("ancestor substitution published")
	}
	if s.manager.Revision() != 1 {
		t.Fatal("failed reload changed generation")
	}
	staticRootResponse(t, s, "old")
	if err = os.Remove(parent); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(saved, parent); err != nil {
		t.Fatal(err)
	}
	staticRootFile(t, base, "parent/site/index.html", "new")
	if err = s.Reload(ctx, 3); err != nil {
		t.Fatal(err)
	}
	staticRootResponse(t, s, "new")
}

func TestStaticPlatformTempDirectoryGrant(t *testing.T) {
	ctx := context.Background()
	directory, err := os.MkdirTemp(os.TempDir(), "static-platform-grant-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	staticRootFile(t, directory, "index.html", "platform")
	// os.TempDir is an application-chosen platform anchor, not a ContentURL value.
	authority, err := os.OpenRoot(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer authority.Close()
	cfg := staticRootConfig(filepath.Base(directory))
	cfg.StaticLocalRoot = authority
	s, err := New(ctx, Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	staticRootResponse(t, s, "platform")
	t.Logf("platform authority %q served a checked relative directory", os.TempDir())
}

func TestStaticLocalAuthorityCannotBeGrantedByJSON(t *testing.T) {
	base := staticPhysicalDir(t)
	file := filepath.Join(base, "config.json")
	if err := os.WriteFile(file, []byte(`{"StaticLocalRoot":"/","StaticContent":[{"Path":"/site","ContentURL":"."}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := (config.Loader{}).Load(context.Background(), file); err == nil {
		t.Fatal("JSON granted a filesystem authority")
	}
}
