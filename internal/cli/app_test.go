package cli

import (
	"encoding/json"
	"testing"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/cli-contract/registry"
)

func TestAppValidates(t *testing.T) {
	if err := clicontract.Validate(App(Deps{})); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestSchemaDocument(t *testing.T) {
	doc, err := App(Deps{}).SchemaDocument()
	if err != nil {
		t.Fatalf("SchemaDocument: %v", err)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := m["schema"]; got != "cli-contract/cli-schema/v1" {
		t.Fatalf("schema id = %v, want cli-contract/cli-schema/v1", got)
	}
	commands, ok := m["commands"].(map[string]any)
	if !ok {
		t.Fatalf("commands = %T, want an object", m["commands"])
	}
	for _, name := range []string{"post", "history", "rooms", "watch", "quiet", "resume", "schema", "manifest", "version"} {
		if _, ok := commands[name]; !ok {
			t.Fatalf("commands missing %q", name)
		}
	}
	watch, ok := commands["watch"].(map[string]any)
	if !ok {
		t.Fatalf("watch = %T, want an object", commands["watch"])
	}
	subs, ok := watch["subcommands"].(map[string]any)
	if !ok {
		t.Fatalf("watch subcommands = %T, want an object", watch["subcommands"])
	}
	for _, name := range []string{"add", "cancel", "list"} {
		if _, ok := subs[name]; !ok {
			t.Fatalf("watch subcommands missing %q", name)
		}
	}
}

func TestManifest(t *testing.T) {
	b, err := registry.Manifest(App(Deps{}), registry.Options{Binary: Name, CommonFlags: []string{"url", "token"}})
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	tools, ok := m["tools"].(map[string]any)
	if !ok || len(tools) == 0 {
		t.Fatalf("tools = %v, want a non-empty object", m["tools"])
	}
	for _, name := range []string{
		"whinny-post", "whinny-history", "whinny-rooms",
		"whinny-watch-add", "whinny-watch-cancel", "whinny-watch-list",
		"whinny-quiet", "whinny-resume",
		"whinny-schema", "whinny-manifest", "whinny-version",
	} {
		if _, ok := tools[name]; !ok {
			t.Fatalf("tools missing %q", name)
		}
	}
}
