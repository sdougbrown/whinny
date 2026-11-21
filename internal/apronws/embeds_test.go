package apronws

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDecodeEmbeds(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []Embed
	}{
		{"absent", ``, nil},
		{"null", `null`, nil},
		{"empty array", `[]`, []Embed{}},
		{
			"image embed with url",
			`[{"kind":"image","url":"https://apron.test/i/1.png","caption":"a chart"}]`,
			[]Embed{{Kind: "image", URL: "https://apron.test/i/1.png", Caption: "a chart"}},
		},
		{
			"file embed with title",
			`[{"kind":"file","title":"roster.csv"}]`,
			[]Embed{{Kind: "file", Title: "roster.csv"}},
		},
		{"malformed entry", `[{"kind":1}]`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw json.RawMessage
			if tt.raw != "" {
				raw = json.RawMessage(tt.raw)
			}
			got := DecodeEmbeds(raw)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeEmbeds = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDecodeMentions(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{"absent", ``, nil},
		{"array", `["u1","u2"]`, []string{"u1", "u2"}},
		{"empty array", `[]`, []string{}},
		{"malformed", `{"u1":true}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw json.RawMessage
			if tt.raw != "" {
				raw = json.RawMessage(tt.raw)
			}
			got := DecodeMentions(raw)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeMentions = %+v, want %+v", got, tt.want)
			}
		})
	}
}
