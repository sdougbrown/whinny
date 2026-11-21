package apronws

import "encoding/json"

// Embed is one entry of a message body's embeds. Attachments ride as
// embeds, never in the message text; the fields the protocol defines are
// the kind, the title/caption pair, and the attachment URL.
type Embed struct {
	Kind    string `json:"kind,omitempty"`
	Title   string `json:"title,omitempty"`
	Caption string `json:"caption,omitempty"`
	URL     string `json:"url,omitempty"`
}

// DecodeEmbeds decodes a body's raw embeds. Unparseable or absent embeds
// decode to nil: embeds are display metadata, and a malformed entry is
// not a delivery-stopping error.
func DecodeEmbeds(raw json.RawMessage) []Embed {
	if len(raw) == 0 {
		return nil
	}
	var out []Embed
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// DecodeMentions decodes a body's raw mentions (a JSON array of user_id
// strings). Unparseable or absent mentions decode to nil.
func DecodeMentions(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}
