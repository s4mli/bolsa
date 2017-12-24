package job

import (
	"encoding/json"
	"fmt"

	"github.com/samwooo/bolsa/gadgets/endpoints/restful"
)

type Telegram struct {
	Retries       int
	Email         string      `json:",omitempty"`
	CallbackURL   string      `json:",omitempty"`
	CustomFields  interface{} `json:",omitempty"`
	RelevantNames interface{} `json:",omitempty"`
}

func (t *Telegram) Post(c *restful.Context) (interface{}, error) {
	return fmt.Sprintf("post telegram %+v", *c), nil
}

func (t *Telegram) notificationPayload() map[string]interface{} {
	var assign = func(v interface{}, payload map[string]interface{}, key string) {
		if v != nil && v != "" {
			payload[key] = v
		}
	}
	payload := map[string]interface{}{}
	assign(t.Email, payload, "email")
	assign(t.CustomFields, payload, "customFields")
	assign(t.RelevantNames, payload, "relevantNames")
	return payload
}

type TelegramStreamer struct{}

func (n *TelegramStreamer) ContextsToBytes(cs []Context) ([]byte, error) {
	return json.Marshal(cs)
}

func (n *TelegramStreamer) BytesToContexts(bs []byte) ([]Context, error) {
	var telegrams []Telegram
	if err := json.Unmarshal(bs, &telegrams); err != nil {
		return nil, err
	} else {
		var cs []Context
		for _, t := range telegrams {
			cs = append(cs, t)
		}
		return cs, nil
	}
}

func NewTelegramStreamer() *TelegramStreamer {
	return &TelegramStreamer{}
}
