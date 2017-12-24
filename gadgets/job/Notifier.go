package job

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/samwooo/bolsa/gadgets/logging"
)

type Notifier struct {
	logger logging.Logger
}

func (n *Notifier) notify(t Telegram) (ActionStatus, error) {
	payload := t.notificationPayload()
	timestamp := time.Now().Unix()
	payload["timestamp"] = timestamp
	if jsonStr, err := json.Marshal(payload); err != nil {
		return FAILED, err
	} else {
		if req, err := http.NewRequest("POST", t.CallbackURL,
			bytes.NewBuffer(jsonStr)); err != nil {
			return FAILED, err
		} else {
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			if resp, err := client.Do(req); err != nil {
				return FAILED, err
			} else {
				defer resp.Body.Close()
				n.logger.Debugf("Post %s to %s response with %d", string(jsonStr),
					t.CallbackURL, resp.StatusCode)
				err := fmt.Errorf(resp.Status)
				if resp.StatusCode == http.StatusOK {
					err = nil
				}
				return ActionStatus(resp.StatusCode), err
			}
		}
	}
}

func (n *Notifier) Act(c Context) ActionResult {
	ar := ActionResult{C: c}
	if telegram, ok := c.(Telegram); !ok {
		ar.S, ar.E = FAILED, fmt.Errorf("act invalid telegram %+v", c)
	} else {
		ar.S, ar.E = n.notify(telegram)
	}
	if ar.E != nil {
		n.logger.Err(ar.E)
	}
	return ar
}

func newNotifier() *Notifier {
	return &Notifier{logger: logging.GetLogger(" < notifier > ")}
}
