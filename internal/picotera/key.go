package picotera

import (
	"fmt"
	"time"
)

func RequestKey(id string, ts time.Time) string {
	return fmt.Sprintf("artifacts/%s/%s.request.json.zst", ts.UTC().Format("2006-01-02"), id)
}

func ResponseKey(id string, ts time.Time) string {
	return fmt.Sprintf("artifacts/%s/%s.response.json.zst", ts.UTC().Format("2006-01-02"), id)
}
