package publicapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
)

var errCursor = errors.New("invalid candle page token")

type candleCursor struct {
	V        int    `json:"v"`
	Market   string `json:"m"`
	Interval int32  `json:"i"`
	Start    string `json:"s"`
	End      string `json:"e"`
	Last     string `json:"l"`
	Limit    uint32 `json:"n"`
}

func signCursor(key []byte, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}
func encodeCursor(key []byte, c candleCursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b) + "." + base64.RawURLEncoding.EncodeToString(signCursor(key, b)), nil
}
func decodeCursor(key []byte, token string) (c candleCursor, err error) {
	if len(token) > 1024 {
		return c, errCursor
	}
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return c, errCursor
	}
	b, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return c, errCursor
	}
	signature, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil || !hmac.Equal(signature, signCursor(key, b)) {
		return c, errCursor
	}
	if json.Unmarshal(b, &c) != nil || c.V != 1 || c.Market == "" || c.Start == "" || c.End == "" || c.Last == "" || c.Limit == 0 {
		return c, errCursor
	}
	return c, nil
}
