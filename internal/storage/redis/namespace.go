package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// Namespace isolates a disposable cache from other Kafka source topics and
// explicitly controlled projection generations. Increment Epoch when a source
// is intentionally rebuilt with a different sequence history on the same topic.
// A changed market definition version is isolated at the market key level.
type Namespace struct{ fingerprint string }

func NewNamespace(topic, epoch string) (Namespace, error) {
	if topic == "" || epoch == "" || len(epoch) > 128 {
		return Namespace{}, errors.New("redis: source topic and bounded projection epoch required")
	}
	// Length delimits the fields unambiguously, even if either contains ':' or NUL.
	identity := fmt.Sprintf("%d:%s%d:%s", len(topic), topic, len(epoch), epoch)
	digest := sha256.Sum256([]byte(identity))
	return Namespace{fingerprint: hex.EncodeToString(digest[:16])}, nil
}

func (n Namespace) Valid() bool { return n.fingerprint != "" }

// MarketPrefix preserves Redis Cluster single-market key colocation.
func (n Namespace) MarketPrefix(sourceID uint32, version uint64) string {
	if !n.Valid() || version == 0 {
		return ""
	}
	return fmt.Sprintf("md:v2:%s:m:{%d}:v:%d:", n.fingerprint, sourceID, version)
}

func (n Namespace) Key(sourceID uint32, version uint64, suffix string) string {
	return n.MarketPrefix(sourceID, version) + suffix
}
