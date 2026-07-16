package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/xb0or/opencode-GO/config"
	"github.com/xb0or/opencode-GO/pool"
	"github.com/xb0or/opencode-GO/store"
)

// ollamaQuotaTransport reuses the shared quota transport pool for connection
// reuse across Ollama Cloud quota probes.
var ollamaQuotaClient = &http.Client{
	Timeout:   15 * time.Second,
	Transport: quotaHTTPTransport,
}

// CheckOllamaQuotaByCookie probes a single Ollama Cloud API key by issuing a
// lightweight GET /v1/models request. The key (stored in Key.Value) is used
// as the Bearer token.
//
// Returns expired=true on HTTP 401/403 (key revoked/invalid). Transient
// errors (network, 5xx, timeout) return expired=false + err so the pool does
// NOT disable the key for a temporary upstream issue.
func CheckOllamaQuotaByCookie(k store.Key) (expired bool, err error) {
	apiKey := k.Value
	if apiKey == "" {
		return true, nil // no credential configured — treat as expired
	}

	base := config.BaseURLFor(config.UpstreamOllama)
	url := base + "/v1/models"

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("build ollama quota probe: %w", err)
	}
	req.Header.Set("Authorization", "Bearer ***"+apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := ollamaQuotaClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("ollama quota probe http: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		// Definitive: key is invalid/revoked.
		return true, nil
	case resp.StatusCode >= 500:
		// Transient upstream error — do NOT disable the key.
		body := make([]byte, 200)
		n, _ := resp.Body.Read(body)
		return false, fmt.Errorf("ollama upstream %d: %s", resp.StatusCode, string(body[:n]))
	case resp.StatusCode == http.StatusOK:
		// Key is valid. Optionally parse model count for logging.
		var payload struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		return false, nil
	default:
		// Unexpected status — treat as transient, don't disable.
		return false, fmt.Errorf("ollama quota probe: unexpected status %d", resp.StatusCode)
	}
}

// CheckOllamaGroupQuotas runs concurrent quota probes for every enabled Ollama
// key in the pool. Designed to be called from a background ticker. Returns
// aggregated results for logging.
func CheckOllamaGroupQuotas(p *pool.Picker, maxConcurrency int) []pool.QuotaCheckResult {
	return pool.CheckGroupQuotas(p, string(config.UpstreamOllama), CheckOllamaQuotaByCookie, maxConcurrency)
}

// CheckGoGroupQuotas runs concurrent quota checks for every enabled Go key
// in the pool, using the existing fetchGoQuota logic. Designed for background
// ticker use.
func CheckGoGroupQuotas(p *pool.Picker, maxConcurrency int) []pool.QuotaCheckResult {
	return pool.CheckGroupQuotas(p, string(config.UpstreamGo), checkGoKeyQuota, maxConcurrency)
}

// checkGoKeyQuota is the QuotaChecker callback for Go (opencode.ai) keys. It
// reuses fetchGoQuota and detects cookie expiry via isCookieExpiredError.
func checkGoKeyQuota(k store.Key) (expired bool, err error) {
	cookie := normalizeAuthCookie(k.Cookie)
	if cookie == "" {
		return true, nil // no cookie configured
	}
	workspaceID := normalizeWorkspaceID(k.WorkspaceID)
	if workspaceID == "" {
		// Auto-detect workspace (no quota fetch yet, just resolve).
		wid, _, err := resolveWorkspaceForQuota(cookie)
		if err != nil {
			if isCookieExpiredError(err) {
				return true, nil
			}
			return false, err
		}
		workspaceID = wid
		// Persist resolved workspace so next check is faster.
		store.DB().Model(&store.Key{}).Where("id = ?", k.ID).Update("workspace_id", workspaceID)
	}
	_, err = fetchGoQuota(cookie, workspaceID)
	if err != nil && isCookieExpiredError(err) {
		return true, nil
	}
	return false, err
}
