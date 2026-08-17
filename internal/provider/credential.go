package provider

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9]+`)

// EnvCredentialResolver resolves a credential_ref such as
// "vault://notification/lark-bot" to the environment variable
// NOTIF_CRED_NOTIFICATION_LARK_BOT. It exists so this repository is
// runnable without a real secrets manager; production deployments should
// implement CredentialResolver against their actual vault/KMS instead.
type EnvCredentialResolver struct{}

func (EnvCredentialResolver) Resolve(ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	envName := envNameFor(ref)
	val, ok := os.LookupEnv(envName)
	if !ok {
		return "", fmt.Errorf("no environment variable %s set for credential_ref %q", envName, ref)
	}
	return val, nil
}

func envNameFor(ref string) string {
	stripped := ref
	if idx := strings.Index(stripped, "://"); idx >= 0 {
		stripped = stripped[idx+3:]
	}
	normalized := strings.ToUpper(nonAlnum.ReplaceAllString(stripped, "_"))
	return "NOTIF_CRED_" + normalized
}
