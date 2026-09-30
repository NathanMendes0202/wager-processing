package httpapi

import (
	"errors"
	"github.com/NathanMendes0202/wager-processing/internal/auth"
	"net/http"
	"strings"
)

func providerFromRequest(r *http.Request) (string, error) {
	p, ok := auth.PrincipalFromContext(r.Context())
	if !ok || strings.TrimSpace(p.ProviderID) == "" {
		return "", errors.New("authenticated provider is missing")
	}
	return p.ProviderID, nil
}
func sameProvider(r *http.Request, requested string) bool {
	p, ok := auth.PrincipalFromContext(r.Context())
	return ok && p.ProviderID == requested
}
