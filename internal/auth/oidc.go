package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
)

type Principal struct {
	ProviderID string
	Subject    string
}

type contextKey struct{}

var principalKey contextKey

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey).(Principal)
	return p, ok
}

type Middleware struct {
	issuer       string
	discoveryURL string
	clientID     string
	mu           sync.Mutex
	verifier     *oidc.IDTokenVerifier
}

func NewMiddleware(issuer, discoveryURL, clientID string) (*Middleware, error) {
	if strings.TrimSpace(issuer) == "" {
		return nil, errors.New("KEYCLOAK_ISSUER is required")
	}
	return &Middleware{issuer: issuer, discoveryURL: strings.TrimSpace(discoveryURL), clientID: clientID}, nil
}

func (m *Middleware) getVerifier(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.verifier != nil {
		return m.verifier, nil
	}
	discovery := m.issuer
	pctx := ctx
	if m.discoveryURL != "" && m.discoveryURL != m.issuer {
		// O token e emitido para o issuer publico (localhost:8081), mas dentro do
		// container o discovery e lido pela rede do compose (keycloak:8080).
		discovery = m.discoveryURL
		pctx = oidc.InsecureIssuerURLContext(ctx, m.issuer)
	}
	provider, err := oidc.NewProvider(pctx, discovery)
	if err != nil {
		return nil, fmt.Errorf("oidc provider: %w", err)
	}
	m.verifier = provider.Verifier(&oidc.Config{ClientID: m.clientID})
	return m.verifier, nil
}

func (m *Middleware) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := strings.TrimSpace(r.Header.Get("Authorization"))
		if !strings.HasPrefix(header, "Bearer ") {
			http.Error(w, `{"code":"UNAUTHENTICATED","message":"Bearer token is required"}`, 401)
			return
		}
		verifier, err := m.getVerifier(r.Context())
		if err != nil {
			http.Error(w, `{"code":"AUTH_PROVIDER_UNAVAILABLE","message":"identity provider unavailable"}`, 503)
			return
		}
		token, err := verifier.Verify(r.Context(), strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")))
		if err != nil {
			http.Error(w, `{"code":"UNAUTHENTICATED","message":"invalid or expired token"}`, 401)
			return
		}
		var claims struct {
			Subject string `json:"sub"`
			Azp     string `json:"azp"`
		}
		if err := token.Claims(&claims); err != nil || claims.Subject == "" || claims.Azp == "" {
			http.Error(w, `{"code":"UNAUTHENTICATED","message":"required identity claims are missing"}`, 401)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), Principal{ProviderID: claims.Azp, Subject: claims.Subject})))
	})
}

func RequireInternal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok {
			http.Error(w, `{"code":"UNAUTHENTICATED","message":"authentication required"}`, 401)
			return
		}
		if p.ProviderID != "wager-internal" {
			http.Error(w, `{"code":"FORBIDDEN","message":"internal service credentials required"}`, 403)
			return
		}
		next.ServeHTTP(w, r)
	})
}
