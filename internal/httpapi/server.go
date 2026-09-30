package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/NathanMendes0202/wager-processing/internal/auth"
	"go.uber.org/fx"

	"github.com/NathanMendes0202/wager-processing/internal/config"
	"github.com/NathanMendes0202/wager-processing/internal/domain"
	"github.com/NathanMendes0202/wager-processing/internal/messaging"
	"github.com/NathanMendes0202/wager-processing/internal/metrics"
	"github.com/NathanMendes0202/wager-processing/internal/repository"
	"github.com/NathanMendes0202/wager-processing/internal/service"
)

type Server struct {
	httpServer *http.Server
	auth       *auth.Middleware
	wallets    *service.WalletService
	wagers     *service.WagerService
	walletRepo *repository.WalletRepository
	wagerRepo  *repository.WagerRepository
	publisher  *messaging.Publisher
	metrics    *metrics.Metrics
}

func NewServer(cfg config.Config, authMiddleware *auth.Middleware, wallets *service.WalletService, wagers *service.WagerService, walletRepo *repository.WalletRepository, wagerRepo *repository.WagerRepository, publisher *messaging.Publisher, m *metrics.Metrics) *Server {
	s := &Server{auth: authMiddleware, wallets: wallets, wagers: wagers, walletRepo: walletRepo, wagerRepo: wagerRepo, publisher: publisher, metrics: m}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.Handle("POST /wallets", s.auth.Require(auth.RequireInternal(http.HandlerFunc(s.createWallet))))
	mux.Handle("GET /wallets/{walletID}", s.auth.Require(auth.RequireInternal(http.HandlerFunc(s.getWallet))))
	mux.Handle("GET /wallets/{walletID}/ledger", s.auth.Require(auth.RequireInternal(http.HandlerFunc(s.ledger))))
	mux.Handle("POST /wallets/{walletID}/reconciliation", s.auth.Require(auth.RequireInternal(http.HandlerFunc(s.reconcile))))
	mux.Handle("POST /wagering/transactions", s.auth.Require(http.HandlerFunc(s.bet)))
	mux.Handle("POST /wagering/transactions/async", s.auth.Require(http.HandlerFunc(s.publishBet)))
	mux.Handle("GET /wagering/transactions/{transactionID}", s.auth.Require(http.HandlerFunc(s.getTransaction)))
	mux.Handle("GET /providers/{providerID}/wagering/transactions/{externalTransactionID}", s.auth.Require(http.HandlerFunc(s.getProviderTransaction)))
	s.httpServer = &http.Server{Addr: cfg.HTTPAddr, Handler: requestLogging(mux)}
	return s
}

func RegisterLifecycle(lc fx.Lifecycle, server *Server) {
	logger := slog.Default()
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				if err := server.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Error("http server stopped unexpectedly", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error { return server.httpServer.Shutdown(ctx) },
	})
}

func requestLogging(next http.Handler) http.Handler {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", requestID)
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Info("http_request", "requestId", requestID, "method", r.Method, "path", r.URL.Path, "durationMs", time.Since(start).Milliseconds())
	})
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.walletRepo.Ready(ctx); err != nil {
		writeJSON(w, 503, map[string]any{"status": "not_ready", "postgres": "down", "sqs": "unknown"})
		return
	}
	if err := s.publisher.Ready(ctx); err != nil {
		writeJSON(w, 503, map[string]any{"status": "not_ready", "postgres": "ready", "sqs": "down"})
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ready", "postgres": "ready", "sqs": "ready"})
}

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
type createWalletDTO struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}
type wagerDTO struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId,omitempty"`
}

func (s *Server) createWallet(w http.ResponseWriter, r *http.Request) {
	var req createWalletDTO
	if !decode(w, r, &req) {
		return
	}
	player, err := uuid.Parse(req.PlayerID)
	if err != nil {
		problem(w, 400, "INVALID_PLAYER_ID", err.Error())
		return
	}
	money, err := domain.NewMoney(req.InitialBalance.Amount, req.InitialBalance.Currency)
	if err != nil {
		problem(w, 400, "INVALID_MONEY", err.Error())
		return
	}
	wallet, err := s.wallets.Create(r.Context(), player, money)
	if err != nil {
		if isUnique(err) {
			problem(w, 409, "WALLET_ALREADY_EXISTS", "wallet already exists")
		} else {
			problem(w, 500, "INTERNAL_ERROR", err.Error())
		}
		return
	}
	writeJSON(w, 201, map[string]any{"id": wallet.ID.String(), "playerId": wallet.PlayerID.String(), "balance": moneyDTO{wallet.Balance.String(), wallet.Currency}, "version": wallet.Version})
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("walletID"))
	if err != nil {
		problem(w, 400, "INVALID_WALLET_ID", err.Error())
		return
	}
	wallet, err := s.wallets.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			problem(w, 404, "WALLET_NOT_FOUND", "wallet not found")
		} else {
			problem(w, 500, "INTERNAL_ERROR", err.Error())
		}
		return
	}
	writeJSON(w, 200, map[string]any{"id": wallet.ID.String(), "playerId": wallet.PlayerID.String(), "balance": moneyDTO{wallet.Balance.String(), wallet.Currency}, "version": wallet.Version})
}

func (s *Server) ledger(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("walletID"))
	if err != nil {
		problem(w, 400, "INVALID_WALLET_ID", err.Error())
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if _, e := fmtInt(v, &limit); e != nil {
			problem(w, 400, "INVALID_LIMIT", e.Error())
			return
		}
	}
	entries, next, err := s.walletRepo.ListLedger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		problem(w, 500, "INTERNAL_ERROR", err.Error())
		return
	}
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, map[string]any{"id": e.ID.String(), "transactionId": e.TransactionID.String(), "direction": e.Direction, "money": moneyDTO{e.Money.String(), e.Money.Currency()}, "balanceBefore": moneyDTO{e.BalanceBefore.String(), e.BalanceBefore.Currency()}, "balanceAfter": moneyDTO{e.BalanceAfter.String(), e.BalanceAfter.Currency()}, "createdAt": e.CreatedAt})
	}
	writeJSON(w, 200, map[string]any{"items": out, "nextCursor": next})
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("walletID"))
	if err != nil {
		problem(w, 400, "INVALID_WALLET_ID", err.Error())
		return
	}
	rec, err := s.walletRepo.Reconcile(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			problem(w, 404, "WALLET_NOT_FOUND", "wallet not found")
		} else {
			problem(w, 500, "INTERNAL_ERROR", err.Error())
		}
		return
	}
	if !rec.Consistent && s.metrics != nil {
		s.metrics.ReconciliationDifference()
	}
	writeJSON(w, 200, map[string]any{"walletId": id.String(), "storedBalance": moneyDTO{rec.StoredBalance.String(), rec.StoredBalance.Currency()}, "calculatedBalance": moneyDTO{rec.CalculatedBalance.String(), rec.CalculatedBalance.Currency()}, "difference": moneyDTO{rec.Difference.String(), rec.Difference.Currency()}, "consistent": rec.Consistent, "checkedEntries": rec.CheckedEntries})
}

func (s *Server) publishBet(w http.ResponseWriter, r *http.Request) {
	authProvider, err := providerFromRequest(r)
	if err != nil {
		problem(w, 401, "UNAUTHENTICATED", err.Error())
		return
	}
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idempotencyKey == "" {
		problem(w, 400, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key is required")
		return
	}
	var req wagerDTO
	if !decode(w, r, &req) {
		return
	}
	if !supportedKind(req.Kind) {
		problem(w, 400, "UNSUPPORTED_KIND", "kind must be BET, WIN, LOSS, REFUND or ROLLBACK")
		return
	}
	if strings.TrimSpace(req.ProviderID) != authProvider {
		problem(w, 403, "FORBIDDEN", "providerId is not authorized for this token")
		return
	}
	player, err := uuid.Parse(req.PlayerID)
	if err != nil {
		problem(w, 400, "INVALID_PLAYER_ID", err.Error())
		return
	}
	wallet, err := uuid.Parse(req.WalletID)
	if err != nil {
		problem(w, 400, "INVALID_WALLET_ID", err.Error())
		return
	}
	money, err := domain.NewMoney(req.Money.Amount, req.Money.Currency)
	if err != nil {
		problem(w, 400, "INVALID_MONEY", "amount or currency is invalid")
		return
	}
	if err := repository.ValidateAmount(req.Kind, money); err != nil {
		problem(w, 400, "INVALID_MONEY", err.Error())
		return
	}
	messageID := uuid.New().String()
	err = s.publisher.Publish(r.Context(), messaging.WagerTransactionMessage{
		MessageID: messageID, Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC(),
		Data: messaging.WagerTransactionData{ProviderID: authProvider, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: idempotencyKey,
			PlayerID: player.String(), WalletID: wallet.String(), RoundID: req.RoundID, GameID: req.GameID, Kind: req.Kind,
			Money: messaging.MoneyData{Amount: money.String(), Currency: money.Currency()}, ReferenceExternalID: req.ReferenceExternalTransactionID},
	})
	if err != nil {
		problem(w, 503, "QUEUE_UNAVAILABLE", "transaction could not be queued")
		return
	}
	writeJSON(w, 202, map[string]any{"messageId": messageID, "status": "QUEUED", "providerId": authProvider})
}

func (s *Server) bet(w http.ResponseWriter, r *http.Request) {
	authProvider, err := providerFromRequest(r)
	if err != nil {
		problem(w, 401, "UNAUTHENTICATED", err.Error())
		return
	}
	if r.Header.Get("Idempotency-Key") == "" {
		problem(w, 400, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key is required")
		return
	}
	var req wagerDTO
	if !decode(w, r, &req) {
		return
	}
	if !supportedKind(req.Kind) {
		problem(w, 400, "UNSUPPORTED_KIND", "kind must be BET, WIN, LOSS, REFUND or ROLLBACK")
		return
	}
	provider := strings.TrimSpace(req.ProviderID)
	if provider != authProvider {
		problem(w, 403, "FORBIDDEN", "providerId is not authorized for this token")
		return
	}
	if provider == "" {
		problem(w, 400, "INVALID_PROVIDER_ID", "providerId is required")
		return
	}
	player, err := uuid.Parse(req.PlayerID)
	if err != nil {
		problem(w, 400, "INVALID_PLAYER_ID", err.Error())
		return
	}
	wallet, err := uuid.Parse(req.WalletID)
	if err != nil {
		problem(w, 400, "INVALID_WALLET_ID", err.Error())
		return
	}
	money, err := domain.NewMoney(req.Money.Amount, req.Money.Currency)
	if err != nil {
		problem(w, 400, "INVALID_MONEY", "amount or currency is invalid")
		return
	}
	if err := repository.ValidateAmount(req.Kind, money); err != nil {
		problem(w, 400, "INVALID_MONEY", err.Error())
		return
	}
	result, err := s.wagers.Process(r.Context(), service.WagerCommand{ProviderID: provider, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: r.Header.Get("Idempotency-Key"), PlayerID: player, WalletID: wallet, RoundID: req.RoundID, GameID: req.GameID, Kind: req.Kind, Amount: money, ReferenceExternalID: req.ReferenceExternalTransactionID})
	if err != nil {
		switch {
		case errors.Is(err, service.ErrPendingReference):
			problem(w, 202, "PENDING_REFERENCE", "transaction is waiting for its reference transaction")
		case errors.Is(err, repository.ErrIdempotencyConflict):
			problem(w, 409, "IDEMPOTENCY_CONFLICT", err.Error())
		case errors.Is(err, repository.ErrExternalIDConflict):
			problem(w, 409, "EXTERNAL_TRANSACTION_CONFLICT", err.Error())
		case errors.Is(err, repository.ErrWalletNotFound):
			problem(w, 404, "WALLET_NOT_FOUND", err.Error())
		case errors.Is(err, repository.ErrInvalidAmount):
			problem(w, 400, "INVALID_MONEY", err.Error())
		case errors.Is(err, repository.ErrUnsupportedKind):
			problem(w, 400, "UNSUPPORTED_KIND", err.Error())
		default:
			slog.Error("wager_process_failed", "error", err.Error(), "providerId", provider)
			problem(w, 500, "INTERNAL_ERROR", "internal error")
		}
		return
	}
	body := map[string]any{"transactionId": result.TransactionID.String(), "status": result.Status, "idempotentReplay": result.Replay}
	if result.Balance.Currency() != "" {
		body["balance"] = moneyDTO{result.Balance.String(), result.Balance.Currency()}
	}
	if result.Status == "REJECTED" {
		// Business rejection: final and auditable, distinct from 4xx input errors.
		body["failureCode"] = result.FailureCode
		writeJSON(w, 422, body)
		return
	}
	status := 200
	if !result.Replay {
		status = 201
	}
	writeJSON(w, status, body)
}

func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("transactionID"))
	if err != nil {
		problem(w, 400, "INVALID_TRANSACTION_ID", err.Error())
		return
	}
	tx, err := s.wagerRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			problem(w, 404, "TRANSACTION_NOT_FOUND", "transaction not found")
		} else {
			problem(w, 500, "INTERNAL_ERROR", err.Error())
		}
		return
	}
	if !sameProvider(r, tx.ProviderID) {
		problem(w, 403, "FORBIDDEN", "provider access is restricted to its own transactions")
		return
	}
	writeWager(w, tx)
}
func (s *Server) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	requestedProvider := r.PathValue("providerID")
	if !sameProvider(r, requestedProvider) {
		problem(w, 403, "FORBIDDEN", "provider access is restricted to its own transactions")
		return
	}
	tx, err := s.wagerRepo.GetByProviderExternal(r.Context(), r.PathValue("providerID"), r.PathValue("externalTransactionID"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			problem(w, 404, "TRANSACTION_NOT_FOUND", "transaction not found")
		} else {
			problem(w, 500, "INTERNAL_ERROR", err.Error())
		}
		return
	}
	writeWager(w, tx)
}
func writeWager(w http.ResponseWriter, t repository.WagerDetails) {
	writeJSON(w, 200, map[string]any{"transactionId": t.ID.String(), "providerId": t.ProviderID, "externalTransactionId": t.ExternalTransactionID, "playerId": t.PlayerID.String(), "walletId": t.WalletID.String(), "roundId": t.RoundID, "gameId": t.GameID, "kind": t.Kind, "status": t.Status, "money": moneyDTO{t.Money.String(), t.Money.Currency()}, "referenceExternalTransactionId": t.ReferenceExternalID, "referenceTransactionId": uuidString(t.ReferenceTransaction), "failureCode": t.FailureCode, "result": map[string]any{"balance": moneyDTO{t.ResultBalance.String(), t.ResultBalance.Currency()}, "walletVersion": t.ResultWalletVersion}, "createdAt": t.CreatedAt, "updatedAt": t.UpdatedAt})
}

func uuidString(id *uuid.UUID) string {
	if id == nil {
		return ""
	}
	return id.String()
}

func supportedKind(kind string) bool {
	switch kind {
	case "BET", "WIN", "LOSS", "REFUND", "ROLLBACK":
		return true
	}
	return false
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		problem(w, 400, "INVALID_JSON", err.Error())
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { next.ServeHTTP(w, r) })
}
func isUnique(err error) bool {
	var e interface{ SQLState() string }
	return errors.As(err, &e) && e.SQLState() == "23505"
}
func fmtInt(v string, out *int) (int, error) {
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, errors.New("must be a positive integer")
		}
		n = n*10 + int(c-'0')
		if n > 100 {
			return 0, errors.New("maximum is 100")
		}
	}
	*out = n
	return n, nil
}
