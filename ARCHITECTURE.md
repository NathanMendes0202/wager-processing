# Architecture

## 1. Visão geral

O serviço é uma aplicação Go distribuída em que PostgreSQL é a fonte de verdade financeira. HTTP e SQS entram no mesmo caso de uso, enquanto Inbox e transactional Outbox garantem processamento durável e publicação assíncrona.

```text
             ┌───────────────┐
HTTP ───────►│               │
             │ Wager Service │──────► PostgreSQL
SQS ────────►│               │          │
             └───────┬───────┘          ├── Wallet
                     │                  ├── Ledger
                     │                  ├── WagerTransaction
                     │                  ├── Inbox
                     │                  └── Outbox
                     ▼
                Outbox Worker
                     │
                     ▼
                 SQS Events
```

## 2. Domínio

`internal/domain` contém os contratos financeiros independentes da infraestrutura:

- `Money`: unidades menores inteiras e ISO 4217;
- `Wallet`: saldo, moeda e versão;
- `TransactionKind`: `OPENING`, `BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`;
- `TransactionStatus`: `PENDING`, `PENDING_REFERENCE`, `PROCESSED`, `REJECTED`, `FAILED`;
- `FailureCode`: razões estáveis de rejeição;
- `LedgerEntry`: lançamento financeiro e invariantes de saldo;
- máquina de estados de `WagerTransaction`.

Repositories cuidam de SQL, transações e locks. Serviços orquestram casos de uso e adapters.

## 3. Concorrência financeira

Toda alteração de saldo utiliza:

```sql
SELECT balance_minor, currency, version
FROM wallets
WHERE id = $1 AND player_id = $2
FOR UPDATE;
```

A mesma transação atualiza saldo, versão e ledger. O PostgreSQL é responsável por serializar escritores da mesma carteira.

Isso garante que duas apostas de R$80 concorrendo sobre R$100 produzam exatamente um débito e uma rejeição.

## 4. Idempotência

Há duas proteções complementares:

1. `(provider_id, idempotency_key)`;
2. `(provider_id, external_transaction_id)`.

O payload hash é SHA-256 do JSON canônico dos campos financeiros e de identificação. Replay retorna o resultado persistido originalmente.

## 5. Inbox transacional

O consumer SQS não executa `Claim`, processamento e `Complete` em transações independentes.

O fluxo é:

```text
BEGIN
  Inbox claim
  Wager transaction
  Wallet
  Ledger
  Outbox
  Inbox complete
COMMIT
DeleteMessage
```

Uma falha antes do commit mantém a mensagem elegível para reentrega. Uma reentrega depois do commit encontra a Inbox concluída ou a operação idempotente.

## 6. Pending Reference

`REFUND` e `ROLLBACK` que não encontram referência não falham silenciosamente. A operação é persistida como `PENDING_REFERENCE`, recebe retry persistente e gera `WagerTransactionPendingReference`.

Após dez tentativas, a operação é `REJECTED` com `REFERENCE_NOT_FOUND` e gera `WagerTransactionRejected`.

O worker utiliza `FOR UPDATE SKIP LOCKED` para permitir múltiplas instâncias.

## 7. Transactional Outbox

Wallet, ledger, estado da transação e evento são persistidos no mesmo commit.

O publisher disputa registros usando `FOR UPDATE SKIP LOCKED`. Cada evento mantém seu `eventId` original. Falhas aumentam tentativas e aplicam backoff; depois do limite, o evento é encaminhado para a DLQ e marcado como morto.

## 8. Eventos

Eventos de domínio/integração:

- `WagerTransactionProcessed`;
- `WagerTransactionRejected`;
- `WagerTransactionPendingReference`;
- `WalletBalanceChanged`.

Cada envelope contém:

```text
eventId
eventType
aggregateId
correlationId
causationId
occurredAt
version
data
```

Payloads são structs Go concretas e o JSON da Outbox é tratado como snapshot imutável.

## 9. SQS FIFO

Entrada:

- fila: `wager-transactions.fifo`;
- DLQ: `wager-transactions-dlq.fifo`;
- `MessageGroupId`: `providerId`;
- `MessageDeduplicationId`: `messageId`.

Saída:

- fila: `wager-events.fifo`;
- DLQ: `wager-events-dlq.fifo`;
- `MessageGroupId`: `aggregateId`;
- `MessageDeduplicationId`: `eventId`.

FIFO é apenas uma camada adicional. Inbox e idempotência permanecem necessárias para segurança financeira.

## 10. Segurança

Keycloak fornece tokens OIDC para providers e operações internas. O verifier usa issuer/discovery, valida assinatura/expiração e verifica `azp`.

Rotas financeiras verificam o provider do token contra o provider persistido. Rotas internas exigem o client interno.

## 11. Health e shutdown

`/health/live` representa a vida do processo.

`/health/ready` verifica PostgreSQL e SQS.

Fx registra hooks para HTTP, consumer, Outbox publisher e Pending Reference worker. O shutdown cancela os contextos e aguarda as goroutines.

## 12. Migrations

Migrations são embutidas em `internal/platform/database/migrations` e protegidas por `pg_advisory_lock`, permitindo que múltiplas instâncias iniciem simultaneamente sem aplicar a mesma versão duas vezes.

## 13. Multi-instance

O serviço `api` não possui `container_name` nem porta publicada. Nginx recebe o tráfego externo e encaminha para o conjunto escalado de APIs.

```bash
make scale-3
```

O estado compartilhado e as garantias de concorrência permanecem no PostgreSQL.

## 14. Observabilidade

Logs estruturados usam `slog`. Métricas incluem status de transações, replay, retries, DLQ, conflitos reais de serialização/deadlock, divergências de reconciliação, duração de processamento e lag da Outbox.

Duração e lag são histogramas Prometheus.

## 15. Recuperação

### Antes do commit financeiro

A mensagem SQS não é removida. A transação é desfeita e a mensagem pode ser reentregue.

### Depois do commit e antes do delete SQS

A Inbox/idempotência identifica a operação já confirmada e impede novo efeito financeiro.

### Falha do publisher

A Outbox permanece pendente, é recuperada por outra instância e preserva o `eventId`.

### Restart

Saldo, ledger, Inbox, Outbox, idempotência e tentativas de referência permanecem no PostgreSQL.

## 16. Testes

A suíte cobre unidade, race detector, PostgreSQL real, LocalStack, Keycloak, concorrência em múltiplos processos, Inbox, Outbox, retry/DLQ, referências tardias/esgotadas, reconciliação, autenticação, HTTP/SQS e restart.
