# Wager Processing

Serviço distribuído em Go para processamento financeiro de apostas, com PostgreSQL como fonte de verdade, SQS FIFO, transactional Inbox/Outbox, Keycloak/OIDC, idempotência, concorrência segura, retry de referências e reconciliação.

## 🚀 Como Executar os Testes

Este projeto utiliza um `Makefile` para gerenciar os comandos de build, execução e testes.

### Pré-requisitos
* **Go** (versão compatível com o `go.mod`)
* **Docker e Docker Compose** (para subida da infraestrutura de banco de dados e mensageria)
* **Make** 
  * *Linux/macOS:* Já vem instalado nativamente.
  * *Windows:* Recomenda-se executar os comandos via **Git Bash** (que já inclui o utilitário `make` e o interpretador padrão).

### Comandos Disponíveis
Comandos:

```bash
make test
make race
make vet
make integration
make test-multiprocess
make e2e-keycloak
make e2e-restart
make scale-3
```

## 1. Objetivos e garantias

- Valores monetários são representados em unidades menores inteiras; não há `float` para dinheiro.
- PostgreSQL é a fonte de verdade do saldo, transações, ledger, Inbox e Outbox.
- Duas instâncias podem disputar a mesma carteira sem permitir saldo negativo ou débito duplicado.
- A mesma operação pode chegar por HTTP ou SQS sem produzir dois efeitos financeiros.
- Reentregas SQS são protegidas por Inbox e idempotência.
- Eventos de saída são gravados na mesma transação da operação financeira.
- Referências ausentes são persistidas como `PENDING_REFERENCE` e processadas por worker com backoff.
- Falha definitiva de referência termina como `REJECTED / REFERENCE_NOT_FOUND`.
- Provedores são isolados pelo token OIDC e pelo `providerId` persistido.

## 2. Stack

- Go 1.24+
- PostgreSQL 16
- AWS SDK for Go v2 / SQS FIFO
- LocalStack para desenvolvimento e integração
- Keycloak 26 / OIDC
- Uber Fx
- Docker Compose
- Nginx para o cenário de múltiplas instâncias

## 3. Estrutura

```text
cmd/
  api/
  multiprocess-concurrency-test/
internal/
  app/
  auth/
  config/
  domain/
  httpapi/
  integrationtest/
  messaging/
  metrics/
  platform/database/
  repository/
  service/
scripts/
docker/
```

O domínio contém `Money`, `Wallet`, `WagerTransaction`, estados/tipos de transação, códigos de falha e `LedgerEntry`. Regras de persistência e locks permanecem nos repositories.

## 4. Como executar

Crie `.env` localmente a partir de `.env.example`. O arquivo `.env` não deve ser versionado.

```bash
cp .env.example .env
docker compose up -d --build
```

A API fica disponível em `http://localhost:8080` através do Nginx.

Keycloak fica em `http://localhost:8081` e LocalStack em `http://localhost:4566`.

## 5. Testes

```bash
go test ./...
go test -race ./...
go vet ./...
```


Integração real com PostgreSQL/LocalStack/Keycloak:

```bash
make integration
```

Concorrência em processos independentes:

```bash
make test-multiprocess
```

End-to-end:

```bash
make e2e-compose
make e2e-keycloak
make e2e-restart
```

Três instâncias independentes atrás do Nginx:

```bash
make scale-3
```

## 6. Modelo financeiro

### Money

`Money` usa unidades menores inteiras e código ISO 4217. A aplicação rejeita códigos de moeda não suportados e nunca converte dinheiro para `float`.

### Wallet

A carteira possui:

- `playerId`;
- moeda;
- saldo em unidades menores;
- versão monotônica.

O saldo é alterado sob `SELECT ... FOR UPDATE`. A operação de débito falha com `INSUFFICIENT_BALANCE` quando o saldo não é suficiente.

### LedgerEntry

Cada movimento monetário é um lançamento imutável contendo:

- carteira;
- transação;
- direção `DEBIT` ou `CREDIT`;
- valor;
- saldo antes;
- saldo depois.

O PostgreSQL possui trigger de proteção contra `UPDATE`, `DELETE` e `TRUNCATE` no ledger.

## 7. Tipos e estados de transação

Tipos externos:

| Tipo | Efeito |
| --- | --- |
| `BET` | débito |
| `WIN` | crédito |
| `LOSS` | operação sem movimento monetário; valor deve ser `0.00` |
| `REFUND` | crédito da aposta referenciada |
| `ROLLBACK` | desfaz o movimento da referência |

`OPENING` é reservado à abertura interna da carteira.

Estados:

```text
PENDING
  ├── PROCESSED
  ├── REJECTED
  └── PENDING_REFERENCE
          ├── PROCESSED
          └── REJECTED
```

`PROCESSED`, `REJECTED` e `FAILED` são terminais. A camada de domínio contém a máquina de estados para impedir transições inválidas.

## 8. Idempotência

A identidade financeira é composta por `providerId` + `idempotencyKey`, com proteção adicional por `providerId` + `externalTransactionId`.

O hash de payload é SHA-256 sobre JSON UTF-8 determinístico contendo:

```text
providerId
externalTransactionId
playerId
walletId
roundId
gameId
kind
amount
currency
referenceExternalTransactionId (quando presente)
```

Comportamento:

| Situação | Resultado |
| --- | --- |
| mesma chave + mesmo payload | replay do resultado persistido |
| mesma chave + payload diferente | conflito |
| mesmo external ID + outra chave | conflito |
| replay de operação concluída | retorna o saldo observado no processamento original |

O replay não recalcula o saldo atual da carteira.

## 9. API HTTP

### Público

```http
GET /health/live
GET /health/ready
GET /metrics
```

### Interno

```http
POST /wallets
GET /wallets/:walletId
GET /wallets/:walletId/ledger
POST /wallets/:walletId/reconciliation
```

Operações internas exigem o client OIDC `wager-internal`.

### Provider

```http
POST /wagering/transactions
POST /wagering/transactions/async
GET /wagering/transactions/:transactionId
GET /providers/:providerId/wagering/transactions/:externalTransactionId
```

Operações de provider exigem token válido e `azp` compatível com o `providerId`.

### Status HTTP

| Situação | HTTP |
| --- | ---: |
| entrada inválida | 400 |
| token ausente/inválido/expirado | 401 |
| recurso de outro provider / operação não autorizada | 403 |
| recurso inexistente | 404 |
| conflito de idempotência/external ID | 409 |
| rejeição financeira | 422 |
| `PENDING_REFERENCE` | 202 |
| indisponibilidade transitória | 503 |

## 10. Failure codes

Os códigos financeiros estáveis incluem:

```text
WALLET_PLAYER_MISMATCH
CURRENCY_MISMATCH
REFERENCE_REQUIRED
REFERENCE_NOT_FOUND
REFERENCE_NOT_PROCESSED
REFERENCE_MISMATCH
REFERENCE_AMOUNT_MISMATCH
INVALID_REFERENCE
INVALID_REFERENCE_KIND
ALREADY_REVERSED
INSUFFICIENT_BALANCE
INSUFFICIENT_BALANCE_FOR_REVERSAL
```

## 11. REFUND e ROLLBACK

`REFUND` exige referência `BET` processada.

`ROLLBACK` pode referenciar `BET`, `WIN` ou `REFUND` processados.

A referência precisa ter o mesmo:

- provider;
- carteira;
- jogador;
- rodada;
- moeda;
- valor.

Uma mesma operação não pode receber duas reversões do mesmo tipo. O banco garante essa regra com índice único parcial.

## 12. SQS FIFO e Inbox

A entrada usa `wager-transactions.fifo` e possui DLQ própria.

Envelope:

```json
{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}
```

`messageId` é obrigatório. Ele não é substituído pelo ID interno atribuído pelo SQS.

O fluxo é:

```text
SQS
  ↓
BEGIN
  ↓
Inbox claim
  ↓
Wager + Wallet + Ledger + Outbox
  ↓
Inbox complete
  ↓
COMMIT
  ↓
DeleteMessage
```

Assim, o efeito financeiro e a confirmação durável da Inbox são atômicos.

### Erros SQS

- mensagem inválida: erro permanente e request DLQ;
- conflito de idempotência/external ID: erro permanente;
- falha transitória: mensagem permanece na fila e recebe backoff de visibilidade;
- processamento bem-sucedido: mensagem é removida após o commit.

Backoff do consumer: 5 segundos, dobrando até 5 minutos. A fila também possui redrive com limite de 5 recebimentos.

`MessageGroupId` de entrada usa `providerId`; `MessageDeduplicationId` usa `messageId`. A aplicação não depende exclusivamente da deduplicação do FIFO: Inbox e idempotência são as garantias duráveis.

## 13. Pending Reference

`REFUND` e `ROLLBACK` podem chegar antes da operação referenciada.

Nesse caso:

```text
PENDING_REFERENCE
  ↓
retry 1
  ↓
retry 2
  ↓
...
  ↓
retry 10
  ↓
REJECTED / REFERENCE_NOT_FOUND
```

O intervalo inicial é 5 segundos, com backoff exponencial limitado a 5 minutos.

A primeira entrada em `PENDING_REFERENCE` produz `WagerTransactionPendingReference`. Ao resolver a referência, a operação segue o fluxo financeiro normal. Ao esgotar tentativas, é produzido `WagerTransactionRejected`.

## 14. Transactional Outbox

Estado financeiro e evento são confirmados na mesma transação.

Envelope publicado:

```json
{
  "eventId": "...",
  "eventType": "WagerTransactionProcessed",
  "aggregateId": "...",
  "correlationId": "...",
  "causationId": "...",
  "occurredAt": "...",
  "version": 1,
  "data": {}
}
```

Eventos:

| Evento | Gatilho |
| --- | --- |
| `WagerTransactionProcessed` | conclusão de operação, inclusive `LOSS` |
| `WagerTransactionRejected` | rejeição definitiva |
| `WagerTransactionPendingReference` | operação aguardando referência |
| `WalletBalanceChanged` | alteração efetiva do saldo |

`WalletBalanceChanged` contém `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter` e `walletVersion`.

O `eventId` é persistente e usado também como `MessageDeduplicationId` na saída.

Outbox suporta múltiplos publishers, `FOR UPDATE SKIP LOCKED`, backoff persistente, recuperação de trabalho abandonado e DLQ após o limite de tentativas.

## 15. Reconciliação

```http
POST /wallets/:walletId/reconciliation
```

O saldo é reconstruído a partir do ledger, incluindo a abertura, e comparado ao saldo armazenado.

A resposta informa:

```text
storedBalance
calculatedBalance
difference
consistent
checkedEntries
```

A reconciliação não altera o saldo. Divergências são registradas e contabilizadas em métrica.

## 16. Segurança

Keycloak fornece tokens OIDC para:

- `provider-a`;
- `provider-b`;
- `wager-internal`.

O token é validado por issuer/discovery, assinatura, expiração, `sub` e `azp`.

O código não aceita acesso de um provider a transações de outro provider. Operações internas não são liberadas para clients de provider.

Os testes E2E cobrem credencial ausente, inválida e expirada, isolamento entre providers e operações internas.

## 17. Health checks

`/health/live` verifica somente a disponibilidade do processo.

`/health/ready` verifica:

- PostgreSQL;
- fila SQS de entrada.

Se uma dependência essencial estiver indisponível, readiness retorna `503`.

## 18. Observabilidade

Logs são JSON via `log/slog` e podem carregar `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId` sem registrar credenciais ou payload financeiro completo.

Métricas disponíveis:

```text
wager_transactions_total
wager_idempotency_replays_total
wager_retries_total
wager_dlq_total
wager_request_dlq_total
wager_concurrency_conflicts_total
wager_reconciliation_differences_total
wager_processing_duration_seconds
wager_outbox_lag_seconds
```

Duração de processamento e lag da Outbox são expostos como histogramas com buckets e contagem/soma.

Conflito de concorrência é contabilizado apenas para erros PostgreSQL de serialização/deadlock; `INSUFFICIENT_BALANCE` é rejeição de negócio.

## 19. Concorrência e recuperação

O cenário crítico do desafio é:

```text
saldo = R$100

instância A → BET R$80
instância B → BET R$80

resultado:
1 operação processada
1 operação rejeitada
saldo final = R$20
```

Há testes com:

- 50 entregas simultâneas da mesma aposta;
- duas ou três instâncias independentes;
- mesma chave de idempotência;
- apostas concorrentes distintas;
- carteiras distintas;
- HTTP e SQS para a mesma operação;
- restart do processo.

## 20. Múltiplas instâncias

A API não fixa `container_name` nem publica diretamente sua porta. O Nginx é o ponto de entrada:

```text
localhost:8080
      ↓
    Nginx
   ↙  ↓  ↘
 API API API
   \  |  /
 PostgreSQL
```

Use:

```bash
make scale-3
```

As garantias compartilhadas ficam no PostgreSQL: locks por carteira, idempotência, Inbox, ledger e Outbox.

## 21. Fx e shutdown

Uber Fx compõe banco, migrations, autenticação, repositories, Inbox, Outbox, consumer, publishers, worker de referências e HTTP.

No shutdown, os workers recebem cancelamento e suas goroutines são aguardadas até o prazo fornecido pelo Fx. O servidor HTTP trata `http.ErrServerClosed` como encerramento normal e registra falhas inesperadas.

A migration utiliza `pg_advisory_lock` para impedir que múltiplas instâncias executem a mesma migration simultaneamente.

## 22. Testes obrigatórios e E2E

A suíte deve ser executada com PostgreSQL, LocalStack e Keycloak reais, sem substituir toda a infraestrutura por mocks.

Cobertura relevante:

- Money, escala, moeda, limites e valores inválidos;
- carteira e invariantes;
- máquina de estados;
- BET/WIN/LOSS/REFUND/ROLLBACK;
- idempotência e hash;
- concorrência e race detector;
- Inbox e reentrega;
- retry e DLQ;
- Pending Reference tardia e esgotada;
- Outbox concorrente e recuperação;
- reconciliação;
- Keycloak válido/inválido/expirado;
- isolamento entre providers;
- HTTP ↔ SQS;
- restart;
- ciclo Start/Stop do Fx;
- três instâncias independentes.

Comandos:

```bash
make test
make race
make vet
make integration
make test-multiprocess
make e2e-keycloak
make e2e-restart
make scale-3
```

## 23. Limitações de ambiente

Os testes E2E dependem de Docker Compose, PostgreSQL, LocalStack e Keycloak disponíveis. A execução completa do projeto exige Go 1.24 ou superior.
