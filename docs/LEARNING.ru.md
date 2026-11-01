# Как читать этот проект (для Java/.NET-разработчика)

Порядок чтения: от бизнес-логики к инфраструктуре.

1. `internal/domain/` — правила, без зависимостей.
2. `internal/app/` — сценарии и интерфейсы (порты).
3. `internal/adapter/memory/` — простая реализация портов, на ней видно контракт.
4. `internal/worker/` — конкурентность в чистом виде.
5. `internal/adapter/postgres/`, `kafka/`, `httpapi/` — инфраструктура.
6. `cmd/settlement/main.go` — как всё собирается и как устроено завершение.

## Java/.NET → Go: соответствия

| В Java/.NET | В Go | Где посмотреть |
|---|---|---|
| Spring/ASP.NET DI-контейнер | Ручная сборка в `main`, зависимости через конструктор | `cmd/settlement/main.go` |
| `implements` / `: IService` | Неявная реализация интерфейса, проверка `var _ I = (*T)(nil)` | `memory/store.go` |
| Интерфейс рядом с реализацией | Интерфейс объявляет **потребитель**, маленький | `app/ports.go`, `httpapi.BetService` |
| Исключения | Ошибка — обычное значение; `%w` оборачивает, `errors.Is/As` проверяют | `domain/errors.go` |
| `@Transactional` / `TransactionScope` | Транзакция лежит в `context.Context`, репозиторий её достаёт | `postgres/db.go` |
| `CancellationToken` | `context.Context`: отмена, таймауты, request-scoped значения | везде |
| `ExecutorService` / `Task.WhenAll` | Горутины + каналы + `sync.WaitGroup` / `errgroup` | `worker/pool.go`, `main.go` |
| Builder / optional params | Functional options `WithClock(...)` | `app/service.go` |
| Filters / ASP.NET middleware | `func(http.Handler) http.Handler` | `httpapi/middleware.go` |
| JUnit `@ParameterizedTest` / xUnit `[Theory]` | Table-driven тесты + `t.Run` | `domain/bet_test.go` |
| Mockito / Moq | Ручной fake на маленький интерфейс | `app/service_test.go` (`failingOutbox`) |
| `BigDecimal` / `decimal` | Целые числа в минорных единицах | `domain/money.go` |

## Что спросят на собеседовании по Go и где это в проекте

- **Горутины и планировщик (G-M-P)**: горутина ~2–8 КБ стека, M:N планировщик,
  work stealing. В коде: воркеры в `worker/pool.go`.
- **Каналы**: буферизованные и небуферизованные; чтение из закрытого канала
  возвращает zero value; запись в закрытый — panic; `range` по каналу до `close`.
  В коде: `jobs` в `pool.go` закрывает только отправитель.
- **`select`**: ждёт первый готовый case; используется с `ctx.Done()`
  для отмены. В коде: продюсер в `pool.go`, `retry.go`, `relay.go`.
- **Утечки горутин**: горутина, заблокированная на канале навсегда. Защита —
  контекст и закрытие каналов. `time.After` в цикле держит таймер до
  срабатывания — поэтому в `retry.go` `NewTimer` + `Stop`.
- **`sync`**: `Mutex`/`RWMutex`, `WaitGroup`, `Once`, `atomic`. В коде:
  `memory/store.go`, `pool.go`, тесты.
- **Data race**: `go test -race` в CI.
- **Слайсы**: заголовок (ptr, len, cap), `append` может переаллоцировать,
  общий backing array. В коде: `make([]T, 0, n)` в `consumer.go`,
  `slices.Clone` в `memory/store.go` (снапшот для отката транзакции).
- **Мапы**: не потокобезопасны, порядок обхода случайный, нельзя взять адрес
  элемента. В коде: мапа под мьютексом в `memory/store.go`.
- **Интерфейсы**: `(type, value)`; nil-указатель в интерфейсе ≠ nil-интерфейс.
- **GC**: конкурентный tri-color mark & sweep, `GOGC`, `GOMEMLIMIT`; escape
  analysis решает, что идёт в кучу (`go build -gcflags=-m`).
- **`defer`**: аргументы вычисляются сразу, выполнение — LIFO при выходе;
  можно менять именованный результат (`WithinTx` в `postgres/db.go`).
- **panic/recover**: только для действительно неожиданного; `recover`
  работает только в `defer`. В коде: middleware `Recover`.
- **Generics**: `worker.Process[T]`, `httpapi.decode[T]`.

## Упражнения, чтобы «потрогать» Go

1. Перепиши `worker.Process` через `errgroup.SetLimit` и сравни бенчмарки
   (`make bench`).
2. Добавь Prometheus-метрики (счётчик рассчитанных ставок, гистограмма
   длительности HTTP) через middleware.
3. Добавь dead-letter topic для битых сообщений в `kafka/consumer.go`.
4. Добавь эндпоинт `GET /matches/{id}/bets` с пагинацией по курсору
   (`WHERE (created_at, id) > ($1, $2)`).
5. Запусти `go build -gcflags=-m ./internal/domain/` и разберись,
   что уходит в кучу и почему.
