# kafkax

Go-клиент Kafka с изоляцией по тенантам (продюсер) и партициям (консьюмер), встроенной поддержкой OpenTelemetry и graceful shutdown.

## Установка

```bash
go get github.com/alfzs/kafkax
```

Требует CGO (`CGO_ENABLED=1` и C-компилятор) для сборки — зависимость тянет
`github.com/confluentinc/confluent-kafka-go/v2`:

```bash
go get github.com/confluentinc/confluent-kafka-go/v2
```

Отдельно устанавливать `librdkafka` в системе **не нужно**: по умолчанию
confluent-kafka-go линкует в бинарник собственную бандлированную статическую
сборку librdkafka. Поэтому C-компилятор и заголовки нужны только на этапе
сборки (build-стадия multi-stage Dockerfile) — в рантайм-образ их тащить не
надо.

Нюансы:
- Финальный образ всё равно должен быть на glibc-дистрибутиве (Debian/Ubuntu
  и т.п.) — часть функций cgo (`getaddrinfo` и т.п.) требует shared glibc
  даже при статической линковке librdkafka. `FROM scratch` не подойдёт.
- Для Alpine (musl) добавьте `-tags musl` к `go build`/`go get` — без него
  бинарник, собранный под glibc, на musl не запустится.
- `-tags dynamic` переключает на динамическую линковку системной
  librdkafka — в этом случае она должна быть установлена отдельно
  (`apt-get install librdkafka-dev` и т.п.), но по умолчанию kafkax этот
  тег не использует.

## Быстрый старт

### Продюсер

```go
cfg := kafkax.Config{
    Brokers:          []string{"kafka:9092"},
    ClientID:         "my-service",
    SecurityProtocol: "PLAINTEXT",
    GracefulTimeout:  3 * time.Minute,
    Producer: kafkax.Producer{
        RequiredAcks:  1,
        MessageTimeout: 30 * time.Second,
    },
}

producer, err := kafkax.NewKafkaProducer(ctx, cfg)
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

err = producer.SendMessage(ctx, kafkax.PublishRequest{
    TenantID: tenantID,
    Topic:    "orders",
    Value:    payload,
    Headers: kafkax.Headers{
        {Key: "signature", Value: signature},
    },
})
```

Ключи `traceparent`, `tracestate`, `baggage` зарезервированы под W3C trace
propagation — `SendMessage` вернёт ошибку, если один из них встретится в
`PublishRequest.Headers`.

### Консьюмер

```go
type orderHandler struct{}

func (h *orderHandler) ProcessMessage(ctx context.Context, msg kafkax.IncomingMessage) error {
    // ctx содержит OTel-span — используй его для дочерних операций
    if sig, ok := msg.Headers.Get("signature"); ok {
        // проверка подписи
    }
    order, err := encoding.UnmarshalProto[pb.Order](msg.Value)
    if err != nil {
        return err
    }
    // ...
    return nil
}

consumer, err := kafkax.NewKafkaConsumer(cfg)
if err != nil {
    log.Fatal(err)
}

consumer.AddHandler("orders", &orderHandler{})
consumer.SubscribeAll()
consumer.Start(ctx)
defer consumer.Stop()
```

## Архитектура

### Продюсер — изоляция по тенантам

Для каждого уникального `TenantID` создаётся отдельный воркер с буферным каналом. Медленный или застрявший тенант не влияет на доставку сообщений других тенантов.

```
SendMessage(tenantID=A) ──► worker-A ──► Kafka
SendMessage(tenantID=B) ──► worker-B ──► Kafka
SendMessage(tenantID=A) ──► worker-A (тот же)
```

Воркеры, простаивающие дольше `InactiveWorkerTTL`, завершаются фоновым сборщиком.

### Консьюмер — изоляция по партициям

Для каждой партиции создаётся отдельный воркер, что обеспечивает параллельную обработку сообщений из разных партиций при сохранении порядка внутри одной.

```
partition 0 ──► worker-0 ──► ProcessMessage (sequential)
partition 1 ──► worker-1 ──► ProcessMessage (sequential)
partition 2 ──► worker-2 ──► ProcessMessage (sequential)
```

Коммит offset выполняется только после успешной обработки (`EnableAutoCommit: false`).

**Защита от poison pill.** При ошибке `ProcessMessage` консьюмер повторяет вызов до `HandlerMaxRetries` раз с паузой `HandlerRetryDelay`. После исчерпания попыток offset коммитится и обработка продолжается — сообщение пропускается, чтобы не блокировать партицию навсегда.

## OpenTelemetry

### Трассировка сквозь Kafka

Продюсер создаёт дочерний span (`SpanKind=Producer`) и инжектирует W3C TraceContext в Kafka headers. Консьюмер извлекает его и создаёт дочерний span (`SpanKind=Consumer`), восстанавливая цепочку трассировки.

```
[HTTP handler span]
    └── [Producer span]  ← SendMessage
            └── [Consumer span]  ← ProcessMessage (ctx содержит parent span)
                    └── [DB query span]
```

kafkax использует глобальные провайдеры `otel.GetTracerProvider()` и `otel.GetMeterProvider()`. **OTel не обязателен** — если провайдеры не настроены, используются встроенные no-op реализации: span'ы и метрики отбрасываются, в Kafka headers ничего не инжектируется. Функциональность пакета от этого не зависит.

Если OTel нужен, настройте провайдеры до создания продюсера или консьюмера:

### Метрики

| Метрика | Тип | Описание |
|---|---|---|
| `kafkax.producer.messages.sent` | Counter | Сообщения, успешно доставленные в Kafka |
| `kafkax.producer.messages.failed` | Counter | Сообщения с ошибкой доставки |
| `kafkax.producer.message.duration` | Histogram | Latency от `SendMessage` до delivery ack, мс |
| `kafkax.producer.workers.active` | UpDownCounter | Активные воркеры тенантов |
| `kafkax.producer.queue.depth` | Gauge | Сообщения, ожидающие в очередях воркеров тенантов |
| `kafkax.consumer.messages.processed` | Counter | Успешно обработанные и закоммиченные сообщения |
| `kafkax.consumer.messages.failed` | Counter | Сообщения, пропущенные после исчерпания попыток |
| `kafkax.consumer.messages.retried` | Counter | Повторные попытки обработчика |
| `kafkax.consumer.processing.duration` | Histogram | Latency `ProcessMessage`, мс |
| `kafkax.consumer.commit.errors` | Counter | Неудачные вызовы `CommitMessage` |
| `kafkax.consumer.workers.active` | UpDownCounter | Активные воркеры партиций |
| `kafkax.consumer.queue.depth` | Gauge | Сообщения, ожидающие в очередях воркеров партиций |

## Конфигурация

### Корневые параметры

| Поле | Env | По умолчанию | Описание |
|---|---|---|---|
| `Brokers` | `KAFKAX_BROKERS` | — | Адреса брокеров `host:port`, через запятую. Достаточно одного — остальные обнаруживаются автоматически |
| `ClientID` | `KAFKAX_CLIENT_ID` | — | Идентификатор клиента в логах и метриках брокера |
| `SecurityProtocol` | — | `PLAINTEXT` | Протокол связи: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `GracefulTimeout` | — | `3m` | Таймаут graceful shutdown для Stop/Close |

### SASL

Обязателен только при `SecurityProtocol = SASL_PLAINTEXT` или `SASL_SSL`.

| Поле | Env | Описание |
|---|---|---|
| `SASL.Username` | `KAFKAX_SASL_USERNAME` | Имя пользователя |
| `SASL.Password` | `KAFKAX_SASL_PASSWORD` | Пароль |
| `SASL.Mechanism` | — | Механизм: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |

### TLS

| Поле | По умолчанию | Описание |
|---|---|---|
| `TLS.CaCertPath` | — | Путь к CA-сертификату брокера. Пустое — системный trust store |
| `TLS.ClientCertPath` | — | Клиентский сертификат для mTLS |
| `TLS.ClientKeyPath` | — | Клиентский ключ для mTLS |
| `TLS.IdentificationAlgorithm` | `https` | `ssl.endpoint.identification.algorithm`. Secure by default (совпадает с умолчанием librdkafka); `none` — только через `InsecureSkipVerify` |
| `TLS.InsecureSkipVerify` | `false` | Отключить проверку сертификата. Только для разработки |

### Продюсер

| Поле | По умолчанию | Описание |
|---|---|---|
| `RequiredAcks` | `1` | Подтверждения записи: `0` fire-and-forget, `1` лидер, `-1` все реплики ISR |
| `AckTimeout` | `5s` | Таймаут ожидания ack от брокера (`request.timeout.ms`) |
| `FlushTimeout` | `1m` | Таймаут финального flush при Close |
| `MaxRetries` | `3` | Повторные попытки при временных ошибках брокера |
| `RetryBackoff` | `100ms` | Пауза между повторными попытками |
| `BatchSize` | `1000` | Максимум сообщений в батче |
| `BatchBytes` | `1048576` | Максимальный размер батча, байт |
| `BatchTimeout` | `1s` | Максимальное время накопления батча |
| `Linger` | `0ms` | Задержка перед отправкой для укрупнения батча |
| `CompressionType` | `lz4` | Сжатие: `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `MaxInflight` | `1` | Неподтверждённых запросов на соединение. При `EnableIdempotence=true` должен быть `1` |
| `EnableIdempotence` | `true` | Exactly-once на уровне продюсера |
| `MessageQueueSize` | `1000` | Ёмкость канала воркера тенанта |
| `MessageTimeout` | `30s` | Суммарный таймаут SendMessage: очередь + delivery ack |
| `InactiveWorkerTTL` | `1h` | TTL воркера тенанта без активности |
| `CleanupWorkerInterval` | `10m` | Период фонового сборщика неактивных воркеров |

### Консьюмер

| Поле | Env | По умолчанию | Описание |
|---|---|---|---|
| `Group` | `KAFKAX_CONSUMER_GROUP` | — | Consumer group ID |
| `EnableAutoCommit` | — | `false` | Автокоммит offset. Оставлять `false` |
| `InitialOffset` | — | `earliest` | Начальный offset: `earliest` или `latest` |
| `MinBytes` | — | `1` | Минимум байт для fetch-ответа брокера |
| `MaxBytes` | — | `10485760` | Максимум байт в fetch-ответе |
| `MaxWait` | — | `250ms` | Максимальное время ожидания данных при fetch |
| `SocketTimeout` | — | `30s` | Таймаут TCP-соединения |
| `SessionTimeout` | — | `45s` | Таймаут сессии в consumer group |
| `HeartbeatInterval` | — | `3s` | Интервал heartbeat. Рекомендуется ≤ SessionTimeout/3 |
| `IsolationLevel` | — | `read_committed` | `read_committed` или `read_uncommitted` |
| `MaxPollInterval` | — | `1m` | Максимальный интервал между ReadMessage |
| `ReadTimeout` | — | `2s` | Таймаут одного ReadMessage |
| `ReadErrorBackoff` | — | `1s` | Пауза после нетаймаутной ошибки чтения |
| `MessageQueueSize` | — | `1000` | Ёмкость канала воркера партиции |
| `HandlerMaxRetries` | — | `3` | Повторных вызовов ProcessMessage при ошибке. `0` = бесконечно |
| `HandlerRetryDelay` | — | `1s` | Пауза между повторными вызовами обработчика |
| `InactiveWorkerTTL` | — | `1h` | TTL воркера партиции без активности |
| `CleanupWorkerInterval` | — | `10m` | Период фонового сборщика неактивных воркеров |

## Загрузка конфигурации из env

Теги `env` совместимы с [cleanenv](https://github.com/ilyakaznacheev/cleanenv):

```go
var cfg kafkax.Config
cleanenv.ReadEnv(&cfg)
```

Пример `.env`:

```env
KAFKAX_BROKERS=kafka-1.example.com:9092,kafka-2.example.com:9092
KAFKAX_CLIENT_ID=my-service
KAFKAX_CONSUMER_GROUP=my-service.group
KAFKAX_SASL_USERNAME=my-service-user
KAFKAX_SASL_PASSWORD=secret
```

## Десериализация Protobuf

Пакет `encoding` предоставляет хелпер для десериализации proto-сообщений без передачи шаблона:

```go
import "github.com/alfzs/kafkax/encoding"

func (h *handler) ProcessMessage(_ context.Context, msg kafkax.IncomingMessage) error {
    order, err := encoding.UnmarshalProto[pb.OrderCreated](msg.Value)
    if err != nil {
        return err
    }
    // ...
}
```

## Graceful Shutdown

**Продюсер** — `Close()` переводит продюсер в режим остановки, дожидается завершения всех воркеров и вызывает финальный `Flush` с таймаутом `FlushTimeout`. Повторный вызов безопасен.

**Консьюмер** — `Stop()` сигнализирует воркерам о завершении. Каждый воркер дочитывает накопленные в канале сообщения (drain) в пределах `GracefulTimeout`, после чего завершается принудительно. Повторный вызов безопасен.

Оба клиента также завершают работу при отмене контекста, переданного в `NewKafkaProducer`/`Start`, однако явный вызов `Close`/`Stop` предпочтительнее — он гарантирует drain-фазу.
