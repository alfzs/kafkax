# kafkax

Go-клиент Kafka: продюсер и консьюмер с изоляцией по партициям, OpenTelemetry
и graceful shutdown из коробки. Построен на
[franz-go](https://github.com/twmb/franz-go).

Чистый Go, **без cgo**: статическая сборка, обычная кросс-компиляция,
`FROM scratch` и Alpine работают без тегов сборки, детектор гонок видит весь
код клиента.

## Установка

```bash
go get github.com/alfzs/kafkax/v2
```

```go
import "github.com/alfzs/kafkax/v2"
```

Требуется Go 1.26+.

## Быстрый старт

### Продюсер

```go
cfg := kafkax.Config{
    Brokers:         []string{"kafka:9092"},
    ClientID:        "my-service",
    GracefulTimeout: 3 * time.Minute,
    DialTimeout:     10 * time.Second,
    Producer: kafkax.Producer{
        RequiredAcks:      -1,
        EnableIdempotence: true,
        MessageTimeout:    30 * time.Second,
        FlushTimeout:      time.Minute,
    },
}

producer, err := kafkax.NewKafkaProducer(cfg)
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

err = producer.SendMessage(ctx, kafkax.PublishRequest{
    Topic: "orders",
    Key:   orderID[:],
    Value: payload,
    Headers: kafkax.Headers{
        {Key: "signature", Value: signature},
    },
})
```

`SendMessage` синхронен и потокобезопасен: параллельные вызовы батчатся внутри
franz-go, так что «синхронный» не означает «по запросу на сообщение». Весь путь
отправки укладывается в один бюджет — `Producer.MessageTimeout`, отсчитываемый
от входа в метод.

Ключи `traceparent`, `tracestate` и `baggage` зарезервированы за W3C trace
propagation: `SendMessage` вернёт `ErrReservedHeaderKey`, если один из них
встретится в `PublishRequest.Headers`.

### Консьюмер

```go
type orderHandler struct{}

func (h *orderHandler) ProcessMessage(ctx context.Context, msg kafkax.IncomingMessage) error {
    // ctx содержит OTel-span — используйте его для дочерних операций
    if sig, ok := msg.Headers.Get("signature"); ok {
        // проверка подписи
    }
    return h.store(ctx, msg.Value)
}

cfg.Consumer = kafkax.Consumer{
    Group:             "my-service.group",
    InitialOffset:     kafkax.OffsetEarliest,
    SessionTimeout:    45 * time.Second,
    HeartbeatInterval: 3 * time.Second,
    RebalanceTimeout:  time.Minute,
    CommitInterval:    5 * time.Second,
    HandlerMaxRetries: 3,
    HandlerRetryDelay: time.Second,
}

consumer, err := kafkax.NewKafkaConsumer(cfg)
if err != nil {
    log.Fatal(err)
}

if err := consumer.AddHandler("orders", &orderHandler{}); err != nil {
    log.Fatal(err)
}

if err := consumer.Start(ctx); err != nil { // не блокирует
    log.Fatal(err)
}
defer consumer.Stop()
```

Обработчики регистрируются до `Start`; подписка на топики происходит внутри
`Start`, отдельного шага для неё нет. Повторный `Start` вернёт
`ErrConsumerStarted`, консьюмер после `Stop` не перезапускается.

> **Буферы записей.** `IncomingMessage.Key`, `.Value` и `.Headers` ссылаются на
> буферы franz-go и валидны только на время вызова `ProcessMessage`. Если
> данные нужны дольше — копируйте.

## Гарантии доставки

**At-least-once.** Оффсет отмечается к коммиту только после того, как
обработчик вернул `nil`, а фоновый коммит (`Consumer.CommitInterval`) отправляет
брокеру исключительно отмеченное. Автокоммит по факту чтения не используется:
он двигал бы оффсет до обработки и превращал гарантию в at-most-once при
падении процесса.

Следствие, о котором нужно знать при внедрении: **дубликаты штатны**. Сообщение,
обработанное непосредственно перед падением или ребалансом, будет обработано
повторно. Обработчик обязан быть идемпотентным — exactly-once библиотека не даёт
и дать не может.

## Политика повторов

> **Прочитайте этот раздел целиком перед выкаткой в прод.** Умолчание здесь
> отличается от привычного, и отличается намеренно: по умолчанию отравленное
> сообщение **останавливает свою партицию**, а не пропускается.

Когда `ProcessMessage` возвращает ошибку, сообщение проходит два независимых
этапа.

### Этап 1 — повторы

Управляется `Consumer.HandlerMaxRetries` и `Consumer.HandlerRetryDelay`:

| `HandlerMaxRetries` | Поведение |
|---|---|
| `0` (по умолчанию) | вызвать обработчик один раз, повторов не делать |
| `N > 0` | сделать `N` повторов сверх первого вызова — всего `N+1` вызовов |
| `-1` | повторять бесконечно |

Повторы идут в горутине партиционного воркера и **блокируют партицию**: пока
сообщение повторяется, следующие сообщения этой же партиции ждут. Это цена
сохранения порядка, а не недосмотр. При `-1` партиция заблокирована, пока
обработчик не вернёт `nil` или консьюмер не остановят; этап 2 не наступает
никогда.

Паника внутри `ProcessMessage` перехватывается, оборачивается в
`ErrHandlerPanic` и идёт тем же путём, что обычная ошибка, — воркер не падает.

### Этап 2 — разрешение отказа

Наступает, когда повторы исчерпаны. Исходов два, и выбирает между ними наличие
`Config.OnMessageSkipped`:

**Хук задан и вернул `nil`** — сообщение считается пропущенным: оффсет
отмечается, коммит двигается дальше, партиция продолжает работу. Метрика —
`status="skipped"`.

Возврат `nil` — это заявление «я забрал сообщение»: записал в DLQ, в базу, в
лог. Пустой хук, возвращающий `nil`, — молчаливая потеря данных.

```go
cfg.OnMessageSkipped = func(ctx context.Context, msg kafkax.IncomingMessage, cause error) error {
    return dlq.Publish(ctx, msg, cause) // nil ⇒ оффсет двигается
}
```

**Хук не задан, вернул ошибку или запаниковал** — оффсет НЕ отмечается,
партиция ставится на паузу и остаётся на непрокоммиченном оффсете. Метрика —
`status="error"`, в лог пишется запись уровня `Error`. Сообщение приедет снова
после ребаланса или перезапуска процесса. Остальные партиции продолжают
работать.

### Почему пауза, а не пропуск

Застрявшая партиция видна сразу — по растущему лагу, по
`kafkax.consumer.messages.processed{status="error"}` и по логу. Потерянное
сообщение не видно ничем. Пропуск — явное действие потребителя пакета, а не то,
что случается само.

Пауза снимается в момент, когда партиция назначается консьюмеру снова —
ребалансом или перезапуском процесса, то есть тогда же, когда она начнёт
читаться с проваленного оффсета заново. Отдельного API для снятия паузы нет
намеренно: возобновить чтение, не разобравшись с причиной, значит вернуться в
тот же цикл.

Пока партиция стоит, `kafkax.consumer.partitions.paused` держит ненулевое
значение. Это гейдж, а не счётчик событий: алерт «стоит хотя бы одна партиция»
строится только по нему — разовая запись в логе к моменту дежурства уже уедет
из окна. Записи, которые воркер успел вычитать до паузы, он выбрасывает не
отмечая; их видно как `status="dropped"`.

**Важно.** Пока партиция остаётся за тем же экземпляром и ребаланса не
происходит, отравленное сообщение **не приезжает заново само по себе**. В
однопроцессном развёртывании это значит: до перезапуска (или до входа в группу
второго экземпляра) партиция стоит. Признак — растущий лаг **без** новых записей
уровня `Error`: сообщение до обработчика больше не доходит, поэтому и счётчик
`status="error"` перестаёт расти.

### Три осмысленных конфигурации

| Задача | Настройки |
|---|---|
| Порядок важнее прогресса, потеря недопустима | `HandlerMaxRetries: -1` — партиция ждёт столько, сколько нужно |
| Потеря недопустима, но зависать в обработчике нельзя | `HandlerMaxRetries: N`, `OnMessageSkipped: nil` — партиция встаёт на паузу, инцидент разбирает дежурный. **Это умолчание** |
| Прогресс важнее отдельного сообщения | `HandlerMaxRetries: N` + `OnMessageSkipped` с записью в DLQ и возвратом `nil` — единственный вариант, в котором конвейер не останавливается никогда |

Отдельный случай: если контекст отменён во время паузы между повторами (идёт
остановка консьюмера), сообщение не отмечается и партицию не травит — оно
просто приедет снова. Метрика — `status="cancelled"`, отдельно от `skipped`
именно потому, что коммит здесь не двигается: `skipped` означает потерянное
сообщение, `cancelled` случается на каждом штатном деплое. Длительность под
`cancelled` не пишется — обработка не закончилась, мерить нечего.

## Ошибки продюсера

Sentinel-ошибки делятся по одному признаку — можно ли повторить, не рискуя
дубликатом:

| Ошибка | Значение | Повтор |
|---|---|---|
| `ErrProducerClosed` | сообщение точно не ушло | безопасен |
| `ErrDeliveryTimeout` | могло уйти | создаёт дубликат |
| `ErrDeliveryFailed` | отказ брокера; через `errors.As` достаётся `*kerr.Error` | зависит от кода |

Проверяются через `errors.Is`; текст сообщения частью контракта не является.

## Middleware консьюмера

`AddHandler` принимает опциональную цепочку `ConsumerMiddleware` — обёртку над
`ConsumerHandler` в духе `http.Handler`:

```go
type ConsumerMiddleware func(ConsumerHandler) ConsumerHandler
```

Middleware применяются в порядке перечисления: первый в списке — внешний,
выполняется первым и может решить не звать `next` вовсе.

```go
consumer.AddHandler("orders", &orderHandler{}, loggingMiddleware, metricsMiddleware)
```

Функцию можно передать как обработчик через `ConsumerHandlerFunc`, а собрать
цепочку вне консьюмера — через `kafkax.Chain`.

### MatchKeyMiddleware

Готовая middleware для маршрутизации сообщений с композитным ключом: не тот
адресат тихо пропускается, до `ProcessMessage` дело не доходит.

```go
consumer.AddHandler("events", &orderHandler{},
    kafkax.MatchKeyMiddleware(myTenantID, myExternalBotID))
```

## Композитные ключи

`encoding.EncodeKey` собирает бинарный ключ Kafka-сообщения из нескольких
значений (`uuid.UUID`, `string`, `int64`, `bool`) — без обратного декодирования:
консьюмер знает свои значения и сравнивает, а не разбирает чужой ключ.

```go
import "github.com/alfzs/kafkax/v2/encoding"

// Продюсер
key, err := encoding.EncodeKey(tenantID, externalBotID)
if err != nil {
    return err
}

producer.SendMessage(ctx, kafkax.PublishRequest{Topic: "events", Key: key, Value: payload})

// Консьюмер — вручную, без middleware
func (h *handler) ProcessMessage(ctx context.Context, msg kafkax.IncomingMessage) error {
    if !encoding.MatchKey(msg.Key, myTenantID, myExternalBotID) {
        return nil // не наш адресат
    }
    ...
}
```

`encoding.ValidateKeyLength(key, parts...)` проверяет, что `key` не короче
длины, которую дал бы `EncodeKey(parts...)`, и возвращает `ErrInvalidKey`, если
это не так, — сигнал усечённого или повреждённого сообщения, в отличие от
валидного по длине ключа другого тенанта (для него `MatchKey` просто вернёт
`false`). `MatchKeyMiddleware` делает эту проверку сама.

## Архитектура консьюмера

Цикл опроса раздаёт записи партиционным воркерам: по горутине на назначенную
партицию, порядок внутри партиции сохраняется, разные партиции обрабатываются
параллельно.

```
poll ──┬─► partition 0 ──► ProcessMessage (последовательно)
       ├─► partition 1 ──► ProcessMessage (последовательно)
       └─► partition 2 ──► ProcessMessage (последовательно)
```

Пропускная способность настраивается числом партиций топика, а не параметрами
библиотеки. `Consumer.MessageQueueSize` задаёт, насколько цикл опроса может
обгонять обработку.

У продюсера собственного слоя очередей и воркеров нет: батчинг, упорядочивание
по партиции и лимит памяти делает клиент Kafka. Backpressure — это
`Producer.MaxBufferedRecords` / `MaxBufferedBytes`: при заполнении буфера
`SendMessage` ждёт освобождения места.

## OpenTelemetry

Трассировка и транспортные метрики приходят из
[kotel](https://github.com/twmb/franz-go/tree/master/plugin/kotel) по
семантическим соглашениям OTel: контекст переносится через заголовки, спаны
именуются по конвенции.

```
[HTTP handler span]
    └── [Producer span]  ← SendMessage
            └── [Consumer span]  ← ProcessMessage (ctx содержит parent span)
                    └── [DB query span]
```

Используются глобальные провайдеры `otel.GetTracerProvider()` и
`otel.GetMeterProvider()`. **OTel не обязателен** — при ненастроенных
провайдерах работают no-op реализации: спаны и метрики отбрасываются, в
заголовки ничего не пишется. Функциональность пакета от этого не зависит.

### Доменные метрики

| Метрика | Тип | Атрибуты | Описание |
|---|---|---|---|
| `kafkax.producer.messages.sent` | Counter | `topic` | Сообщения, доставленные в Kafka |
| `kafkax.producer.messages.failed` | Counter | `topic` | Сообщения с ошибкой доставки |
| `kafkax.producer.messages.rejected` | Counter | `reason` | Отбраковка на входе: `empty_topic`, `invalid_headers` |
| `kafkax.producer.message.duration` | Histogram (s) | `topic`, `status` | Длительность `SendMessage` целиком |
| `kafkax.consumer.messages.processed` | Counter | `topic`, `status` | Сообщения с терминальным исходом: `success`, `skipped`, `error`, `cancelled`, `dropped` |
| `kafkax.consumer.message.duration` | Histogram (s) | `topic`, `status` | Время обработчика включая все повторы и паузы; под `cancelled` и `dropped` не пишется |
| `kafkax.consumer.handler.retries` | Counter | `topic` | Неудачные вызовы обработчика, за которыми последовал повтор |
| `kafkax.consumer.fetch.errors` | Counter | `topic` | Эпизоды партиционных ошибок опроса: инкремент на смену состояния, не на каждый опрос |
| `kafkax.consumer.group.errors` | Counter | — | Эпизоды отказа сессии группы: сообщений нет вообще, ни по одной партиции |
| `kafkax.consumer.workers.active` | UpDownCounter | — | Работающие партиционные воркеры |
| `kafkax.consumer.partitions.paused` | UpDownCounter | — | Партиции, стоящие на непрокоммиченном оффсете после отравленного сообщения |
| `kafkax.consumer.panics` | Counter | `site` | Перехваченные паники в горутинах библиотеки |

Длительности — в секундах, как требует OTel; границы бакетов заданы пакетом
явно, потому что умолчание SDK размечено под миллисекунды. Атрибута `partition`
нет ни у одной метрики: он умножает кардинальность на число партиций, не давая
ничего сверх того, что уже есть в спане.

Статусы `messages.processed` разделены по одному признаку — сдвинулся ли коммит
за записью:

| Статус | Коммит | Когда |
|---|---|---|
| `success` | сдвинулся | обработчик вернул `nil` |
| `skipped` | **сдвинулся** | повторы исчерпаны, `OnMessageSkipped` вернул `nil` — сообщение забрал потребитель пакета |
| `error` | нет | повторы исчерпаны без хука, партиция ставится на паузу |
| `cancelled` | нет | отмена контекста застала паузу между повторами; сообщение приедет снова |
| `dropped` | нет | запись вычитана из очереди уже отравленной партиции и выброшена не глядя |

На дашборд «сколько сообщений потеряно» идёт только `skipped`. Остальные
неуспешные исходы означают «сообщение осталось в топике», и суммировать их с
`skipped` значит завышать счёт на каждом деплое и на каждой паузе.
Ненулевой `dropped` показывает масштаб отравления: за одной остановившейся
партицией стоит не одно сообщение, а весь буфер, набранный до паузы.

Атрибут `topic` пишется только для запросов, прошедших валидацию. Значение
приходит из `PublishRequest` и пакетом не ограничено, поэтому отбраковка на
входе учитывается отдельным счётчиком с замкнутым множеством причин: иначе
приложение, подставляющее в топик пользовательский ввод, порождало бы новую
серию на каждое уникальное значение.

Транспортные метрики (соединения, байты, ошибки чтения и записи) регистрирует
kotel под своими именами.

## Конфигурация

### Корневые параметры

| Поле | Env | По умолчанию | Описание |
|---|---|---|---|
| `Brokers` | `KAFKAX_BROKERS` | — | Адреса брокеров `host:port` через запятую. Достаточно одного — остальные обнаруживаются автоматически |
| `ClientID` | `KAFKAX_CLIENT_ID` | — | Идентификатор клиента в логах и метриках брокера |
| `GracefulTimeout` | `KAFKAX_GRACEFUL_TIMEOUT` | `3m` | Общий бюджет на остановку в `Close`/`Stop` |
| `DialTimeout` | `KAFKAX_DIAL_TIMEOUT` | `10s` | Таймаут установки соединения с брокером |

Отдельного поля с протоколом безопасности нет: протокол выводится из
конфигурации. TLS включается флагом `TLS.Enabled`, SASL — непустым
`SASL.Mechanism`.

Программные поля (без env и yaml):

| Поле | Описание |
|---|---|
| `Logger` | `*slog.Logger` библиотеки. При `nil` — `slog.Default()`. Логи franz-go идут туда же на уровне `Debug` |
| `TLSConfig` | Готовый `*tls.Config`. Задан — имеет приоритет над всей секцией `TLS`. Нужен для mTLS с ротацией, кастомного `VerifyPeerCertificate`, сертификатов из памяти |
| `ExtraOpts` | `[]kgo.Opt`, добавляются последними и побеждают всё, что вывела библиотека. Аварийный выход, не замена конфигурации |
| `OnPanic` | Вызывается после восстановления паники в горутине библиотеки: `site` (`handler`, `process_message`, `partition_worker`, `on_message_skipped`), `recovered`, `stack`. Синхронный — не должен блокироваться |
| `OnMessageSkipped` | Судьба сообщения, исчерпавшего повторы. См. «Политика повторов» |

### SASL

Применяется только при непустом `Mechanism`.

| Поле | Env | Описание |
|---|---|---|
| `SASL.Mechanism` | `KAFKAX_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. Регистр не важен |
| `SASL.Username` | `KAFKAX_SASL_USERNAME` | Имя пользователя |
| `SASL.Password` | `KAFKAX_SASL_PASSWORD` | Пароль. Не попадает в логи: `SASL` реализует `slog.LogValuer` и `fmt.Stringer` |

### TLS

| Поле | Env | По умолчанию | Описание |
|---|---|---|---|
| `TLS.Enabled` | `KAFKAX_TLS_ENABLED` | `false` | Включает TLS |
| `TLS.CACertPath` | `KAFKAX_TLS_CA_CERT_PATH` | — | PEM-файл CA. Пусто — системный trust store |
| `TLS.ClientCertPath` | `KAFKAX_TLS_CLIENT_CERT_PATH` | — | Клиентский сертификат для mTLS. Только вместе с ключом |
| `TLS.ClientKeyPath` | `KAFKAX_TLS_CLIENT_KEY_PATH` | — | Клиентский ключ для mTLS |
| `TLS.ServerName` | `KAFKAX_TLS_SERVER_NAME` | — | Имя, по которому проверяется сертификат брокера. Нужно при подключении по IP или через прокси |
| `TLS.InsecureSkipVerify` | `KAFKAX_TLS_INSECURE_SKIP_VERIFY` | `false` | Отключает проверку сертификата целиком. Только для локальной отладки — делает соединение уязвимым к MITM. Библиотека пишет `WARN` при каждом создании такого клиента |

### Продюсер

| Поле | Env `KAFKAX_PRODUCER_…` | По умолчанию | Описание |
|---|---|---|---|
| `RequiredAcks` | `REQUIRED_ACKS` | `-1` | `-1` все реплики ISR, `1` только лидер, `0` без подтверждения. `1` и `0` требуют `EnableIdempotence: false` — иначе `Validate` вернёт ошибку, а не отключит идемпотентность молча |
| `EnableIdempotence` | `ENABLE_IDEMPOTENCE` | `true` | Брокер дедуплицирует повторные отправки и держит порядок в партиции при нескольких запросах в полёте |
| `MaxInflight` | `MAX_INFLIGHT` | `5` | Неподтверждённых produce-запросов на брокера. Применяется **только** при `EnableIdempotence: false`, где безопасно лишь `1` |
| `MaxRetries` | `MAX_RETRIES` | `3` | Повторов доставки одной записи при повторяемой ошибке брокера |
| `AckTimeout` | `ACK_TIMEOUT` | `5s` | Таймаут подтверждения записи на стороне брокера. Не путать с `MessageTimeout` |
| `RetryBackoff` | `RETRY_BACKOFF` | `100ms` | Пауза между повторами. Фиксированная, без экспоненциального роста |
| `Linger` | `LINGER` | `0s` | Ожидание перед отправкой неполного батча. `SendMessage` всё равно инициирует немедленную отправку по затронутым партициям |
| `BatchBytes` | `BATCH_BYTES` | `1048576` | Верхняя граница размера батча |
| `CompressionType` | `COMPRESSION_TYPE` | `lz4` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `MaxBufferedRecords` | `MAX_BUFFERED_RECORDS` | `10000` | Записей в памяти до подтверждения. Это и есть backpressure — лимит общий на клиента |
| `MaxBufferedBytes` | `MAX_BUFFERED_BYTES` | `0` | Тот же лимит в байтах. `0` — без лимита |
| `MessageTimeout` | `MESSAGE_TIMEOUT` | `30s` | Полный бюджет одного `SendMessage`. Минимум — `1s` |
| `FlushTimeout` | `FLUSH_TIMEOUT` | `1m` | Верхняя граница финального flush при `Close`. Реально — `min(FlushTimeout, остаток GracefulTimeout)` |

### Консьюмер

| Поле | Env `KAFKAX_CONSUMER_…` | По умолчанию | Описание |
|---|---|---|---|
| `Group` | `GROUP` | — | Идентификатор consumer group. Обязателен |
| `InitialOffset` | `INITIAL_OFFSET` | `earliest` | Откуда читать группу без сохранённого оффсета: `earliest` или `latest` |
| `MinBytes` | `MIN_BYTES` | `1` | Минимальный объём данных в ответе на fetch |
| `MaxBytes` | `MAX_BYTES` | `52428800` | Максимальный объём данных в ответе |
| `MaxPartitionBytes` | `MAX_PARTITION_BYTES` | `1048576` | Максимум с одной партиции в ответе |
| `MaxWait` | `MAX_WAIT` | `500ms` | Сколько брокер ждёт накопления `MinBytes` |
| `SessionTimeout` | `SESSION_TIMEOUT` | `45s` | После какого молчания координатор считает консьюмера мёртвым |
| `HeartbeatInterval` | `HEARTBEAT_INTERVAL` | `3s` | Период heartbeat. Не более `SessionTimeout/3` |
| `RebalanceTimeout` | `REBALANCE_TIMEOUT` | `1m` | Сколько координатор ждёт отдачи партиций. Должен превышать максимальное время обработки батча |
| `IsolationLevel` | `ISOLATION_LEVEL` | `read_committed` | `read_committed` или `read_uncommitted` |
| `MaxPollRecords` | `MAX_POLL_RECORDS` | `500` | Верхняя граница числа записей за один опрос |
| `MessageQueueSize` | `MESSAGE_QUEUE_SIZE` | `100` | Ёмкость канала партиционного воркера |
| `CommitInterval` | `COMMIT_INTERVAL` | `5s` | Период фоновой отправки отмеченных оффсетов. Влияет на окно переобработки, но не на гарантию at-least-once |
| `HandlerMaxRetries` | `HANDLER_MAX_RETRIES` | `0` | `0` без повторов, `N` — `N` повторов, `-1` бесконечно. См. «Политика повторов» |
| `HandlerRetryDelay` | `HANDLER_RETRY_DELAY` | `1s` | Пауза между повторами. Обязателен при `HandlerMaxRetries != 0` |

### Валидация

`Config.Validate()` проверяет обе секции сразу — для приложения, создающего из
одного `Config` и продюсер, и консьюмер. Конструкторы вызывают проверку только
своей роли: продюсеру незачем требовать `consumer.group`, консьюмеру —
`producer.flush_timeout`. Поэтому `Config`, прошедший `NewKafkaProducer`, может
не пройти `Validate`.

Ошибки собираются все разом через `errors.Join`, а не возвращаются по первой:
иначе неполный конфиг чинится по одному полю за перезапуск. Список
разворачивается через `errors.Unwrap() []error`.

## Загрузка конфигурации из env

Теги `env` совместимы с [cleanenv](https://github.com/ilyakaznacheev/cleanenv):

```go
var cfg kafkax.Config
if err := cleanenv.ReadEnv(&cfg); err != nil {
    log.Fatal(err)
}
```

```env
KAFKAX_BROKERS=kafka-1.example.com:9092,kafka-2.example.com:9092
KAFKAX_CLIENT_ID=my-service
KAFKAX_CONSUMER_GROUP=my-service.group
KAFKAX_TLS_ENABLED=true
KAFKAX_SASL_MECHANISM=SCRAM-SHA-512
KAFKAX_SASL_USERNAME=my-service-user
KAFKAX_SASL_PASSWORD=secret
```

## Graceful shutdown

**Продюсер.** `Close()` перестаёт принимать новые отправки, дожидается
завершения тех, что уже в полёте, и досылает буферизованное финальным flush.
`GracefulTimeout` — общий бюджет на обе фазы, `FlushTimeout` ограничивает сверху
только вторую. Повторный вызов безопасен.

**Консьюмер.** `Stop()` останавливает цикл опроса, дожидается партиционных
воркеров в пределах `GracefulTimeout`, коммитит отмеченное и покидает группу.
Повторный вызов безопасен.

Оба клиента реагируют на отмену контекста, переданного в `Start`, но явные
`Close`/`Stop` предпочтительнее: только они гарантируют фазу дообработки.

## Подмена в тестах

`MessageProducer` и `MessageConsumer` объявлены в пакете, чтобы вызывающий код
подменял клиента в тестах, не поднимая брокер:

```go
type MessageProducer interface {
    SendMessage(ctx context.Context, req PublishRequest) error
    Close() error
}
```
