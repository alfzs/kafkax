# Аудит тестов (2026-07-07)

> **Статус: все находки исправлены (2026-07-07).**

Аудит тестового покрытия (`config_test.go`, `producer_test.go`,
`consumer_test.go`, `headers_test.go`, `otel_test.go`, `helpers_test.go`,
`encoding/proto.go`) по трём направлениям: качество unit-тестов и пробелы в
покрытии, изоляция/наличие integration-тестов, утечки горутин и потокобезопасность.
Каждое направление проверено отдельным параллельным проходом.

Покрытие до фиксов: **47.9%** (`go test . -short -cover`), пакет `encoding` —
**`[no test files]`**. После фиксов: **62.9%** (`go test ./... -short -cover`),
`encoding` — **100%**. `go test -race ./...` проходит без гонок как до, так и
после (`-short`, полный прогон и `-tags=integration`).

## Находки

### 1. ✅ `handleMessage` (`consumer.go:500`) — retry/skip/commit-логика не выполнялась ни одним тестом — исправлено

Добавлен `TestKafkaConsumer_HandleMessage_RetriesAndSkipsAfterMaxRetries`
(`consumer_test.go`): вызывает `c.handleMessage(ctx, &kafka.Message{...})`
напрямую, без брокера, с хендлером, вернувшим ошибку через
`mockHandler.returnErr` (ранее объявленное, но неиспользуемое поле). Тест
запускает `handleMessage` в отдельной горутине с таймаутом-предохранителем,
т.к. `CommitMessage` без брокера синхронно блокируется на "Local: Waiting for
coordinator" вплоть до `Consumer.SessionTimeout` — для этого добавлен
`fastCommitConfig()` (`helpers_test.go`) с уменьшенными
`SessionTimeout`/`SocketTimeout`, чтобы тест оставался быстрым.

### 2. ✅ Нет round-trip теста для пользовательских заголовков (produce → consume) — исправлено

Добавлен `TestKafkaConsumer_HandleMessage_HeadersRoundTrip`
(`consumer_test.go`): строит `kafka.Header` через `toKafkaHeaders` (тот же
путь, что использует `produce()`), передаёт в `handleMessage` и проверяет
через `mockHandler.lastMessage()` (новый метод, добавлен захват последнего
полученного `IncomingMessage`), что `fromKafkaHeaders` восстановил
пользовательский заголовок в `IncomingMessage.Headers`. Проверяет границу
`produce()`/`handleMessage()` целиком, а не `toKafkaHeaders`/`fromKafkaHeaders`
по отдельности (это уже покрыто в `headers_test.go`).

### 3. ✅ `TestKafkaProducer_ContextCancel_TriggersShutdown` ничего не проверяла + не было аналога для консьюмера — исправлено

`TestKafkaProducer_ContextCancel_TriggersShutdown` (`producer_test.go`)
переведена с `t.Log("предупреждение...")` на опрос с retry (до 2s): гонка
между `cancel()` и асинхронной горутиной-наблюдателем (`producer.go:224`)
устранена явным ожиданием, а не маскировкой через лог. Добавлен
`TestKafkaConsumer_StartContextCancel_StopsLoopsWithoutClose`
(`consumer_test.go`) — проверяет заявленное в докстринге `Start` поведение:
в отличие от продюсера, у консьюмера нет горутины-наблюдателя, поэтому после
отмены `ctx` loops останавливаются (drain), но `consumer.Close()` ещё не
вызван; последующий явный `Stop()` при этом отрабатывает штатно в пределах
`GracefulTimeout`.

### 4. ✅ Путь ошибки регистрации gauge непокрыт — исправлено

Добавлены `TestNewKafkaProducer_GaugeRegistrationError`/
`TestNewKafkaConsumer_GaugeRegistrationError` (`otel_gauge_test.go`) с
`failingGaugeMeter`/`failingGaugeMeterProvider` — обёрткой над `noop.Meter`,
форсирующей ошибку `Int64ObservableGauge`. Обнаружено и обойдено важное
свойство `go.opentelemetry.io/otel`: `otel.SetMeterProvider` делегирует
провайдер один раз и необратимо для всего процесса
(`internal/global`: `delegateMeterOnce sync.Once`) — восстановление
"оригинального" провайдера после теста не отменяет уже сделанную делегацию и
сломало бы метрики всех остальных тестов пакета. Поэтому подмена глобального
`MeterProvider` выполняется в изолированном subprocess'е (`os.Args[0]` +
`-test.run`), а не в основном тестовом процессе — классический Go-паттерн для
тестирования необратимого глобального состояния.

### 5. ✅ `encoding/proto.go` — не было тестового файла вообще — исправлено

Добавлен `encoding/proto_test.go`: happy path через
`wrapperspb.StringValue` (готовый `proto.Message` из
`google.golang.org/protobuf/types/known/wrapperspb`, уже входящего в
существующую зависимость `google.golang.org/protobuf` — новый пакет не
добавлялся), невалидные байты (`0xFF 0xFF 0xFF`, битый varint) и
nil/пустой вход. Покрытие пакета: `[no test files]` → **100%**.

### 6. ✅ `buildProducerKafkaConfig`/`buildConsumerKafkaConfig` не тестировались напрямую — исправлено

Добавлены `TestBuildProducerKafkaConfig`/`TestBuildConsumerKafkaConfig`
(`config_test.go`): прямые проверки `kafka.ConfigMap` — условное включение
`sasl.mechanisms`/`sasl.username`/`sasl.password` только при
SASL_PLAINTEXT/SASL_SSL, и корректный маппинг `compression.type`,
`linger.ms`, `bootstrap.servers`, `group.id`, `enable.auto.commit`,
`auto.offset.reset`.

### 7. ✅ Качество ассертов: `err != nil` вместо проверки конкретной ошибки — исправлено

`TestKafkaProducer_SendMessage_ContextCanceled` (`producer_test.go`) теперь
проверяет `errors.Is(err, context.Canceled)` вместо голого `err != nil`.
Изменение только тестовое: `SendMessage` уже оборачивает `ctx.Err()` через
`%w` (исправлено ранее, `docs/context-audit.md`, находка 3) — тест просто
перестал молчаливо мириться с деградацией этой гарантии.

### 8. ✅ Граница retry в `handleMessage` не была зафиксирована тестом — исправлено

Закрыто тем же тестом, что и находка 1
(`TestKafkaConsumer_HandleMessage_RetriesAndSkipsAfterMaxRetries`): явно
проверяется, что при `HandlerMaxRetries=2` `ProcessMessage` вызывается
**ровно 2 раза**, а не 3 и не 1.

### 9. ✅ Не было отдельных unit/integration тестов — частично исправлено (инфраструктура с нуля)

Добавлены `//go:build integration` для двух тестов, содержащих реальные
многосекундные ожидания против недоступного брокера
(`TestKafkaProducer_SendMessage_BrokerUnavailable` →
`producer_integration_test.go`, `TestKafkaConsumer_FullLifecycle` →
`consumer_integration_test.go`), и `Makefile` с целями `test`, `test-race`,
`test-integration`, `cover`. Это не отменяет суть находки: подавляющее
большинство тестов по-прежнему полагается на поведение real librdkafka при
недоступном брокере, а не на mock/testcontainers — полноценная mock-based
unit-инфраструктура осталась вне рамок этого прохода (см. исходную
формулировку находки — это создание отсутствующей инфраструктуры, а не
починка сломанной).

### 10. ✅ Не было тестов на конкурентный доступ к одному инстансу — исправлено

Добавлены `TestKafkaProducer_ConcurrentCloseAndSendMessage`
(`producer_test.go`) и `TestKafkaConsumer_ConcurrentStop` (`consumer_test.go`)
— конкурентные вызовы `Close()`/`SendMessage()` и `Stop()` из нескольких
горутин на одном инстансе, проверено `go test -race`.

### 11. ✅ `goleak.VerifyTestMain` не использовался — исправлено

Добавлена зависимость `go.uber.org/goleak` (`go get`, `go.mod`/`go.sum`) и
`TestMain` (`main_test.go`) с `goleak.VerifyTestMain(m)`. Оба намеренно
неприсоединяемых паттерна (наблюдатель за `ctx` в `NewKafkaProducer`,
`producer.go:224`; хелпер `wg.Wait(); close(done)` в `Close()`/`Stop()`)
завершаются к моменту возврата из `Close()`/`Stop()`, которые вызываются
через `t.Cleanup` в `mustNewProducer`/`mustNewConsumer` — проверено, что
`goleak` не падает ни в `-short`, ни в полном, ни в `-tags=integration`
прогоне.

## Без замечаний

- Табличные тесты во всех файлах последовательно передают `name` в `t.Run`.
- `TestKafkaProducer_Close_Idempotent`/`TestKafkaConsumer_Stop_Idempotent`
  корректно покрывают идемпотентность после переименования
  `isStopping`/`isStarted`.
- Нет тестов, читающих приватные поля напрямую — проверяется наблюдаемое
  поведение, а не детали реализации (сохранено и в новых тестах: доступ к
  `handleMessage`/`consumer.go` через package-internal вызов метода, а не
  через чтение полей).
- `t.Skip`/`t.Skipf` используется только для легитимных случаев
  ("нет брокера", `-short`), отключённых/сломанных тестов нет.
- Нет пакетных мутабельных переменных, от которых тесты могли бы зависеть по
  порядку выполнения (`headers.go:33` `reservedHeaderKeys` — read-only).
- Каждый тест, создающий Consumer/Producer, освобождает его через
  `t.Cleanup`/`defer` — утечек между тестами не найдено.
- `go test -race ./...` (`-count=1 -v -race -timeout 180s`) — все тесты
  проходят, гонок не обнаружено.

## Рекомендованный порядок исправления

1. ✅ Тест `handleMessage` напрямую (без брокера) на retry/skip/commit —
   находка 1.
2. ✅ Round-trip тест пользовательских заголовков — находка 2.
3. ✅ Починить `TestKafkaProducer_ContextCancel_TriggersShutdown`, добавить
   аналог для `KafkaConsumer.Start(ctx)` — находка 3.
4. ✅ `goleak.VerifyTestMain` в `TestMain` — находка 11.
5. ✅ Остальное (5, 6, 7, 8, 10) — исправлено. Находка 9 закрыта частично
   (см. находку 9 выше) — полная mock-based unit-инфраструктура осознанно
   оставлена за рамками этого прохода.
