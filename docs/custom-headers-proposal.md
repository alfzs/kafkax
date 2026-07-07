# Предложение: пользовательские заголовки сообщений (2026-07-07)

> **Статус: реализовано (2026-07-07).**

Задача — дать пользователю библиотеки возможность класть произвольные
Kafka-заголовки в сообщение (пример из обсуждения: подпись сообщения, но
так же применимо к `content-type`, версии схемы, `causation-id` и т.п.) и
читать их на стороне консьюмера. Ниже — идиоматичный вариант без оглядки на
обратную совместимость, так как публичное API уже допускает breaking-change
в этом релизном цикле.

## 1. Тип заголовка — не `map[string]string`

Kafka-заголовок — это упорядоченная пара `(string, []byte)`, допускающая
повторяющиеся ключи (`kafka.Header{Key string, Value []byte}` в
confluent-kafka-go). `map[string]string` не подходит: теряет порядок,
дубликаты ключей и бинарные значения — а подпись сообщения (HMAC, Ed25519)
почти всегда бинарная, и кодировать её в base64 только ради того, чтобы
влезть в `string`, — лишний слой.

Вводим собственный тип, структурно зеркальный `kafka.Header`, но не
зависящий от confluent-kafka-go в публичном API (если клиент когда-нибудь
сменится, публичный тип останется стабильным):

```go
// Header — заголовок Kafka-сообщения.
type Header struct {
	Key   string
	Value []byte
}

// Headers — упорядоченный список заголовков с поиском по ключу.
// Порядок и дубликаты ключей сохраняются, как в самом протоколе Kafka.
type Headers []Header

// Get возвращает значение первого заголовка с данным ключом.
func (h Headers) Get(key string) ([]byte, bool) {
	for _, kv := range h {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return nil, false
}
```

Это тот же паттерн, что `net/http.Header` — тип-обёртка над
списком/мапой с методом `Get`, а не голая мапа в сигнатурах.

## 2. Producer: `PublishRequest.Headers`

```go
type PublishRequest struct {
	TenantID uuid.UUID
	Topic    string
	Key      []byte
	Value    []byte
	Headers  Headers
}
```

### Коллизия с OTel-заголовками — fail fast, не молчаливая перезапись

`produce()` инжектирует W3C trace context через `propagator.Inject`, который
использует `kafkaHeaderCarrier.Set` — а `Set` **перезаписывает** значение при
совпадении ключа (`otel.go:24-31`). Если пользователь случайно назовёт свой
заголовок `traceparent`/`tracestate`/`baggage`, он будет молча затёрт без
какой-либо диагностики — трудноуловимый баг.

Правильное поведение: заранее знать зарезервированные имена и **вернуть
ошибку** из `SendMessage`, если пользователь пытается их использовать,
вместо того чтобы разрешать тихую потерю данных:

```go
var reservedHeaderKeys = map[string]struct{}{
	"traceparent": {},
	"tracestate":  {},
	"baggage":     {},
}

func validateHeaders(headers Headers) error {
	for _, h := range headers {
		if _, reserved := reservedHeaderKeys[h.Key]; reserved {
			return fmt.Errorf("header key %q is reserved for trace propagation", h.Key)
		}
	}
	return nil
}
```

Вызывается в начале `SendMessage`, рядом с уже существующей проверкой
`p.stopping.Load()`.

### Порядок сборки заголовков в `produce()`

```go
headers := make([]kafka.Header, 0, len(msg.Headers)+2)
for _, h := range msg.Headers {
	headers = append(headers, kafka.Header{Key: h.Key, Value: h.Value})
}
// propagator.Inject добавляет traceparent/tracestate НЕ пересекаясь
// с пользовательскими ключами — коллизии отсеяны на этапе validateHeaders.
p.propagator.Inject(ctx, newKafkaHeaderCarrier(&headers))
```

## 3. Consumer: заголовки — часть параметра хендлера, не значение в `ctx`

Заголовки — это данные сообщения (как `Key`/`Value`), а не сквозная
инфраструктурная метадата уровня trace span/deadline. Класть бизнес-данные
в `context.Value` — известный антипаттерн в Go: сам пакет `context`
оговаривает, что `Value` предназначен только для request-scoped данных,
пересекающих границы API, а не для передачи обычных параметров функции.
Библиотека уже кладёт в `ctx` span — это правильное использование (span
действительно сквозной и инфраструктурный), но заголовки сообщения — нет.

Раз меняем API без оглядки на совместимость, правильный шаг — расширить
параметр хендлера явным полем, а не изобретать `HeadersFromContext(ctx)`.
Заодно решает и другую проблему: `ProcessMessage` уже никогда не узнает
partition/offset, если понадобится в будущем — сейчас самое время завести
для этого один симметричный тип, а не собирать параметры по одному с каждым
новым breaking-change.

```go
// IncomingMessage — сообщение Kafka, переданное в consumerHandler.
type IncomingMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   Headers
}

type consumerHandler interface {
	ProcessMessage(ctx context.Context, msg IncomingMessage) error
}
```

`handleMessage`/`processMessage` в `consumer.go` конвертируют `*kafka.Message`
в `IncomingMessage` один раз, на границе пакета — остальной код (retry-цикл,
метрики, трейсинг) не меняется, т.к. работает с `*kafka.Message` как и
раньше и лишь оборачивает его перед вызовом `handler.ProcessMessage`.

## 4. Что сознательно не делаем

- **Не** `map[string]string` для заголовков — теряет порядок, дубликаты,
  бинарные значения (см. п. 1).
- **Не** протаскиваем `kafka.Header`/`*kafka.Message` напрямую в публичное
  API — это утечка типа вендора в контракт библиотеки; при смене клиента
  (например, на `segmentio/kafka-go` или `franz-go`) публичное API пришлось
  бы менять повторно.
- **Не** кладём заголовки в `ctx.Value` — решает задачу "не ломать
  интерфейс", но раз ломать уже можно, это не идиоматичное, а компромиссное
  решение (антипаттерн ради обратной совместимости, которая здесь не нужна).
- **Не** делаем тихую перезапись при коллизии с `traceparent`/`tracestate`/
  `baggage` — по умолчанию Go предпочитает явную ошибку скрытой потере
  данных.

## 5. Объём изменений (для оценки трудозатрат)

- `otel.go` или новый `headers.go`: типы `Header`/`Headers`, метод `Get`,
  `reservedHeaderKeys`, `validateHeaders`.
- `producer.go`: `PublishRequest.Headers`, вызов `validateHeaders` в
  `SendMessage`, сборка `headers` в `produce()` из `msg.Headers` перед
  `propagator.Inject`.
- `consumer.go`: тип `IncomingMessage`, сигнатура `consumerHandler`,
  конвертация `*kafka.Message` → `IncomingMessage` в `handleMessage`.
- `README.md`: обновить пример продюсера и консьюмера, задокументировать
  зарезервированные ключи заголовков.
- Тесты: `producer_test.go` (заголовки в `PublishRequest`, коллизия с
  `traceparent`), `consumer_test.go`/тестовые хендлеры (новая сигнатура
  `ProcessMessage`), `helpers_test.go` (моки хендлеров).
