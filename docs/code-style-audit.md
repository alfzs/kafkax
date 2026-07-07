# Аудит стиля кода (2026-07-07)

> **Статус: все находки исправлены (2026-07-07).**

Аудит стиля кода всей библиотеки (`config.go`, `consumer.go`, `producer.go`,
`otel.go` и тесты) по пяти независимым направлениям: control flow,
дизайн функций, объявления переменных/литералов, работа со строками/типами,
организация кода. Каждое направление проверено отдельным параллельным
проходом.

## Control flow

### 1. ✅ `sc`/`sendErr` не заскоуплены в `if` — исправлено

- `producer.go` (`SendMessage`) — `sc := trace.SpanFromContext(ctx).SpanContext()`
  объявлялся отдельной строкой и использовался только в следующем `if`.
- `producer_test.go` (`TestKafkaProducer_Close_DrainsWorkers` и смежный тест) —
  `sendErr := p.SendMessage(...)` объявлялся отдельно от последующего
  `if sendErr == nil { ... } else { ... }`.

**Фикс:** обе переменные заскоуплены в инициализатор `if`:
`if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() { ... }` и
`if sendErr := p.SendMessage(...); sendErr == nil { ... } else { ... }`.

## Дизайн функций

### 2. ✅ `SendMessage` — 5 параметров — исправлено

`producer.go` — `SendMessage(ctx context.Context, tenantID uuid.UUID, topic string, key, value []byte) error`
превышал лимит в 4 параметра.

**Фикс:** добавлен `PublishRequest{TenantID, Topic, Key, Value}`, сигнатура
изменена на `SendMessage(ctx context.Context, req PublishRequest) error`.
Обновлены все вызовы в `producer_test.go` и пример в `README.md`. Это
breaking-change публичного API, отражённый в мажорной/минорной версии при
следующем релизе.

### 3. ✅ `NewKafkaProducer`/`NewKafkaConsumer` — смешение уровней абстракции — исправлено

Оба конструктора в одном теле строили `kafka.ConfigMap` из `Config`,
регистрировали OTel-инструменты и запускали фоновую горутину — три разных
уровня ответственности в одной функции длиной 100+ строк.

**Фикс:** выделены чистые функции `buildProducerKafkaConfig`/
`buildConsumerKafkaConfig` (Config → kafka.ConfigMap) и
`newProducerMetrics`/`newConsumerMetrics` (Meter → метрики). Сами
конструкторы теперь последовательно вызывают: валидация → сборка конфига →
создание клиента → метрики → сборка структуры → запуск фоновой горутины —
без изменения поведения.

## Организация кода

### 4. ✅ Методы воркеров оторваны от объявления типа — исправлено

`(w *tenantWorker) updateActivity/getLastActivity` и
`(w *partitionWorker) updateActivity/getLastActivity` были объявлены в самом
конце `producer.go`/`consumer.go`, за сотни строк от объявления своих типов
(`tenantWorker`, `partitionWorker` — оба в начале файла).

**Фикс:** методы перенесены сразу после объявления соответствующего типа —
группировка "тип + конструктор + методы" соблюдена.

### 5. ✅ `Message` экспортирован без необходимости — исправлено

`producer.go` — тип `Message` (единица очереди воркера тенанта) был
экспортирован, хотя используется только внутри пакета и не входит в
документированный публичный API (`README.md` оперирует только `SendMessage`).

**Фикс:** тип переименован в `message` (неэкспортированный), обновлены все
внутренние ссылки (`tenantWorker.messageChan`, `SendMessage`, `handleMessage`,
`produce`, `getOrCreateWorker`).

### 6. ✅ Отсутствовал package doc comment — исправлено

Ни один файл не содержал `// Package kafkax ...` — стандартный godoc-вход в
библиотеку отсутствовал.

**Фикс:** добавлен package doc comment над `package kafkax` в `config.go`.

## Работа со строками

### 7. ✅ `%s` вместо `%q` для строковых идентификаторов в error-путях — исправлено

- `config.go:38,41` (`Validate`) — `security.protocol=%s`.
- `consumer.go` (`AddHandler`) — `handler for topic %s already registered`.

Без кавычек границы значения не видны в логах/ошибках (например, пустая
строка или строка с пробелами неотличима от отсутствия значения).

**Фикс:** оба места переведены на `%q`.

## Без замечаний

- Слайсы/мапы инициализируются явно (`make(...)`), нигде не встречается nil
  слайс/мапа, отдаваемая наружу.
- Композитные литералы везде используют именованные поля.
- `:=` и `var` использованы согласно неймингу интента (zero-value → `var`).
- `any`/`reflect` в библиотеке не используются.
- Блант- и дот-импорты отсутствуют.
- Остальные конструкторы, геттеры и хелперы укладываются в ≤4 параметра и
  порядок `ctx` первым.

## Не в скоупе этого прохода

Более крупные изменения, предложенные в ходе аудита, но не покрывающие
единичные конкретные находки (например, `otelslog`-мост вместо ручного
`trace_id` в логах), уже зафиксированы отдельно в
[observability-audit.md](./observability-audit.md) и намеренно не
дублируются здесь.
