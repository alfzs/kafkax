# Аудит зависимостей (2026-07-07)

> **Статус: находка исправлена (2026-07-07).**

Проверка прямых зависимостей `go.mod` на актуальность и статус
поддержки (pkg.go.dev / GitHub releases).

```
require github.com/confluentinc/confluent-kafka-go v1.9.2

require (
	github.com/google/uuid v1.6.0
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/metric v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
	google.golang.org/protobuf v1.36.10
)
```

## Находка

### ✅ `github.com/confluentinc/confluent-kafka-go v1.9.2` — устаревшая ветка v1 — исправлено

Это неверсионированная v1-ветка модуля, фактически замороженная примерно
с 2022 года. Активно поддерживаемая ветка Confluent —
`github.com/confluentinc/confluent-kafka-go/v2`, сейчас на v2.32.x с
регулярными релизами в 2026 году: более новый вшитый librdkafka, поддержка
KIP-848 (next-gen consumer group protocol), протокольные и security-фиксы.

Библиотека оборачивает librdkafka через cgo, поэтому отставание на v1 — это
не просто отставание по Go-API, а ~3+ года непереданных фиксов самого
librdkafka.

**Фикс:** зависимость переведена на `github.com/confluentinc/confluent-kafka-go/v2 v2.15.0`:
- путь импорта во всех файлах (`consumer.go`, `producer.go`, `otel.go`,
  `otel_test.go`) обновлён на `.../v2/kafka`;
- `go.mod`/`go.sum` обновлены через `go get .../v2@latest` + `go mod tidy`;
- используемая в коде API-поверхность (`ConfigMap`, `NewConsumer`,
  `NewProducer`, `Message`/`Header`/`TopicPartition`/`Event`, `Error`,
  `Consumer.Poll`/`CommitMessage`, `Producer.Produce`) между v1 и v2 не
  менялась, так что построчных правок логики не потребовалось;
- новый consumer-group протокол KIP-848 не включён — используется
  протокол по умолчанию (`classic`), совместимый с прежним поведением;
- `README.md` (команда `go get`) обновлён на новый путь;
- build/vet/test пройдены на `v2.15.0`.

## Без замечаний

- `go.opentelemetry.io/otel`, `/metric`, `/trace` — `v1.44.0`, последний
  релиз на момент аудита.
- `github.com/google/uuid v1.6.0` — последний тег (январь 2024), библиотека
  стабильна и зрелая, новой версии для перехода нет.
- `google.golang.org/protobuf v1.36.10` — на один патч-релиз позади
  (`v1.36.11`), отдельного действия не требует.
- Косвенные зависимости (`xxhash`, `go-logr`, `otel/auto/sdk`) подтянуты
  транзитивно через otel/kafka и актуальны для этих версий.
