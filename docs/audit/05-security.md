# Безопасность

Критических находок нет. Разбор охватывает TLS/SASL, обращение с секретами, обработку
недоверенного ввода, границы потребления памяти.

---

## С1 [СЕРЬЁЗНО] SASL PLAIN поверх незашифрованного соединения — без ошибки и без предупреждения

**Где:** `opts.go:43-50` — `kgo.SASL(mech)` добавляется безусловно, независимо от того, вернул ли
`tlsConfig` nil. `config.go:343-367` (`saslErrors`) и `config.go:369-380` (`tlsErrors`) пару
«SASL включён / TLS выключен» не проверяют. `config.go:156` — `TLS.Enabled` по умолчанию `false`;
SASL включается одним лишь непустым `Mechanism`.

franz-go для PLAIN шлёт `zid\0user\0pass` открытым текстом (`pkg/sasl/plain/plain.go:51`).

**Асимметрия:** `InsecureSkipVerify` получает WARN (`opts.go:112-115`), а «пароль открытым
текстом» — ничего.

**Чем грозит.** Опечатка в env (`KAFKA_TLS_ENABLED` не выставлен) приводит к отправке пароля в
открытом виде, и ни одного сигнала об этом нет.

**Исправление.** WARN в `commonOpts` при `SASL.enabled() && tlsCfg == nil`, жёстче для PLAIN; либо
ошибка валидации с явным опт-аутом `SASL.AllowPlaintext`.

*Проверено вручную: `opts.go:34-50` — подтверждается.*

---

## У1 [УМЕРЕННО] Пароль утекает через `%#v` и `json.Marshal`; у `Config` нет `LogValue`

**Где:** `config.go:108-144`.

Редакция держится только на `fmt.Stringer` + `slog.LogValuer`. `%#v` (GoString) игнорирует
`Stringer`; `json.Marshal` не знает ни о том, ни о другом — json-тегов и `MarshalJSON` у `SASL` нет.

`config.go:116-117` — комментарий обещает редакцию «при логировании `Config` целиком», но у
`Config` **нет** `LogValue()`. `slog.Any("config", cfg)` с `JSONHandler` даёт
`!ERROR:json: unsupported type: func(...)` — пароль не утёк лишь потому, что `encoding/json`
спотыкается о поля-функции `OnPanic`/`OnMessageSkipped`. **Уберут их — станет утечкой.**
`TextHandler` редактирует корректно.

Все три метода (`LogValue`, `String`, `redactedOrEmpty`) — **0% покрытия** (см.
[06-tests.md](06-tests.md) К3).

**Исправление.** `SASL.Format`/`GoString`, `SASL.MarshalJSON`, `Config.LogValue`, тесты на все три.

*Проверено вручную: `config.go:106-134` — подтверждается.*

---

## У2 [УМЕРЕННО] У консьюмера нет байтового потолка на очереди воркеров

**Где:** `consumer.go:492` — `make(chan []*kgo.Record, MessageQueueSize)` на каждую партицию.

Записи не копируются: `IncomingMessage` алиасит буферы franz-go, `Key`/`Value`/`Headers` резидентны,
пока батч в канале. Граница потребления ≈ `партиции × MessageQueueSize × MaxPartitionBytes` =
30 × 100 × 1 MiB ≈ **3 ГБ** на умолчаниях.

У продюсера для симметричного риска есть `MaxBufferedBytes` (`opts.go:176-178`), у консьюмера
аналога нет. Блокирующий `dispatch` (`:471-477`) тормозит опрос только когда память **уже** набрана.

**Исправление.** Как минимум задокументировать формулу рядом с `MessageQueueSize`
(`config.go:269-271`); при желании — байтовый семафор.

---

## М1 [МЕЛОЧЬ] Поля размера fetch не валидируются

**Где:** `config.go:449-504` (`consumerErrors`) — `MinBytes`, `MaxBytes`, `MaxPartitionBytes` не
проверяются вовсе, хотя продюсерский `BatchBytes` проверяется на `> 0` (`config.go:415-417`).
franz-go тоже не проверяет: границ нет, есть только молчаливый клэмп `maxPartBytes > maxBytes`
(`pkg/kgo/config.go:233-236`). Ноль проходит и `Validate`, и конструктор.

Смежно: `MaxBufferedBytes < 0` (`config.go:228`) проходит и молча означает «без лимита»
(`opts.go:176` проверяет `> 0`), хотя godoc говорит про ноль.

**Исправление.** `MinBytes > 0`, `MaxBytes > 0`, `MaxPartitionBytes > 0`,
`MaxPartitionBytes <= MaxBytes`, `MaxBufferedBytes >= 0`.

*Проверено вручную: в `config.go` из этой группы валидируется только `MessageQueueSize`
(строка 477).*

---

## Проверено и в порядке

* **TLS `MinVersion` задан явно** (`opts.go:77` = TLS 1.2). На пути с готовым `TLSConfig` не
  навязывается — и правильно.
* **`caCertPool`** (`opts.go:117-132`) строит пустой пул намеренно = пиннинг; при пустом пути
  `RootCAs=nil` = системный стор.
* **`ServerName`:** пустое безопасно — `kgo.DialTLSConfig` клонирует конфиг и подставляет хост
  брокера.
* **`InsecureSkipVerify`:** отдельный флаг, WARN на обоих путях (`opts.go:61-64` и `82-84`).
* **Учётные данные в логах franz-go:** просмотрены все точки SASL-логирования в
  `pkg/kgo/broker.go` — пароль не пишется.
* **Ошибки валидации SASL** подставляют только `Mechanism`, пароль в текст не попадает.
* **`encoding/key.go`:** декодера нет вовсе, недоверенные байты не разбираются. Проверка
  `uint64(len(v)) > math.MaxUint32` перед `PutUint32` корректна. `ValidateKeyLength` вызывается
  раньше `bytes.Equal` (`middleware.go:61-68`).
* **`headers.go`:** зарезервированные ключи (`traceparent`, `tracestate`, `baggage`) отвергаются до
  отправки; инъекции нет по природе бинарного протокола.
* **`math/rand` не используется**; собственной криптографии нет; сравнений секретов нет.
* **Пути к сертификатам в текстах ошибок — не утечка** (ошибка возвращается тому же коду, который
  эти пути и задал).
* **`govulncheck`** (`make audit`) — чисто.
