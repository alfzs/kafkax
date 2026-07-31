package kafkax

// Сверка пакетной документации с кодом.
//
// doc.go формулирует контракт наружу: имена доменных метрик и их вид, значения
// меток, умолчания, имена переменных окружения, тексты ошибок. Мутационная
// проверка эту поверхность не покрывает в принципе — мутация ломает код, а
// расхождение документации с кодом не роняет ни одного теста, документация
// зелёная всегда (docs/audit/09-mutation-sweep.md). Расхождение при этом уже
// случалось: doc.go перечислял четырнадцать доменных метрик из пятнадцати,
// потеряв producer.messages.rejected, и заметить это было нечем.
//
// Поэтому тесты здесь читают исходный текст doc.go и README.md и требуют, чтобы
// каждое проверяемое утверждение сходилось с кодом. Ожидаемое всюду берётся из
// кода или из литерала, но никогда — из той же строки документации, которую
// проверяют: сравнение документации с ней же самой было бы зелёным при любом её
// содержимом.
//
// Что закрепить тестом нельзя, перечислено в docs/audit/09-mutation-sweep.md:
// прозаические объяснения выбора, советы по эксплуатации и утверждения о
// поведении franz-go, у которых нет наблюдаемого следа в коде пакета.

import (
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// repoFile читает файл репозитория относительно каталога пакета.
func repoFile(t *testing.T, rel string) string {
	t.Helper()

	data, err := os.ReadFile(rel) //nolint:gosec // путь приходит из литерала теста, а не снаружи
	if err != nil {
		t.Fatalf("чтение %s: %v", rel, err)
	}

	return string(data)
}

// docComment возвращает пакетный комментарий doc.go одной строкой: маркеры «//»
// сняты, переносы схлопнуты в пробелы.
//
// Схлопывание обязательно: утверждение, разорванное переносом строки
// («must be positive,» + «got 0»), подстрокой не нашлось бы, и тест молча
// проверял бы пустоту.
func docComment(t *testing.T) string {
	t.Helper()

	var b strings.Builder

	for line := range strings.Lines(repoFile(t, "doc.go")) {
		text, ok := strings.CutPrefix(strings.TrimSpace(line), "//")
		if !ok {
			continue
		}

		b.WriteString(strings.TrimSpace(text))
		b.WriteByte(' ')
	}

	return b.String()
}

// packageSources возвращает исходники пакета без тестов и без самого doc.go,
// склеенные в одну строку: по ним ищутся строковые литералы, которые
// документация цитирует.
//
// doc.go исключён намеренно, и это не мелочь: он тоже *.go, и цитата
// «reason="…"» в нём подтверждала бы саму себя — тест оставался бы зелёным
// после переименования литерала в коде. Проверено мутацией: с doc.go в наборе
// подмена строки в consumer_worker.go проходила незамеченной.
func packageSources(t *testing.T) string {
	t.Helper()

	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("перечисление исходников: %v", err)
	}

	var b strings.Builder

	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") || name == "doc.go" {
			continue
		}

		b.WriteString(repoFile(t, name))
	}

	return b.String()
}

// registeredMetrics регистрирует доменные инструменты обеих ролей на
// записывающем метре и возвращает их имена вместе с видом и единицей.
//
// Источник правды — сами конструкторы метрик: список, собранный руками, разошёлся
// бы с ними ровно тем способом, от которого этот файл и защищает.
func registeredMetrics(t *testing.T) map[string]instrumentInfo {
	t.Helper()

	rec := &recordedMetrics{}
	meter := recordingMeter{rec: rec}

	if _, err := newConsumerMetrics(meter); err != nil {
		t.Fatalf("newConsumerMetrics: %v", err)
	}

	if err := (&Producer{}).initMetrics(meter); err != nil {
		t.Fatalf("initMetrics: %v", err)
	}

	return rec.registered()
}

// metricNameRe находит имя доменной метрики без префикса kafkax.
var metricNameRe = regexp.MustCompile(`(?:producer|consumer)(?:\.[a-z]+)+`)

// qualifiedMetricRe находит имя доменной метрики вместе с префиксом.
var qualifiedMetricRe = regexp.MustCompile("`?kafkax\\.(?:producer|consumer)(?:\\.[a-z]+)+`?")

// docMetricEnumeration вырезает из doc.go перечень доменных метрик.
//
// Границы заданы литералами самого перечня: переформулируют вступление —
// тест упадёт здесь с внятным объяснением, а не пропустит проверку молча,
// найдя в пустой строке пустой список.
func docMetricEnumeration(t *testing.T) string {
	t.Helper()

	const (
		head = "с префиксом kafkax.:"
		tail = "Длительности — в секундах."
	)

	doc := docComment(t)

	from := strings.Index(doc, head)
	to := strings.Index(doc, tail)

	if from < 0 || to < from {
		t.Fatalf("в doc.go не нашёлся перечень доменных метрик между %q и %q", head, tail)
	}

	return doc[from+len(head) : to]
}

// TestDocumentationListsEveryDomainMetric — перечни метрик в doc.go и в таблице
// README совпадают с набором, который пакет действительно регистрирует.
//
// Потерянное имя молчаливо вдвойне: метрика существует и пишется, но тот, кто
// строит дашборд по документации, о ней не узнает; лишнее имя, наоборот, шлёт
// дежурного строить панель по ряду, которого в backend'е никогда не будет.
func TestDocumentationListsEveryDomainMetric(t *testing.T) {
	t.Parallel()

	want := slices.Sorted(maps.Keys(registeredMetrics(t)))

	docNames := metricNameRe.FindAllString(docMetricEnumeration(t), -1)
	for i, name := range docNames {
		docNames[i] = "kafkax." + name
	}

	if got := sortedUnique(docNames); !slices.Equal(got, want) {
		t.Errorf("doc.go перечисляет метрики\n%v\nа пакет регистрирует\n%v", got, want)
	}

	readme := qualifiedMetricRe.FindAllString(repoFile(t, "README.md"), -1)
	for i, name := range readme {
		readme[i] = strings.Trim(name, "`")
	}

	if got := sortedUnique(readme); !slices.Equal(got, want) {
		t.Errorf("README называет метрики\n%v\nа пакет регистрирует\n%v", got, want)
	}
}

// TestDocumentationMentionsOnlyRealMetrics — имена метрик, помянутые в doc.go
// вне общего перечня, тоже существуют.
//
// Отдельно от перечня, потому что проверка односторонняя: упоминать в тексте
// каждую метрику никто не обязан, а вот несуществующую — не вправе.
func TestDocumentationMentionsOnlyRealMetrics(t *testing.T) {
	t.Parallel()

	known := registeredMetrics(t)

	for _, name := range qualifiedMetricRe.FindAllString(docComment(t), -1) {
		if _, ok := known[name]; !ok {
			t.Errorf("doc.go ссылается на метрику %q, которой пакет не регистрирует", name)
		}
	}
}

// TestDomainMetricKindsAndUnitsAreStable — вид и единица каждой доменной метрики
// закреплены литералом.
//
// Вид — это контракт с backend'ом, а не деталь: гейдж, ставший счётчиком,
// ломает алерт «стоит хотя бы одна партиция» (doc.go называет
// partitions.paused гейджем прямым текстом), а счётчик, ставший гейджем,
// перестаёт складываться по rate(). Единица не менее важна: doc.go обещает
// «длительности — в секундах», и с единицей «ms» обе гистограммы разъехались бы
// со своими же границами бакетов, оставаясь при этом исправными на вид.
//
// Таблица — литерал, а не обход consumerMetrics: значения, взятые из тех же
// конструкторов, совпали бы с собой при любой правке. Константы kindCounter и
// соседи самоподтверждения не создают: это ярлыки, которыми recordingMeter
// метит свои же методы, а какой из методов вызвать, решает производственный
// код — подмена Int64UpDownCounter на Int64Counter меняет записанный ярлык.
func TestDomainMetricKindsAndUnitsAreStable(t *testing.T) {
	t.Parallel()

	// Пустая единица у трёх счётчиков продюсера — снимок сегодняшнего кода, а
	// не решение: у консьюмера аннотационная единица есть у всех, у продюсера
	// её нет ни у одного. Ряды Prometheus от аннотационной единицы не меняются,
	// поэтому выравнивание безопасно, но делать его надо осознанно и вместе с
	// таблицей README, а не попутной правкой.
	want := map[string]instrumentInfo{
		"kafkax.producer.messages.sent":      {kind: kindCounter, unit: ""},
		"kafkax.producer.messages.failed":    {kind: kindCounter, unit: ""},
		"kafkax.producer.messages.rejected":  {kind: kindCounter, unit: ""},
		"kafkax.producer.message.duration":   {kind: kindHistogram, unit: "s"},
		"kafkax.consumer.messages.processed": {kind: kindCounter, unit: "{message}"},
		"kafkax.consumer.message.duration":   {kind: kindHistogram, unit: "s"},
		"kafkax.consumer.handler.retries":    {kind: kindCounter, unit: "{retry}"},
		"kafkax.consumer.fetch.errors":       {kind: kindCounter, unit: "{episode}"},
		"kafkax.consumer.group.errors":       {kind: kindCounter, unit: "{episode}"},
		"kafkax.consumer.commit.errors":      {kind: kindCounter, unit: "{commit}"},
		"kafkax.consumer.partitions.lost":    {kind: kindCounter, unit: "{partition}"},
		"kafkax.consumer.drain.timeouts":     {kind: kindCounter, unit: "{timeout}"},
		"kafkax.consumer.workers.active":     {kind: kindUpDownCounter, unit: "{worker}"},
		"kafkax.consumer.partitions.paused":  {kind: kindUpDownCounter, unit: "{partition}"},
		"kafkax.consumer.panics":             {kind: kindCounter, unit: "{panic}"},
	}

	if got := registeredMetrics(t); !maps.Equal(got, want) {
		t.Errorf("зарегистрированные инструменты = %v,\nwant %v\n"+
			"вид или единица изменились осознанно? обновите литерал и таблицу метрик в README",
			got, want)
	}
}

// TestMetricAttributeKeysAreStable — множество имён атрибутов доменных метрик
// замкнуто и совпадает с литералом.
//
// Прямая проверка утверждения doc.go «атрибута partition нет ни у одной
// метрики»: кардинальность каждой серии умножается на число партиций, и
// заметно это станет не на ревью, а по счёту за хранение. Заодно стережётся
// вторая половина того же абзаца — отклонение имён от messaging semantic
// conventions: `topic` вместо `messaging.destination.name` выбран сознательно,
// и обратная правка «привести к конвенциям» переименовала бы ряды в
// Prometheus молча.
//
// Источник — исходники пакета, а не список констант: attribute.String
// вызывается и из otel.go, и из точек учёта, и новый ключ появился бы мимо
// любого перечня.
func TestMetricAttributeKeysAreStable(t *testing.T) {
	t.Parallel()

	want := []string{"phase", "reason", "site", "status", "topic"}

	re := regexp.MustCompile(`attribute\.String\("([a-z_]+)"`)

	var keys []string
	for _, m := range re.FindAllStringSubmatch(packageSources(t), -1) {
		keys = append(keys, m[1])
	}

	if got := sortedUnique(keys); !slices.Equal(got, want) {
		t.Errorf("атрибуты метрик = %v, want %v\n"+
			"новый атрибут? проверьте его кардинальность и обновите README вместе с doc.go", got, want)
	}
}

// sortedUnique — отсортированный набор без повторов.
func sortedUnique(values []string) []string {
	out := slices.Clone(values)
	slices.Sort(out)

	return slices.Compact(out)
}

// docQuotedValues достаёт из doc.go значения меток вида key="value".
func docQuotedValues(t *testing.T, key string) []string {
	t.Helper()

	re := regexp.MustCompile(regexp.QuoteMeta(key) + `="([^"]*)"`)

	var out []string
	for _, m := range re.FindAllStringSubmatch(docComment(t), -1) {
		out = append(out, m[1])
	}

	if len(out) == 0 {
		t.Fatalf("в doc.go не нашлось ни одного значения %s=\"…\"", key)
	}

	return out
}

// TestConsumerStatusValuesAreStable — строковые значения атрибута status
// закреплены литералом, как и значения PanicSite.
//
// Причина та же, что у TestPanicSiteValuesAreStable: значения уходят в label
// метрик kafkax.consumer.messages.processed и kafkax.consumer.message.duration,
// на них построены дашборды и таблица README, а компилятор о переименовании
// строковой константы ничего не скажет. Соседний
// TestDocumentationQuotesRealStatusValues ловит расхождение кода с документацией,
// но согласованное переименование обеих сторон пропустил бы — а оно ломающее.
func TestConsumerStatusValuesAreStable(t *testing.T) {
	t.Parallel()

	// Склейкой, а не таблицей: одно сравнение проверяет и значения, и их
	// различность, и порядок прогрева кэша опций.
	const want = "success|error|skipped|cancelled|dropped"

	if got := strings.Join(consumerStatuses, "|"); got != want {
		t.Errorf("consumerStatuses = %q, want %q", got, want)
	}

	// Продюсер размечает свою гистограмму теми же двумя значениями, что и
	// консьюмер: «success» и «error» в двух ролях обязаны читаться одинаково.
	if got := statusSuccess + "|" + statusError; got != "success|error" {
		t.Errorf("статусы продюсера = %q, want %q", got, "success|error")
	}
}

// TestDocumentationQuotesRealStatusValues — каждое status="…" из doc.go есть
// среди значений, которыми пакет действительно размечает метрики.
func TestDocumentationQuotesRealStatusValues(t *testing.T) {
	t.Parallel()

	known := append(slices.Clone(consumerStatuses), statusSuccess, statusError)

	for _, value := range docQuotedValues(t, "status") {
		if !slices.Contains(known, value) {
			t.Errorf("doc.go обещает status=%q, а пакет такого статуса не пишет; есть только %v",
				value, sortedUnique(known))
		}
	}
}

// TestDocumentationQuotesRealPanicSites — каждое site="…" из doc.go есть среди
// констант PanicSite.
//
// Значение site — единственное, по чему дежурный отличает панику обработчика от
// паники обвязки; выдуманное в документации значение отправляет его строить
// фильтр по ряду, которого не существует.
func TestDocumentationQuotesRealPanicSites(t *testing.T) {
	t.Parallel()

	known := []PanicSite{
		PanicSiteHandler,
		PanicSiteProcessMessage,
		PanicSitePartitionWorker,
		PanicSitePollLoop,
		PanicSiteMessageSkipped,
		PanicSitePanicHook,
	}

	for _, value := range docQuotedValues(t, "site") {
		if !slices.Contains(known, PanicSite(value)) {
			t.Errorf("doc.go обещает site=%q, а такой точки восстановления нет; есть %v", value, known)
		}
	}
}

// TestDocumentationQuotesRealLogReasons — каждое reason="…" из doc.go есть в
// исходниках пакета строковым литералом.
//
// Значение reason цитируется в документации дословно ради поиска по журналу:
// «партиция встала, потому что подписка разошлась с картой обработчиков»
// отличается от прочих отравлений только этой строкой.
func TestDocumentationQuotesRealLogReasons(t *testing.T) {
	t.Parallel()

	sources := packageSources(t)

	for _, value := range docQuotedValues(t, "reason") {
		if !strings.Contains(sources, `"`+value+`"`) {
			t.Errorf("doc.go обещает reason=%q, но такого литерала в исходниках пакета нет", value)
		}
	}
}

// TestDocumentationNamesRealEnvVariables — каждая переменная окружения,
// названная в doc.go, есть в тегах Config.
func TestDocumentationNamesRealEnvVariables(t *testing.T) {
	t.Parallel()

	known := map[string]bool{}
	collectEnvTags(reflect.TypeFor[Config](), known)

	found := regexp.MustCompile(`KAFKAX_[A-Z_]+`).FindAllString(docComment(t), -1)
	if len(found) == 0 {
		t.Fatal("в doc.go не нашлось ни одной переменной окружения")
	}

	for _, name := range found {
		if !known[name] {
			t.Errorf("doc.go называет переменную %q, которой в тегах Config нет", name)
		}
	}
}

// collectEnvTags собирает имена переменных окружения из тегов структуры.
func collectEnvTags(typ reflect.Type, out map[string]bool) {
	for f := range typ.Fields() {
		name, ok := f.Tag.Lookup("env")
		if ok {
			out[name] = true

			continue
		}

		if f.Type.Kind() == reflect.Struct {
			collectEnvTags(f.Type, out)
		}
	}
}

// TestDocumentationValidationExampleIsReproducible — пример претензии валидации,
// приведённый в doc.go дословно, действительно порождается Validate.
//
// Пример показывает форму, на которую опирается вызывающий: Go-путь поля плюс
// имя переменной окружения в скобках. Уедет любая из трёх частей — Go-путь,
// суффикс env или текст претензии, — и в документации останется сообщение,
// которого пакет больше не печатает, притом что соседние тесты валидации
// собирают ожидаемое через cfgLabel и такой правки не заметят.
func TestDocumentationValidationExampleIsReproducible(t *testing.T) {
	t.Parallel()

	const want = "Consumer.MaxBytes (env KAFKAX_CONSUMER_MAX_BYTES) must be positive, got 0"

	if doc := docComment(t); !strings.Contains(doc, want) {
		t.Fatalf("doc.go больше не приводит пример %q — обновите тест вместе с документацией", want)
	}

	cfg := testConfig(t)
	cfg.Consumer.MaxBytes = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate принял Consumer.MaxBytes = 0")
	}

	if !strings.Contains(err.Error(), want) {
		t.Errorf("претензия валидации = %q,\nа doc.go обещает подстроку %q", err, want)
	}
}

// TestYamlKeysAreSnakeCaseOfGoPath — yaml-ключи выводятся из Go-пути поля тем
// же правилом, что и имена переменных окружения.
//
// Это обещание doc.go, на котором держится решение называть поле в претензии
// валидации Go-путём, а не yaml-ключом: «yaml-ключ при этом остаётся выводимым
// — это тот же путь в snake_case». Разойдись хоть один тег, и обещание станет
// ложным для того, кто настраивает пакет файлом: претензия называет поле,
// которого в его yaml нет.
//
// Правило берётся из envName, а тег — из структуры: это два независимых
// источника. Само правило сторожит TestEnvNamesMatchStructTags.
func TestYamlKeysAreSnakeCaseOfGoPath(t *testing.T) {
	t.Parallel()

	checked := 0
	walkYamlTags(t, reflect.TypeFor[Config](), &checked)

	// Иначе разъехавшийся обход прошёл бы за успех: ноль проверенных полей —
	// зелёный тест.
	if checked < 30 {
		t.Errorf("проверено полей: %d — обход структуры разъехался", checked)
	}
}

// walkYamlTags сверяет yaml-теги структуры с snake_case имён полей.
func walkYamlTags(t *testing.T, typ reflect.Type, checked *int) {
	t.Helper()

	for f := range typ.Fields() {
		tag, ok := f.Tag.Lookup("yaml")
		if !ok {
			continue
		}

		// «-» — поле, которого в yaml не бывает вовсе: Logger, TLSConfig,
		// ExtraOpts и оба хука задаются только кодом. Обещание doc.go про
		// выводимый yaml-ключ их и не касается.
		if tag == "-" {
			continue
		}

		*checked++

		if want := strings.ToLower(strings.TrimPrefix(envName(f.Name), envPrefix)); tag != want {
			t.Errorf("yaml-тег поля %s = %q, а из Go-имени выводится %q", f.Name, tag, want)
		}

		if f.Type.Kind() == reflect.Struct {
			walkYamlTags(t, f.Type, checked)
		}
	}
}

// sentinelRe находит объявление сентинела в блоке var файла errors.go.
var sentinelRe = regexp.MustCompile(`(?m)^\t(Err[A-Za-z0-9]+)\s+=`)

// TestDocumentationNamesRealErrorSentinels — каждый сентинел, названный в
// doc.go, объявлен в errors.go.
//
// Ссылка на несуществующий ErrXxx — самый безобидный на вид и самый неприятный
// на деле вид гнили: godoc отрисует его без ссылки, и заметить это можно только
// глазами. Переименование сентинела компилятор ловит везде, кроме комментария.
func TestDocumentationNamesRealErrorSentinels(t *testing.T) {
	t.Parallel()

	known := map[string]bool{}
	for _, m := range sentinelRe.FindAllStringSubmatch(repoFile(t, "errors.go"), -1) {
		known[m[1]] = true
	}

	if len(known) == 0 {
		t.Fatal("в errors.go не нашлось ни одного сентинела: разъехался разбор объявлений")
	}

	// Err с заглавной следом — чтобы не считать сентинелами Error и Errorf из
	// прозы про errors.As и fmt.Errorf.
	for _, name := range regexp.MustCompile(`\bErr[A-Z][A-Za-z0-9]*`).FindAllString(docComment(t), -1) {
		if !known[name] {
			t.Errorf("doc.go ссылается на %s, которого в errors.go нет", name)
		}
	}
}

// docQualifiers — типы, к которым doc.go обращается по имени.
//
// Двузначность намеренная и неустранимая: «Producer» в тексте означает и
// клиент, и секцию конфигурации Config.Producer, потому что именно так их
// называет пользователь. Поэтому имя разрешается по обоим типам сразу, и
// Producer.SendMessage (метод клиента) и Producer.MessageTimeout (поле секции)
// оба законны.
func docQualifiers() map[string][]reflect.Type {
	return map[string][]reflect.Type{
		"Config":          {reflect.TypeFor[Config]()},
		"Consumer":        {reflect.TypeFor[ConsumerConfig](), reflect.TypeFor[*Consumer]()},
		"Producer":        {reflect.TypeFor[ProducerConfig](), reflect.TypeFor[*Producer]()},
		"ConsumerHandler": {reflect.TypeFor[ConsumerHandler]()},
		"IncomingMessage": {reflect.TypeFor[IncomingMessage]()},
		"PublishRequest":  {reflect.TypeFor[PublishRequest]()},
		"DeliveryError":   {reflect.TypeFor[*DeliveryError]()},
		"FlushError":      {reflect.TypeFor[*FlushError]()},
		"SASL":            {reflect.TypeFor[SASL]()},
		"TLS":             {reflect.TypeFor[TLS]()},
	}
}

// hasMember отвечает, есть ли у типа такое поле или метод.
func hasMember(typ reflect.Type, name string) bool {
	if _, ok := typ.MethodByName(name); ok {
		return true
	}

	target := typ
	if target.Kind() == reflect.Pointer {
		target = target.Elem()
	}

	if target.Kind() != reflect.Struct {
		return false
	}

	_, ok := target.FieldByName(name)

	return ok
}

// TestDocumentationReferencesResolve — каждая ссылка вида Тип.Член из doc.go
// разрешается в настоящее поле или метод пакета.
//
// Волна 8 переименовала половину публичного API, и единственное, что при таком
// переименовании молчит, — комментарии: инструмент правит объявления и вызовы,
// а «Consumer.SubscribeAll» в тексте остаётся ссылкой в никуда. Один такой
// висяк в репозитории уже находили глазами (RF-DOCS, мёртвая ссылка
// SubscribeAll); этот тест избавляет от необходимости находить следующий так же.
func TestDocumentationReferencesResolve(t *testing.T) {
	t.Parallel()

	qualifiers := docQualifiers()

	refs := regexp.MustCompile(`\b([A-Z][A-Za-z0-9]*)\.([A-Z][A-Za-z0-9]*)\b`).
		FindAllStringSubmatch(docComment(t), -1)
	if len(refs) == 0 {
		t.Fatal("в doc.go не нашлось ни одной ссылки вида Тип.Член: разъехался разбор")
	}

	for _, ref := range refs {
		types, known := qualifiers[ref[1]]
		if !known {
			t.Errorf("doc.go ссылается на %s.%s, а тип %s тесту неизвестен: "+
				"это опечатка или тип надо добавить в docQualifiers", ref[1], ref[2], ref[1])

			continue
		}

		if !slices.ContainsFunc(types, func(typ reflect.Type) bool { return hasMember(typ, ref[2]) }) {
			t.Errorf("doc.go ссылается на %s.%s, а такого поля или метода нет ни в одном из %v",
				ref[1], ref[2], types)
		}
	}
}

// TestDocumentationDefaultsMatchDefaultConfig — умолчания, названные в doc.go
// числом или словом, совпадают с DefaultConfig.
//
// Соседний TestDefaultConfigMatchesStructTags сверяет DefaultConfig с тегами
// структуры, то есть код с кодом. Здесь проверяется третья сторона —
// документация: подъём env-default оба тех источника меняет согласованно и
// оставляет doc.go рассказывать про прежнее поведение.
func TestDocumentationDefaultsMatchDefaultConfig(t *testing.T) {
	t.Parallel()

	def := DefaultConfig()
	doc := docComment(t)

	// «0 (умолчание) — вызвать обработчик один раз, повторов не делать»
	// и «HandlerMaxRetries: N, OnMessageSkipped: nil … Это умолчание».
	if def.Consumer.HandlerMaxRetries != 0 {
		t.Errorf("HandlerMaxRetries по умолчанию = %d, а doc.go называет умолчанием 0",
			def.Consumer.HandlerMaxRetries)
	}

	if def.OnMessageSkipped != nil {
		t.Error("OnMessageSkipped по умолчанию задан, а doc.go называет умолчанием отравление партиции")
	}

	// «при Producer.EnableIdempotence = true (умолчание)».
	if !def.Producer.EnableIdempotence {
		t.Error("EnableIdempotence по умолчанию выключена, а doc.go называет умолчанием true")
	}

	if !strings.Contains(doc, "Producer.EnableIdempotence = true (умолчание)") {
		t.Error("doc.go больше не называет умолчание EnableIdempotence — обновите тест вместе с текстом")
	}

	// «Brokers, ClientID и Consumer.Group умолчания не имеют намеренно».
	if len(def.Brokers) != 0 || def.ClientID != "" || def.Consumer.Group != "" {
		t.Errorf("у Brokers/ClientID/Consumer.Group появилось умолчание (%v, %q, %q), "+
			"а doc.go обещает, что их нет", def.Brokers, def.ClientID, def.Consumer.Group)
	}
}

// TestKotelVersionInCommentsMatchesGoMod — версия kotel, названная в
// комментариях, совпадает с go.mod.
//
// Утверждение «гистограмм в kotel v1.7.0 нет ни одной» верно ровно до
// следующего обновления плагина, и никакой тест не проверит его содержание. Но
// он проверит номер: подъём kotel сделает утверждение спорным, и тест потребует
// перечитать его, а не оставит устаревшую цифру в контракте наблюдаемости.
func TestKotelVersionInCommentsMatchesGoMod(t *testing.T) {
	t.Parallel()

	m := regexp.MustCompile(`plugin/kotel (v\S+)`).FindStringSubmatch(repoFile(t, "go.mod"))
	if m == nil {
		t.Fatal("в go.mod не нашёлся plugin/kotel")
	}

	mentions := regexp.MustCompile(`kotel (v\d\S*)`).FindAllStringSubmatch(
		docComment(t)+packageSources(t), -1)
	if len(mentions) == 0 {
		t.Fatal("ни один комментарий не называет версию kotel: разъехался разбор")
	}

	for _, mention := range mentions {
		if mention[1] != m[1] {
			t.Errorf("комментарий говорит про kotel %s, а go.mod требует %s", mention[1], m[1])
		}
	}
}

// TestDocumentationReferencedTestsExist — тесты, названные в doc.go по имени,
// в репозитории есть.
//
// doc.go ссылается на TestTruncationBelowCommittedOffset как на доказательство
// поведения при усечении темы. Переименуют тест — и ссылка станет обещанием
// проверки, которой не найти; удалят — обещанием проверки, которой нет.
func TestDocumentationReferencedTestsExist(t *testing.T) {
	t.Parallel()

	names := regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9]*`).FindAllString(docComment(t), -1)
	if len(names) == 0 {
		t.Fatal("в doc.go не нашлось ни одной ссылки на тест: разъехался разбор")
	}

	var suite strings.Builder

	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".") && path != "." {
			return fs.SkipDir
		}

		if !entry.IsDir() && strings.HasSuffix(path, "_test.go") {
			suite.WriteString(repoFile(t, path))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("обход репозитория: %v", err)
	}

	for _, name := range sortedUnique(names) {
		if !strings.Contains(suite.String(), "func "+name+"(") {
			t.Errorf("doc.go ссылается на %s, а такого теста в репозитории нет", name)
		}
	}
}
