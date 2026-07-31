package kafkax

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// Тесты обнаружения отставленных ключей.
//
// Ожидаемые тексты здесь набраны литералами, а не собраны из retiredKeys.
// Сверять сообщение с той же таблицей, из которой оно построено, — значит не
// проверять ничего: переписать change на «key is retired» можно было бы, не
// уронив ни одного теста, и вся ценность механизма (назвать замену и суть
// изменения) утекла бы молча. Литерал стоит правки при каждом изменении
// формулировки, и это ровно та цена, которую хочется платить.

// TestRetiredEnvKeyFailsEveryValidationEntry — отставленная переменная
// окружения роняет все три входа в валидацию.
//
// Три, а не один: конструктор продюсера и конструктор консьюмера проверяют
// каждый свою роль, и проверка, поставленная в консьюмерскую секцию, оставила
// бы продюсеру молчаливый обход. Общий вход Validate проверяется заодно — им
// пользуется приложение, создающее из одного Config и то, и другое.
func TestRetiredEnvKeyFailsEveryValidationEntry(t *testing.T) {
	envs := []string{
		"KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE",
		"KAFKAX_CONSUMER_HANDLER_MAX_RETRIES",
	}

	for _, env := range envs {
		t.Run(env, func(t *testing.T) {
			// Значение намеренно правдоподобное: именно перенесённая из v1
			// тысяча и есть тот случай, ради которого всё затевалось.
			t.Setenv(env, "1000")

			cfg := testConfig(t)

			entries := map[string]error{
				"Validate":         cfg.Validate(),
				"validateProducer": cfg.validateProducer(behavior{}),
				"validateConsumer": cfg.validateConsumer(behavior{}),
			}

			for name, err := range entries {
				if err == nil {
					t.Errorf("%s принял конфигурацию с %s в окружении", name, env)

					continue
				}

				if !strings.Contains(err.Error(), env) {
					t.Errorf("%s: ошибка не называет %s:\n%v", name, env, err)
				}

				if !errors.Is(err, ErrInvalidConfig) {
					t.Errorf("%s: ошибка не опознаётся как ErrInvalidConfig: %v", name, err)
				}
			}
		})
	}
}

// TestRetiredEnvKeyReachesConstructors — отставленный ключ виден там, где его
// увидит потребитель: на конструкторе, а не на внутренней функции.
//
// Конструкторы вызывают валидацию до первого сетевого действия, поэтому брокер
// не нужен.
func TestRetiredEnvKeyReachesConstructors(t *testing.T) {
	t.Setenv("KAFKAX_CONSUMER_HANDLER_MAX_RETRIES", "0")

	cfg := testConfig(t)

	if _, err := NewProducer(cfg); err == nil {
		t.Error("NewProducer принял конфигурацию с отставленной переменной окружения")
	}

	if _, err := NewConsumer(cfg); err == nil {
		t.Error("NewConsumer принял конфигурацию с отставленной переменной окружения")
	}
}

// TestRetiredEnvKeyReportedWhenEmpty — пустое значение переменной считается
// заявлением о настройке наравне с заполненным.
//
// KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE= в манифесте — это перенесённая строка
// конфигурации, а не её отсутствие; отличать её от заполненной значило бы
// пропускать ровно тот конфиг, который перенесли не глядя.
func TestRetiredEnvKeyReportedWhenEmpty(t *testing.T) {
	t.Setenv("KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE", "")

	if err := testConfig(t).Validate(); err == nil {
		t.Error("пустая KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE прошла валидацию")
	}
}

// TestRetiredEnvErrorNamesReplacementAndChange — текст ошибки объясняет, что
// изменилось, а не сообщает об устаревании.
//
// «Ключ устарел» оставляет читателя ровно там, откуда он пришёл: значение у
// него уже есть, и он перенесёт его под новым именем. Сообщение обязано
// назвать замену в обеих формах записи и сказать, почему прежнее значение
// нельзя перенести как есть.
func TestRetiredEnvErrorNamesReplacementAndChange(t *testing.T) {
	tests := []struct {
		env  string
		want []string
	}{
		{
			env: "KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE",
			want: []string{
				"KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE is retired",
				"consumer.message_queue_batches",
				"KAFKAX_CONSUMER_MESSAGE_QUEUE_BATCHES",
				"the unit changed from messages to batches",
				"raises the memory ceiling",
			},
		},
		{
			env: "KAFKAX_CONSUMER_HANDLER_MAX_RETRIES",
			want: []string{
				"KAFKAX_CONSUMER_HANDLER_MAX_RETRIES is retired",
				"consumer.handler_retries",
				"KAFKAX_CONSUMER_HANDLER_RETRIES",
				"the meaning of 0 is inverted",
				"in v1 zero meant retry forever",
				"in v3 zero means no retries at all",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv(tt.env, "3")

			cfgWantErr(t, testConfig(t).Validate(), tt.want...)
		})
	}
}

// TestRetiredYAMLKeyFailsDecode — отставленный ключ в файле роняет разбор.
//
// Проверяется настоящим декодером, а не вызовом UnmarshalYAML напрямую: весь
// расчёт механизма в том, что yaml.v3 сам зовёт метод устаревшей формы
// (unmarshal-функция вместо *yaml.Node). Ручной вызов подтвердил бы только
// нашу же арифметику по карте и оставил бы главное допущение непроверенным.
func TestRetiredYAMLKeyFailsDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "message_queue_size",
			src: "brokers: [kafka:9092]\n" +
				"consumer:\n" +
				"  group: billing\n" +
				"  message_queue_size: 1000\n",
			want: []string{
				"consumer.message_queue_size is retired",
				"consumer.message_queue_batches",
				"the unit changed from messages to batches",
			},
		},
		{
			name: "handler_max_retries",
			src: "consumer:\n" +
				"  handler_max_retries: 0\n",
			want: []string{
				"consumer.handler_max_retries is retired",
				"consumer.handler_retries",
				"the meaning of 0 is inverted",
			},
		},
		{
			// Регистр ключей файла декодер игнорирует, сопоставляя их с
			// полями структуры. Отставленный ключ обязан ловиться так же,
			// иначе обход сводится к смене регистра одной буквы.
			name: "регистр ключей не важен",
			src: "Consumer:\n" +
				"  Message_Queue_Size: 1000\n",
			want: []string{"consumer.message_queue_size is retired"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var cfg Config

			err := yaml.Unmarshal([]byte(tt.src), &cfg)

			cfgWantErr(t, err, tt.want...)

			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("ошибка разбора файла не опознаётся как ErrInvalidConfig: %v", err)
			}
		})
	}
}

// TestRetiredYAMLKeyFoundInLegacyMapShape — ключ находится и в отображении
// старой формы.
//
// Сигнатура метода — та самая, что была основной в yaml.v2, поэтому позвать
// его может и v2-декодер, а тот отдаёт вложенные отображения как map[any]any
// вместо map[string]any. Промах на такой карте выглядел бы как «отставленных
// ключей в файле нет», то есть как молчаливый обход всего механизма.
//
// Здесь стоит двойник декодера, а не сам yaml.v2: тащить в зависимости
// библиотеку ради одной ветки дороже, чем подставить карту руками. Ожидаемое
// при этом взято литералом, а не у проверяемого кода.
func TestRetiredYAMLKeyFoundInLegacyMapShape(t *testing.T) {
	t.Parallel()

	unmarshal := func(v any) error {
		out, ok := v.(*any)
		if !ok {
			// Второй проход, в структуру: до него дело не дойдёт.
			return nil
		}

		*out = map[any]any{yamlSectionConsumer: map[any]any{"handler_max_retries": 0}}

		return nil
	}

	var cfg Config

	cfgWantErr(t, cfg.UnmarshalYAML(unmarshal), "consumer.handler_max_retries is retired")
}

// TestYAMLDecodeAppliesLiveKeys — перехват разбора не сломал сам разбор.
//
// UnmarshalYAML читает узел дважды: сначала картой ради имён ключей, потом
// структурой. Забыть второй проход или подменить приёмник нулевым значением —
// и конфигурация из файла молча перестанет применяться, а падать на этом
// нечему: валидация примет умолчания.
func TestYAMLDecodeAppliesLiveKeys(t *testing.T) {
	t.Parallel()

	src := "brokers:\n" +
		"  - kafka:9092\n" +
		"client_id: billing\n" +
		"consumer:\n" +
		"  group: billing.workers\n" +
		"  message_queue_batches: 7\n" +
		"  handler_retries: 5\n"

	// Значение выставлено до разбора, и в файле его нет: метод обязан
	// достраивать переданный Config, а не подменять его нулевым. Раньше
	// свидетелем был Logger, но он уехал из Config в опции (WithLogger), и
	// свидетеля пришлось сменить на поле, которого нет в этом файле.
	const presetTimeout = 42 * time.Second

	cfg := Config{GracefulTimeout: presetTimeout}
	if err := yaml.Unmarshal([]byte(src), &cfg); err != nil {
		t.Fatalf("разбор валидного файла провалился: %v", err)
	}

	if got := cfg.Consumer.MessageQueueBatches; got != 7 {
		t.Errorf("Consumer.MessageQueueBatches = %d, в файле 7", got)
	}

	if got := cfg.Consumer.HandlerRetries; got != 5 {
		t.Errorf("Consumer.HandlerRetries = %d, в файле 5", got)
	}

	if got := cfg.ClientID; got != "billing" {
		t.Errorf("ClientID = %q, в файле billing", got)
	}

	if !reflect.DeepEqual(cfg.Brokers, []string{"kafka:9092"}) {
		t.Errorf("Brokers = %v, в файле [kafka:9092]", cfg.Brokers)
	}

	if cfg.GracefulTimeout != presetTimeout {
		t.Errorf("GracefulTimeout = %v, выставленное до разбора значение %v потеряно",
			cfg.GracefulTimeout, presetTimeout)
	}
}

// TestYAMLDecodeReportsMalformedFile — на испорченном файле остаётся жалоба
// декодера.
//
// Первый проход по узлу разбирается в карту и на скаляре обязан промолчать:
// если бы его ошибка возвращалась наружу, вместо «cannot unmarshal !!str into
// int» пользователь получил бы претензию к типу карты, к его файлу отношения
// не имеющую.
func TestYAMLDecodeReportsMalformedFile(t *testing.T) {
	t.Parallel()

	var cfg Config

	err := yaml.Unmarshal([]byte("consumer:\n  max_poll_records: сто\n"), &cfg)
	if err == nil {
		t.Fatal("разбор нечислового max_poll_records прошёл без ошибки")
	}

	if !strings.Contains(err.Error(), "cannot unmarshal") {
		t.Errorf("жалоба декодера на тип значения подменена:\n%v", err)
	}

	if strings.Contains(err.Error(), "is retired") {
		t.Errorf("испорченный файл принят за конфигурацию с отставленным ключом:\n%v", err)
	}
}

// TestRetiredKeysDoNotShadowLiveFields — отставленный ключ не совпадает ни с
// одним действующим.
//
// Сторож против обратного переименования и против записи, добавленной с
// опечаткой: ключ, который одновременно и отставлен, и обслуживается полем,
// сделал бы рабочую конфигурацию незапускаемой, причём без всякого способа её
// починить.
func TestRetiredKeysDoNotShadowLiveFields(t *testing.T) {
	t.Parallel()

	yamlKeys, envs := cfgLiveKeys(t, reflect.TypeFor[Config](), nil)

	if len(yamlKeys) < 25 || len(envs) < 25 {
		t.Fatalf("собрано ключей: yaml %d, env %d — обход по структуре сломан", len(yamlKeys), len(envs))
	}

	for _, key := range retiredKeys {
		if yamlKeys[key.yamlKey()] {
			t.Errorf("отставленный ключ %s обслуживается полем структуры", key.yamlKey())
		}

		if envs[key.env] {
			t.Errorf("отставленная переменная %s обслуживается полем структуры", key.env)
		}
	}
}

// cfgLiveKeys собирает действующие yaml-пути и имена переменных окружения из
// тегов структуры.
func cfgLiveKeys(t *testing.T, typ reflect.Type, prefix []string) (map[string]bool, map[string]bool) {
	t.Helper()

	yamlKeys := make(map[string]bool)
	envs := make(map[string]bool)

	for field := range typ.Fields() {
		tag, ok := field.Tag.Lookup("yaml")
		if !ok || tag == "-" {
			continue
		}

		path := append(append([]string{}, prefix...), tag)

		if env, ok := field.Tag.Lookup("env"); ok {
			envs[env] = true
		}

		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeFor[Config]() {
			nestedYAML, nestedEnv := cfgLiveKeys(t, field.Type, path)
			for k := range nestedYAML {
				yamlKeys[k] = true
			}

			for k := range nestedEnv {
				envs[k] = true
			}
		}

		yamlKeys[strings.Join(path, ".")] = true
	}

	return yamlKeys, envs
}

// v1YAMLKeys — полный перечень yaml-ключей конфигурации v1.5.0, набранный
// литералом.
//
// Литералом, а не выводом из кода: кода v1 в модуле нет, он живёт только в
// истории git, и вывести перечень в рантайме неоткуда. Собран один раз обходом
// тегов `git show v1.5.0:config.go` и с тех пор — снимок, который не меняется:
// v1 больше не выйдет.
//
// Секции без точки — это сами поля-секции («producer», «tls»); они в файле
// присутствуют и в v3, поэтому в перечне их нет.
var v1YAMLKeys = []string{
	// clientid, а не client_id: у поля ClientID в v1 не было тега yaml, и
	// yaml.v3 берёт имя поля в нижнем регистре, без подчёркиваний.
	"brokers", "clientid", "graceful_timeout", "security_protocol",
	// username и password в v1 тегов yaml не имели, как и ClientID; ключами
	// служили имена полей в нижнем регистре, и с тегами v3 они совпали.
	"sasl.mechanism", "sasl.username", "sasl.password",
	"tls.ca_cert_path", "tls.client_cert_path", "tls.client_key_path",
	"tls.identification_algorithm", "tls.insecure_skip_verify",
	"producer.ack_timeout", "producer.batch_bytes", "producer.batch_size",
	"producer.batch_timeout", "producer.cleanup_worker_interval", "producer.compression_type",
	"producer.enable_idempotence", "producer.flush_timeout", "producer.inactive_worker_ttl",
	"producer.linger", "producer.max_inflight", "producer.max_retries",
	"producer.message_queue_size", "producer.message_timeout", "producer.required_acks",
	"producer.retry_backoff",
	"consumer.cleanup_worker_interval", "consumer.enable_auto_commit",
	"consumer.handler_max_retries", "consumer.handler_retry_delay",
	"consumer.heartbeat_interval", "consumer.inactive_worker_ttl", "consumer.initial_offset",
	"consumer.isolation_level", "consumer.max_bytes", "consumer.max_poll_interval",
	"consumer.max_wait", "consumer.message_queue_size", "consumer.min_bytes",
	"consumer.read_error_backoff", "consumer.read_timeout", "consumer.session_timeout",
	"consumer.socket_timeout",
}

// TestEveryV1KeyIsLiveOrRetired — каждый ключ v1 либо существует в v3, либо
// назван отставленным. Третьего исхода быть не должно.
//
// Это сторож полноты списка, и он важнее любой отдельной записи в нём. Ключ,
// выпавший из обеих категорий, — ровно тот дефект, ради которого механизм и
// заведён: перенесённый конфиг проглотит его молча, настройка перестанет
// действовать, и узнают об этом по поведению в проде, а не по ошибке старта.
// Без такого теста список пополняется только тогда, когда кто-то споткнётся.
//
// Перечень v1 — литерал (см. v1YAMLKeys), живые ключи — обход структуры
// рефлексией, отставленные — таблица. Сверять две стороны, выведенные из одного
// источника, смысла не имело бы.
func TestEveryV1KeyIsLiveOrRetired(t *testing.T) {
	t.Parallel()

	live, _ := cfgLiveKeys(t, reflect.TypeFor[Config](), nil)

	retired := make(map[string]bool, len(retiredKeys))
	for _, k := range retiredKeys {
		retired[strings.Join(k.yamlPath, ".")] = true
	}

	// Обе стороны непусты: сломавшийся обход или опустевшая таблица иначе
	// сделали бы тест зелёным по недосмотру, а не по существу.
	if len(live) < 25 {
		t.Fatalf("живых ключей собрано %d — обход по структуре сломан", len(live))
	}

	if len(retired) < 10 {
		t.Fatalf("отставленных ключей %d — таблица опустела", len(retired))
	}

	for _, key := range v1YAMLKeys {
		if live[key] || retired[key] {
			continue
		}

		t.Errorf("ключ v1 %q не существует в v3 и не назван отставленным: "+
			"перенесённый конфиг потеряет его молча. Добавьте запись в retiredKeys "+
			"с текстом, объясняющим, чем его заменить", key)
	}
}

// TestEveryRetiredKeyCoversBothForms — у каждой записи заполнены обе формы
// записи ключа, кроме единственного оправданного исключения.
//
// Задать настройку можно и файлом, и переменной окружения, поэтому запись с
// пустым env закрывает только половину пути: yaml поймает, окружение пропустит.
// Отказ молчаливый — тесты остальных записей останутся зелёными, и узнать о
// дыре будет неоткуда.
//
// Исключение перечислено литералом, а не выведено из таблицы: список,
// сверяемый сам с собой, разрешил бы любую новую запись без env.
func TestEveryRetiredKeyCoversBothForms(t *testing.T) {
	t.Parallel()

	// clientid менял имя только в файле: переменная KAFKAX_CLIENT_ID как
	// называлась, так и называется, и объявлять её отставленной нельзя — она
	// живая.
	withoutEnv := map[string]bool{"clientid": true}

	for _, key := range retiredKeys {
		path := strings.Join(key.yamlPath, ".")

		switch {
		case key.env != "" && withoutEnv[path]:
			t.Errorf("%q объявлен исключением, но env у него задан (%s) — уберите его из списка исключений",
				path, key.env)
		case key.env == "" && !withoutEnv[path]:
			t.Errorf("у отставленного ключа %q не задан env: настройку можно передать переменной "+
				"окружения, и этот путь останется неперехваченным", path)
		}

		if len(key.yamlPath) == 0 {
			t.Errorf("у записи с env %q не задан путь в файле", key.env)
		}

		if key.change == "" {
			t.Errorf("у отставленного ключа %q пустой текст: отказ без объяснения не лучше молчания", path)
		}
	}
}
