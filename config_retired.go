package kafkax

import (
	"fmt"
	"os"
	"strings"
)

// Отставленные ключи конфигурации.
//
// Мажорная версия даёт право сломать имя поля, и v3 этим правом пользуется:
// два консьюмерских ключа сменили не только имя, но и смысл значения —
// message_queue_size считал сообщения, а стал считать батчи; handler_max_retries
// понимал ноль как «повторять бесконечно», а стал понимать его как «не
// повторять». Одного переименования мало: cleanenv молча игнорирует и
// незнакомый yaml-ключ, и незнакомую переменную окружения, поэтому перенесённый
// из v1/v2 конфиг просто потерял бы настройку и взял умолчание. Разница между
// «потерял настройку» и «сменил единицу измерения» видна не по логам, а по
// потолку памяти в десятки гигабайт.
//
// Поэтому старые имена не исчезают, а становятся отказом на старте с текстом,
// который называет замену и суть изменения.

// retiredKey — ключ конфигурации, снятый с употребления.
//
// Хранится сразу в двух формах записи, потому что задать настройку можно двумя
// способами и оба надо перехватить: yamlPath — путь в файле («consumer»,
// «message_queue_size»), env — имя переменной окружения. Выводить второе из
// первого нельзя: у отставленного ключа нет поля в структуре, а значит нет и
// Go-пути, из которого envName собирает имя переменной.
type retiredKey struct {
	yamlPath []string
	env      string
	// change — что именно изменилось, а не факт устаревания. «Ключ устарел»
	// оставляет читателя с вопросом, на что менять и можно ли перенести
	// значение; здесь названы и замена, и смена смысла, и последствие переноса
	// значения как есть.
	change string
}

// retiredKeys — полный список отставленных ключей.
//
// Список, а не проверка на каждый ключ россыпью: обе точки перехвата (файл и
// окружение) обходят его целиком, поэтому новая запись закрывает сразу оба
// пути, и забыть одну из половин невозможно.
//
// Записи не удаляются вместе с выходом следующей мажорной версии: ценность
// отказа в том, что он встречает конфиг, пролежавший в репозитории годы.
var retiredKeys = []retiredKey{
	{
		yamlPath: []string{"consumer", "message_queue_size"},
		env:      "KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE",
		change: "renamed to consumer.message_queue_batches (env KAFKAX_CONSUMER_MESSAGE_QUEUE_BATCHES) " +
			"and the unit changed from messages to batches: v1 buffered single messages, v3 buffers whole " +
			"poll batches, so a value carried over as is raises the memory ceiling " +
			"(partitions x batches x max_partition_bytes) by up to max_poll_records times; " +
			"pick a batch count, the default is 100",
	},
	{
		yamlPath: []string{"consumer", "handler_max_retries"},
		env:      "KAFKAX_CONSUMER_HANDLER_MAX_RETRIES",
		change: "renamed to consumer.handler_retries (env KAFKAX_CONSUMER_HANDLER_RETRIES) " +
			"and the meaning of 0 is inverted: in v1 zero meant retry forever and the value counted " +
			"handler calls, in v3 zero means no retries at all, N means N retries on top of the first " +
			"call and -1 means retry forever",
	},
}

// yamlKey — путь ключа в файле конфигурации, как его пишут в yaml.
func (k retiredKey) yamlKey() string {
	return strings.Join(k.yamlPath, ".")
}

// err собирает претензию к отставленному ключу. source — как ключ записан там,
// где его нашли: yaml-путь для файла, имя переменной для окружения. Разные
// формы записи в одном тексте только сбивали бы с толку — читатель правит тот
// источник, из которого пришёл ключ, а замену в обеих формах называет change.
func (k retiredKey) err(source string) error {
	return fmt.Errorf("%s is retired: %s", source, k.change)
}

// retiredEnvErrors ищет отставленные ключи в переменных окружения.
//
// Вызывается из commonErrors — единственного места, через которое проходят все
// три входа в валидацию (Validate, validateProducer, validateConsumer), а
// значит и оба конструктора. Обойти проверку, не обойдя валидацию целиком,
// нельзя.
//
// Секция ключа роли не играет: продюсерский конструктор ругается и на
// консьюмерский ключ. Переменная в окружении процесса — это утверждение
// оператора «настройка действует», и утверждение ложно независимо от того,
// какую роль процесс играет. Цена — сервис, которому досталась общая на всё
// развёртывание карта переменных, чинит её целиком, а не по частям; молчание
// стоило бы дороже, потому что второго повода узнать об отставленном ключе у
// него не будет.
//
// Значение переменной не разбирается: пустая KAFKAX_CONSUMER_MESSAGE_QUEUE_SIZE=
// — такое же заявление о настройке, как и заполненная.
func retiredEnvErrors() []error {
	var errs []error

	for _, key := range retiredKeys {
		if _, ok := os.LookupEnv(key.env); ok {
			errs = append(errs, key.err(key.env))
		}
	}

	return errs
}

// UnmarshalYAML перехватывает разбор Config из yaml, чтобы отставленный ключ в
// файле стал ошибкой загрузки, а не молча пропущенной строкой.
//
// Это единственная точка, мимо которой yaml пройти не может: cleanenv.ReadConfig
// разбирает файл декодером yaml, а декодер отдаёт узел этому методу, если он у
// типа есть. Проверять после разбора нечего — незнакомый ключ до Config не
// доезжает, в структуре его нет, и валидация видит поле с умолчанием, будто его
// и не задавали.
//
// Сигнатура — устаревшая форма yaml.Unmarshaler (та, что была основной в
// yaml.v2). Выбрана ради неё самой: она не упоминает типов библиотеки, поэтому
// gopkg.in/yaml.v3 не попадает в зависимости пакета и, через него, в граф
// модулей каждого потребителя. yaml.v3 эту форму поддерживает наравне с
// узловой (decode.go, obsoleteUnmarshaler), и это закреплено тестом, который
// гоняет настоящий декодер.
//
// Разбор идёт двумя проходами по одному узлу: сначала в карту — за именами
// ключей, которых в структуре нет, потом в саму структуру. Ошибка первого
// прохода игнорируется намеренно: узел, который не разобрался в карту (скаляр,
// список), не содержит и ключей, а внятную жалобу на него сформулирует второй
// проход.
//
// Поля, заданные до разбора, сохраняются: метод достраивает переданное
// значение, а не подменяет его нулевым, — иначе Logger или TLSConfig,
// выставленные до чтения файла, пропали бы.
func (c *Config) UnmarshalYAML(unmarshal func(any) error) error {
	var raw any
	if err := unmarshal(&raw); err == nil {
		if retired := newConfigError("config file", retiredYAMLErrors(raw)); retired != nil {
			return retired
		}
	}

	// Именованный тип без методов: разбор Config напрямую вызвал бы этот же
	// метод и ушёл в бесконечную рекурсию.
	type plainConfig Config

	plain := plainConfig(*c)
	if err := unmarshal(&plain); err != nil {
		return err
	}

	*c = Config(plain)

	return nil
}

// retiredYAMLErrors ищет отставленные ключи в разобранном дереве файла.
func retiredYAMLErrors(root any) []error {
	var errs []error

	for _, key := range retiredKeys {
		if yamlHasPath(root, key.yamlPath) {
			errs = append(errs, key.err(key.yamlKey()))
		}
	}

	return errs
}

// yamlHasPath отвечает, есть ли в дереве ключ по пути path. Значение не
// смотрится: наличие ключа и есть заявление о настройке, а «consumer:
// message_queue_size:» без значения — та же строка в файле, что и со значением.
func yamlHasPath(root any, path []string) bool {
	node := root

	for _, key := range path {
		child, ok := yamlChild(node, key)
		if !ok {
			return false
		}

		node = child
	}

	return true
}

// yamlChild достаёт значение по ключу из узла-отображения.
//
// Регистр не важен: декодер сопоставляет ключи файла с полями структуры
// регистронезависимо, поэтому «Message_Queue_Size» настроил бы поле наравне с
// «message_queue_size» — и отставленный ключ обязан ловиться так же.
//
// Разобраны обе формы отображения: yaml.v3 отдаёт map[string]any, yaml.v2 —
// map[any]any. Вторая ветка нужна не ради v2 как такового, а чтобы проверка не
// выродилась в тихое «ключа нет» на декодере, который отдаст другую карту.
func yamlChild(node any, key string) (any, bool) {
	switch m := node.(type) {
	case map[string]any:
		for k, v := range m {
			if strings.EqualFold(k, key) {
				return v, true
			}
		}
	case map[any]any:
		for k, v := range m {
			if s, ok := k.(string); ok && strings.EqualFold(s, key) {
				return v, true
			}
		}
	}

	return nil, false
}
