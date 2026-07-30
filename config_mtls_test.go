package kafkax

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Ветка mTLS: клиентская пара «сертификат + ключ», которую tlsConfig грузит с
// диска.
//
// Класс дефекта — код, который никогда не исполнялся. Валидация конфигурации
// путей не открывает (и правильно делает: Validate обязана быть чистой), а
// единственный тест «полный mTLS» в config_test.go подставляет несуществующие
// /c.pem и /k.pem. В результате вызов tls.LoadX509KeyPair не проверялся ни разу
// ни на успехе, ни на отказе: опечатка в имени поля, перепутанные местами
// аргументы cert/key или проглоченная ошибка обнаружились бы только у того, кто
// первым включит mTLS в проде, и в виде отказа установить соединение с
// брокером.
//
// Пара поэтому строится настоящая — самоподписанная, в t.TempDir(). Фикстуры в
// репозитории не годятся: сертификат протухает, и тест начинает падать через
// год после написания по причине, не имеющей отношения к коду.

// mtlsWritePEM кладёт PEM-блок в файл каталога dir и возвращает путь.
func mtlsWritePEM(t *testing.T, dir, name, blockType string, der []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("запись %s: %v", path, err)
	}

	return path
}

// mtlsSelfSignedPair строит самоподписанную пару в каталоге dir и возвращает
// пути к сертификату и ключу вместе с DER сертификата.
//
// DER отдаётся наружу не для полноты: без него ассерт «сертификат загружен»
// выродился бы в «в поле лежит хоть что-то». Сравнение с исходными байтами —
// единственный способ доказать, что прочитан именно указанный в конфигурации
// файл, а не подобранный где-то ещё.
//
// ECDSA P-256, а не RSA: генерация RSA-2048 занимает десятки миллисекунд и
// плохо предсказуема по времени, а криптостойкость здесь не проверяется вовсе —
// проверяется чтение файлов.
func mtlsSelfSignedPair(t *testing.T, dir string) (certPath, keyPath string, certDER []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "kafkax-test-client"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err = x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("x509.MarshalPKCS8PrivateKey: %v", err)
	}

	certPath = mtlsWritePEM(t, dir, "client.pem", "CERTIFICATE", certDER)
	keyPath = mtlsWritePEM(t, dir, "client-key.pem", "PRIVATE KEY", keyDER)

	return certPath, keyPath, certDER
}

// TestTLSConfigLoadsClientKeyPair — указанная в конфигурации клиентская пара
// действительно попадает в *tls.Config и доезжает до клиента franz-go.
//
// Единственный тест, который вообще исполняет ветку tls.LoadX509KeyPair. Ассерт
// стоит на трёх вещах сразу, и каждая закрывает свой способ сломаться незаметно:
// длина Certificates отличает «пару загрузили» от «ветку не вошли», сравнение
// DER с исходником отличает загрузку указанного файла от любого другого, а
// проверка через kgo.NewClient — от «собрали *tls.Config и забыли отдать
// транспорту». Последнее особенно важно: конфигурация без DialTLSConfig
// молча уходит в незашифрованное соединение вместо отказа.
func TestTLSConfigLoadsClientKeyPair(t *testing.T) {
	t.Parallel()

	certPath, keyPath, certDER := mtlsSelfSignedPair(t, t.TempDir())

	cfg := testConfig(t)
	cfg.TLS = TLS{
		Enabled:        true,
		ServerName:     "broker.example",
		ClientCertPath: certPath,
		ClientKeyPath:  keyPath,
	}

	got, err := cfg.tlsConfig(testLogger(t))
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}

	if got == nil {
		t.Fatal("tlsConfig = nil при заданной клиентской паре")
	}

	if len(got.Certificates) != 1 {
		t.Fatalf("Certificates: %d, want 1 — клиентская пара не загружена", len(got.Certificates))
	}

	leaf := got.Certificates[0]
	if len(leaf.Certificate) == 0 {
		t.Fatal("в загруженной паре нет ни одного сертификата")
	}

	if !bytes.Equal(leaf.Certificate[0], certDER) {
		t.Error("загружен не тот сертификат, что указан в tls.client_cert_path")
	}

	// Ключ без пары бесполезен: рукопожатие mTLS требует подписи, а не только
	// предъявления сертификата.
	if leaf.PrivateKey == nil {
		t.Error("приватный ключ не загружен: сертификат нечем подтвердить")
	}

	opts, err := cfg.producerOpts(testLogger(t))
	if err != nil {
		t.Fatalf("producerOpts: %v", err)
	}

	dialCfg, ok := optsClient(t, opts).OptValue(kgo.DialTLSConfig).(*tls.Config)
	if !ok || dialCfg == nil {
		t.Fatal("DialTLSConfig не задан: клиент пойдёт к брокеру без TLS")
	}

	if len(dialCfg.Certificates) != 1 {
		t.Errorf("Certificates у клиента: %d, want 1 — пара потерялась по дороге", len(dialCfg.Certificates))
	}
}

// TestTLSConfigLoadsCACertificate — годный CA-файл доезжает до RootCAs.
//
// Тот же класс дефекта с другой стороны соединения, и до появления настоящего
// сертификата в тестах он был непокрыт ровно по той же причине: проверялись
// только отказы caCertPool (файла нет, файл не PEM), а успешная ветка —
// присваивание RootCAs — не исполнялась ни разу. Молча потерянный пул означал
// бы проверку брокера системным trust store вместо указанного в конфигурации:
// соединение поднимется, и подмена брокера останется незамеченной.
//
// Сравнить пул с исходником напрямую нечем — x509.CertPool непрозрачен, — но
// его назначение наблюдаемо: пул, в который попал наш самоподписанный корень,
// обязан признать этот же сертификат валидным. Проверка через Verify, а не
// через число Subjects: последнее устарело и, главное, не отличает «в пуле
// лежит нужный сертификат» от «в пуле лежит какой-то».
func TestTLSConfigLoadsCACertificate(t *testing.T) {
	t.Parallel()

	certPath, _, certDER := mtlsSelfSignedPair(t, t.TempDir())

	cfg := testConfig(t)
	cfg.TLS = TLS{Enabled: true, CACertPath: certPath}

	got, err := cfg.tlsConfig(testLogger(t))
	if err != nil {
		t.Fatalf("tlsConfig: %v", err)
	}

	if got.RootCAs == nil {
		t.Fatal("RootCAs = nil: брокер будет проверяться системным trust store вместо указанного CA")
	}

	root, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}

	verifyOpts := x509.VerifyOptions{
		Roots:     got.RootCAs,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	if _, err := root.Verify(verifyOpts); err != nil {
		t.Errorf("собранный пул не признаёт указанный в конфигурации корень: %v", err)
	}
}

// TestTLSConfigRejectsBrokenClientKeyPair — нечитаемая или несогласованная пара
// валит сборку конфигурации, а не отбрасывается молча.
//
// Обратная половина той же непокрытой ветки, и по последствиям она хуже
// успешной: молчаливый откат оставил бы *tls.Config без Certificates, соединение
// поднялось бы, а брокер отверг бы клиента уже на рукопожатии — с сообщением о
// сертификате, а не о конфигурации. Отсюда два ассерта: ошибка обязана быть, и
// вместе с ней обязан быть nil-конфиг, чтобы вызывающий физически не мог
// продолжить с полуфабрикатом.
//
// Три случая — три разных места отказа: файла нет (ошибка ОС), файл не PEM
// (ошибка разбора), пара из разных ключей (ошибка криптографической проверки).
// Последний важнее прочих: файлы читаются оба и оба валидны, так что поймать
// такую конфигурацию можно только сверкой ключа с сертификатом.
func TestTLSConfigRejectsBrokenClientKeyPair(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	certPath, keyPath, _ := mtlsSelfSignedPair(t, dir)

	// Вторая пара нужна целиком, но используется от неё только ключ: он валиден
	// сам по себе и не подходит к сертификату первой.
	_, otherKeyPath, _ := mtlsSelfSignedPair(t, t.TempDir())

	garbagePath := filepath.Join(dir, "garbage.pem")
	if err := os.WriteFile(garbagePath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("подготовка файла: %v", err)
	}

	missingPath := filepath.Join(dir, "missing.pem")

	tests := []struct {
		name string
		cert string
		key  string
		want string
	}{
		{
			name: "файла сертификата нет",
			cert: missingPath,
			key:  keyPath,
			// Путь в тексте обязателен: без него сообщение не отличает опечатку
			// в client_cert_path от опечатки в client_key_path.
			want: missingPath,
		},
		{
			name: "сертификат не PEM",
			cert: garbagePath,
			key:  keyPath,
			want: "failed to find any PEM data",
		},
		{
			name: "ключ от другой пары",
			cert: certPath,
			key:  otherKeyPath,
			want: "private key does not match public key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := testConfig(t)
			cfg.TLS = TLS{Enabled: true, ClientCertPath: tt.cert, ClientKeyPath: tt.key}

			got, err := cfg.tlsConfig(testLogger(t))
			// Общий префикс называет секцию конфигурации; частный текст —
			// причину. Проверяются оба: без префикса ошибка ОС о файле не
			// подсказывает, при чём тут Kafka, а без причины непонятно, что
			// именно чинить в файле.
			cfgWantErr(t, err, "loading client key pair", tt.want)

			if got != nil {
				t.Errorf("вместе с ошибкой возвращён *tls.Config %+v — соединение поднимется без клиентского сертификата", got)
			}
		})
	}
}
