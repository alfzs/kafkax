package kafkax

import (
	"strings"
	"testing"
)

// TestPanicSiteValuesAreStable стережёт строковые значения PanicSite.
//
// Тип и константы (RF-API-09) защищают потребителя от переименования, но не
// защищают дашборды: значение уходит в label site метрики kafkax.consumer.panics
// и в поле site лога, а на них построены алерты и таблица в README. Смена
// значения — ломающее изменение, невидимое компилятору, поэтому оно фиксируется
// здесь списком.
func TestPanicSiteValuesAreStable(t *testing.T) {
	t.Parallel()

	sites := []PanicSite{
		PanicSiteHandler,
		PanicSiteProcessMessage,
		PanicSitePartitionWorker,
		PanicSitePollLoop,
		PanicSiteMessageSkipped,
		PanicSitePanicHook,
	}

	// Склейкой, а не таблицей: так одно сравнение проверяет и сами значения, и
	// их различность — совпадение двух сайтов сделало бы метрику неразличимой
	// по site, ради чего атрибут и заведён.
	const want = "handler|process_message|partition_worker|poll_loop|on_message_skipped|on_panic"

	parts := make([]string, 0, len(sites))
	for _, site := range sites {
		parts = append(parts, string(site))
	}

	if got := strings.Join(parts, "|"); got != want {
		t.Errorf("значения PanicSite = %q, want %q\nновая точка recover? добавьте её в конец", got, want)
	}
}
