package wordfreq

import (
	"bufio"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func scanAll(t *testing.T, r io.Reader) []string {
	t.Helper()
	sc := bufio.NewScanner(r)
	sc.Split(ScanWords)
	var words []string
	for sc.Scan() {
		words = append(words, sc.Text())
	}
	require.NoError(t, sc.Err())
	return words
}

func TestScanWords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty", in: "", want: nil},
		{name: "only separators", in: " ,.;\n\t!", want: nil},
		{name: "latin", in: "Hello, world!", want: []string{"Hello", "world"}},
		{name: "cyrillic", in: "Привет, мир!", want: []string{"Привет", "мир"}},
		{name: "digits", in: "go 1.27", want: []string{"go", "1", "27"}},
		{name: "inner apostrophe", in: "don't stop", want: []string{"don't", "stop"}},
		{name: "typographic apostrophe", in: "it’s fine", want: []string{"it’s", "fine"}},
		{name: "inner hyphen", in: "кто-то пришёл", want: []string{"кто-то", "пришёл"}},
		{name: "outer joiners", in: "'quoted' -dash- rock--roll", want: []string{"quoted", "dash", "rock", "roll"}},
		{name: "trailing joiner", in: "rock-", want: []string{"rock"}},
		{name: "combining mark", in: "cafe\u0301 ok", want: []string{"cafe\u0301", "ok"}},
		{name: "invalid utf-8", in: "ab\xffcd", want: []string{"ab", "cd"}},
		{name: "mixed scripts", in: "Go и Питон", want: []string{"Go", "и", "Питон"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, scanAll(t, strings.NewReader(tt.in)))
			require.Equal(t, tt.want, scanAll(t, iotest.OneByteReader(strings.NewReader(tt.in))), "one byte per read")
		})
	}
}
