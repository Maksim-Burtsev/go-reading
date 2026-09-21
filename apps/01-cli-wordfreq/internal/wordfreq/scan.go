package wordfreq

import (
	"unicode"
	"unicode/utf8"
)

// ScanWords is a bufio.SplitFunc that splits text into words: runs of
// letters, digits and combining marks. An apostrophe or a hyphen between two
// such runes belongs to the word, so "don't" and "кто-то" are single words.
// Every other rune, including invalid UTF-8, separates words.
func ScanWords(data []byte, atEOF bool) (advance int, token []byte, err error) {
	start := 0
	for start < len(data) {
		if !atEOF && !utf8.FullRune(data[start:]) {
			return start, nil, nil
		}
		r, size := utf8.DecodeRune(data[start:])
		if isWordRune(r) {
			break
		}
		start += size
	}

	for i := start; i < len(data); {
		if !atEOF && !utf8.FullRune(data[i:]) {
			return start, nil, nil
		}
		r, size := utf8.DecodeRune(data[i:])
		switch {
		case isWordRune(r):
			i += size
			continue
		case isJoiner(r):
			rest := data[i+size:]
			if !atEOF && !utf8.FullRune(rest) {
				return start, nil, nil
			}
			if next, _ := utf8.DecodeRune(rest); isWordRune(next) {
				i += size
				continue
			}
		}
		return i + size, data[start:i], nil
	}

	if atEOF && start < len(data) {
		return len(data), data[start:], nil
	}
	return start, nil, nil
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.Is(unicode.Mn, r)
}

func isJoiner(r rune) bool {
	return r == '\'' || r == '\u2019' || r == '-'
}
