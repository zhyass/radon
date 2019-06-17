/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sqltypes

import (
	"bytes"
	"unicode"
)

const (
	patternMatch = iota
	patternOne
	patternAny
)

type cmpType int

const (
	percent = iota + 1
	match
	like
)

type CmpLike struct {
	patChars   []byte
	patTyps    []byte
	ignoreCase bool
	cmpTyp     cmpType
}

func NewCmpLike(pattern []byte, escape byte, ignoreCase bool) *CmpLike {
	if bytes.Compare([]byte("%"), pattern) == 0 {
		return &CmpLike{ignoreCase: ignoreCase, cmpTyp: percent}
	}

	length := len(pattern)
	if length == 0 {
		return &CmpLike{ignoreCase: ignoreCase, cmpTyp: like}
	}

	patLen := 0
	lastAny := false
	isFullMatch := true
	patChars := make([]byte, 0, len(pattern))
	patTyps := make([]byte, 0, len(pattern))
	for i := 0; i < length; i++ {
		var typ byte
		b := pattern[i]
		switch b {
		case escape:
			lastAny = false
			typ = patternMatch
			if i < length-1 {
				i++
				b = pattern[i]
				if !(b == escape || b == '_' || b == '%') {
					// Invalid escape, fall back to escape byte.
					i--
					b = escape
				}
			}
		case '_':
			isFullMatch = false
			if lastAny {
				patChars[patLen-1], patChars[patLen] = b, patChars[patLen-1]
				patTyps[patLen-1], patTyps[patLen] = patternOne, patternAny
				patLen++
				continue
			}
			typ = patternOne
		case '%':
			if lastAny {
				continue
			}
			isFullMatch = false
			typ = patternAny
			lastAny = true
		default:
			typ = patternMatch
			lastAny = false
		}
		patChars[patLen] = b
		patTyps[patLen] = typ
		patLen++
	}
	if isFullMatch {
		return &CmpLike{patChars, patTyps, ignoreCase, match}
	}
	return &CmpLike{patChars, patTyps, ignoreCase, like}
}

func (c *CmpLike) Compare(val []byte) bool {
	switch c.cmpTyp {
	case percent:
		return true
	case match:
		if c.ignoreCase {
			return bytes.EqualFold(c.patChars, val)
		}
		return bytes.Compare(c.patChars, val) == 0
	case like:
		return isMatch(val, c.patChars, c.patTyps, c.ignoreCase)
	}
	panic("unknow.cmp.type")
}

func isMatch(val, patChars, patTyps []byte, ignodeCase bool) bool {
	idx := 0
	for i := 0; i < len(patChars); i++ {
		switch patTyps[i] {
		case patternMatch:
			if idx >= len(val) || !compareByte(val[idx], patChars[i], ignodeCase) {
				return false
			}
			idx++
		case patternOne:
			idx++
			if idx > len(val) {
				return false
			}
		case patternAny:
			i++
			if i == len(patChars) {
				return true
			}
			for idx < len(val) {
				if compareByte(val[idx], patChars[i], ignodeCase) && isMatch(val[idx:], patChars[i:], patTyps[i:], ignodeCase) {
					return true
				}
				idx++
			}
			return false
		}
	}
	return idx == len(val)
}

func compareByte(a, b byte, ignoreCase bool) bool {
	if ignoreCase {
		return a == b
	}

	return unicode.ToUpper(rune(a)) == unicode.ToUpper(rune(b))
}
