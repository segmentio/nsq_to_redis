// Package template provides a simple templating functionality.
// Given a template in the format "foo:{bar}" and data `{"bar":"b"}`,
// eval returns "foo:b".
// It allows multiple variables to be supplied. Given the format "{foo}:{bar}"
// and data `{"foo": "f", "bar": b}`, eval returns "f:b".
// It allows nested variables. Given the format "foo:{bar.baz}" and data
// and data `{"bar": { "baz" : "b"}}`, eval returns "foo:b".
package template

import (
	"bytes"
	"errors"

	"github.com/tidwall/gjson"
)

var ErrMissingClosingBrace = errors.New("missing '}'")

type T struct {
	nodes []node
}

// Returns a new template.
func New(format string) (*T, error) {
	var b bytes.Buffer
	state := sLiteral
	var nodes []node

	for i := 0; i < len(format); i++ {
		c := format[i]
		switch state {
		case sLiteral:
			switch c {
			case '{':
				nodes = append(nodes, node{
					literal: b.String(),
				})
				b.Reset()
				state = sVariable
			default:
				b.WriteByte(c)
			}
		case sVariable:
			switch c {
			case '}':
				nodes = append(nodes, node{
					variable: b.String(),
				})
				b.Reset()
				state = sLiteral
				break
			default:
				b.WriteByte(c)
			}
		}
	}

	if state == sVariable {
		return nil, ErrMissingClosingBrace
	}

	if b.Len() != 0 {
		nodes = append(nodes, node{
			literal: b.String(),
		})
	}

	return &T{nodes}, nil
}

const (
	sLiteral = iota
	sVariable
)

func (t *T) Eval(data string) (string, error) {
	var b bytes.Buffer
	for _, n := range t.nodes {
		b.WriteString(n.Eval(data))
	}
	return b.String(), nil
}

type node struct {
	variable string
	literal  string
}

func (n node) Eval(data string) string {
	if n.variable == "" {
		return n.literal
	}

	return gjson.Get(data, n.variable).String()
}
