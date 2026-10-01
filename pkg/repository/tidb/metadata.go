package tidb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

type metadataNode struct {
	id, parent int
	property   []byte
	typ        string
	atom, text []byte
}

func parseJSON(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	var more any
	if err := d.Decode(&more); err != io.EOF {
		return nil, fmt.Errorf("invalid trailing JSON")
	}
	return v, nil
}
func atomValue(v any) (string, []byte, error) {
	switch x := v.(type) {
	case nil:
		return "null", nil, nil
	case bool:
		return "boolean", []byte(strconv.FormatBool(x)), nil
	case string:
		return "string", []byte(x), nil
	case json.Number:
		n, ok := new(big.Rat).SetString(string(x))
		if !ok {
			return "", nil, fmt.Errorf("invalid JSON number")
		}
		return "number", []byte(n.RatString()), nil
	case map[string]any:
		return "object", nil, nil
	case []any:
		return "array", nil, nil
	default:
		return "", nil, fmt.Errorf("unsupported JSON value %T", v)
	}
}
func jsonString(v string) string {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(v)
	return strings.TrimSuffix(b.String(), "\n")
}
func pgNumberText(s string) string {
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == 'e' || r == 'E' })
	exponent := 0
	if len(parts) == 2 {
		exponent, _ = strconv.Atoi(parts[1])
	}
	mantissa := parts[0]
	point := strings.IndexByte(mantissa, '.')
	if point < 0 {
		point = len(mantissa)
	}
	digits := strings.ReplaceAll(mantissa, ".", "")
	point += exponent
	// The PostgreSQL JSON numeric range is finite, so expansion is bounded.
	if point > 131072 || point < -16383 {
		return sign + s
	}
	if point <= 0 {
		digits = "0." + strings.Repeat("0", -point) + digits
	} else if point >= len(digits) {
		digits += strings.Repeat("0", point-len(digits))
	} else {
		digits = digits[:point] + "." + digits[point:]
	}
	integer, fraction, has := strings.Cut(digits, ".")
	integer = strings.TrimLeft(integer, "0")
	if integer == "" {
		integer = "0"
	}
	if has {
		digits = integer + "." + fraction
	} else {
		digits = integer
	}
	if n, ok := new(big.Rat).SetString(digits); ok && n.Sign() == 0 {
		sign = ""
	}
	return sign + digits
}
func pgJSONText(v any, top bool) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case json.Number:
		return pgNumberText(string(x))
	case bool:
		return strconv.FormatBool(x)
	case string:
		if top {
			return x
		}
		return jsonString(x)
	case []any:
		out := make([]string, len(x))
		for i, v := range x {
			out[i] = pgJSONText(v, false)
		}
		return "[" + strings.Join(out, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			if len(keys[i]) != len(keys[j]) {
				return len(keys[i]) < len(keys[j])
			}
			return keys[i] < keys[j]
		})
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			out = append(out, jsonString(k)+": "+pgJSONText(x[k], false))
		}
		return "{" + strings.Join(out, ", ") + "}"
	}
	return ""
}
func metadataNodes(raw []byte) ([]metadataNode, error) {
	v, err := parseJSON(raw)
	if err != nil {
		return nil, err
	}
	var result []metadataNode
	var visit func(any, int, []byte) error
	visit = func(v any, parent int, key []byte) error {
		typ, atom, err := atomValue(v)
		if err != nil {
			return err
		}
		id := len(result) + 1
		node := metadataNode{id: id, parent: parent, property: key, typ: typ, atom: atom}
		if typ != "null" {
			node.text = []byte(pgJSONText(v, true))
		}
		result = append(result, node)
		switch x := v.(type) {
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if err = visit(x[k], id, []byte(k)); err != nil {
					return err
				}
			}
		case []any:
			for _, v := range x {
				if err = visit(v, id, nil); err != nil {
					return err
				}
			}
		}
		return nil
	}
	err = visit(v, 0, nil)
	return result, err
}
func (s *store) publishMetadata(ctx context.Context, tx *sql.Tx, rows []entity) error {
	var deletes []string
	var deleteArgs []any
	var values []string
	var args []any
	size := 0
	flush := func() error {
		if len(values) == 0 {
			return nil
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO v1_olap_metadata(tenant_id,kind,entity_key,inserted_at,node_id,parent_node,property_name,property_hash,node_type,atom,atom_hash,pg_text) VALUES "+strings.Join(values, ",")+" ON DUPLICATE KEY UPDATE node_id=node_id", args...)
		values = nil
		args = nil
		size = 0
		return err
	}
	type indexed struct {
		row   entity
		nodes []metadataNode
	}
	var indexedRows []indexed
	for _, row := range rows {
		var raw []byte
		switch row.Kind {
		case "task":
			v, err := decodeEntity[sqlcv1.V1TasksOlap](row)
			if err != nil {
				return err
			}
			raw = v.AdditionalMetadata
		case "dag":
			v, err := decodeEntity[sqlcv1.V1DagsOlap](row)
			if err != nil {
				return err
			}
			raw = v.AdditionalMetadata
			deletes = append(deletes, "(tenant_id=? AND kind=? AND entity_key=? AND inserted_at=?)")
			deleteArgs = append(deleteArgs, uuidArg(row.Tenant), row.Kind, []byte(row.Key), row.InsertedAt)
		case "event":
			v, err := decodeEntity[sqlcv1.V1EventsOlap](row)
			if err != nil {
				return err
			}
			raw = v.AdditionalMetadata
		default:
			continue
		}
		if len(raw) == 0 || string(raw) == "{}" {
			continue
		}
		nodes, err := metadataNodes(raw)
		if err != nil {
			return err
		}
		indexedRows = append(indexedRows, indexed{row, nodes})
	}
	if len(deletes) > 0 {
		if _, err := tx.ExecContext(ctx, "DELETE FROM v1_olap_metadata WHERE "+strings.Join(deletes, " OR "), deleteArgs...); err != nil {
			return err
		}
	}
	for _, item := range indexedRows {
		row := item.row
		for _, n := range item.nodes {
			if len(values) == 200 || size >= 1<<20 {
				if err := flush(); err != nil {
					return err
				}
			}
			ph, ah := sha256.Sum256(n.property), sha256.Sum256(n.atom)
			values = append(values, "(?,?,?,?,?,?,?,?,?,?,?,?)")
			args = append(args, uuidArg(row.Tenant), row.Kind, []byte(row.Key), row.InsertedAt, n.id, n.parent, n.property, ph[:], n.typ, n.atom, ah[:], n.text)
			size += len(n.property) + len(n.atom) + len(n.text) + 200
		}
	}
	return flush()
}

type metadataCompiler struct {
	serial      int
	args        []any
	owner, kind string
}

func (c *metadataCompiler) node(v any, parent, name string, key []byte, root bool) (string, error) {
	typ, atom, err := atomValue(v)
	if err != nil {
		return "", err
	}
	alias := fmt.Sprintf("mn%d", c.serial)
	c.serial++
	parts := []string{alias + ".tenant_id=" + c.owner + ".tenant_id", alias + ".kind=" + c.kind, alias + ".entity_key=" + c.owner + ".entity_key", alias + ".inserted_at=" + c.owner + ".inserted_at", alias + ".parent_node=" + parent}
	if root {
		parts = append(parts, alias+".node_id=1")
	} else if name != "" {
		hash := sha256.Sum256(key)
		parts = append(parts, alias+".property_hash=?", alias+".property_name=?")
		c.args = append(c.args, hash[:], key)
	}
	parts = append(parts, alias+".node_type=?")
	c.args = append(c.args, typ)
	if typ != "object" && typ != "array" && typ != "null" {
		hash := sha256.Sum256(atom)
		parts = append(parts, alias+".atom_hash=?", alias+".atom=?")
		c.args = append(c.args, hash[:], atom)
	}
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			q, err := c.node(x[k], alias+".node_id", "property", []byte(k), false)
			if err != nil {
				return "", err
			}
			parts = append(parts, q)
		}
	case []any:
		for _, v := range x {
			q, err := c.node(v, alias+".node_id", "", nil, false)
			if err != nil {
				return "", err
			}
			parts = append(parts, q)
		}
	}
	return "EXISTS(SELECT 1 FROM v1_olap_metadata " + alias + " WHERE " + strings.Join(parts, " AND ") + ")", nil
}
func metadataPredicate(owner, kind string, expected map[string]any, op repository.AdditionalMetadataOperator) (string, []any, error) {
	if len(expected) == 0 {
		return "", nil, nil
	}
	raw, err := json.Marshal(expected)
	if err != nil {
		return "", nil, err
	}
	value, err := parseJSON(raw)
	if err != nil {
		return "", nil, err
	}
	if op == repository.AdditionalMetadataOperatorAnd {
		c := metadataCompiler{owner: owner, kind: kind}
		q, err := c.node(value, "0", "", nil, true)
		return q, c.args, err
	}
	keys := make([]string, 0, len(expected))
	for k := range expected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	var args []any
	for _, k := range keys {
		want, ok := expected[k].(string)
		if !ok {
			return "", nil, fmt.Errorf("metadata OR values must be strings")
		}
		h := sha256.Sum256([]byte(k))
		parts = append(parts, "(mn.property_hash=? AND mn.property_name=? AND mn.pg_text=?)")
		args = append(args, h[:], []byte(k), []byte(want))
	}
	return "EXISTS(SELECT 1 FROM v1_olap_metadata mn WHERE mn.tenant_id=" + owner + ".tenant_id AND mn.kind=" + kind + " AND mn.entity_key=" + owner + ".entity_key AND mn.inserted_at=" + owner + ".inserted_at AND mn.parent_node=1 AND (" + strings.Join(parts, " OR ") + "))", args, nil
}
