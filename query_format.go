package ejdb

import (
	"encoding/json"
	"fmt"
	"strings"
)

func (q *Query) Canonical() string {
	return q.parsed.canonical()
}

func (pq parsedQuery) canonical() string {
	parts := []string{formatFilterNode(pq.filter, 0)}
	switch pq.action {
	case actionDelete:
		parts = append(parts, "del")
	case actionApply:
		parts = append(parts, "apply "+formatValueExpr(pq.actionArg))
	case actionUpsert:
		parts = append(parts, "upsert "+formatValueExpr(pq.actionArg))
	}
	if pq.projection != nil {
		parts = append(parts, formatProjection(pq.projection))
	}
	if opts := formatOptions(pq); opts != "" {
		parts = append(parts, opts)
	}
	return strings.Join(parts, "\n| ")
}

func formatFilterNode(n filterNode, parentPrec int) string {
	switch x := n.(type) {
	case filterAtom:
		return formatQueryFilter(x.term)
	case filterAnd:
		return wrapFilter(formatFilterNode(x.left, 2)+" and "+formatFilterNode(x.right, 2), parentPrec, 2)
	case filterOr:
		return wrapFilter(formatFilterNode(x.left, 1)+" or "+formatFilterNode(x.right, 1), parentPrec, 1)
	case filterNot:
		return "not " + formatFilterNode(x.node, 3)
	case filterGroup:
		return "(" + formatFilterNode(x.node, 0) + ")"
	default:
		return "/*"
	}
}

func wrapFilter(s string, parent, self int) string {
	if self < parent {
		return "(" + s + ")"
	}
	return s
}

func formatQueryFilter(f queryFilter) string {
	prefix := ""
	if f.anchor != "" {
		prefix = "@" + f.anchor
	}
	if f.all {
		if f.allPath != "" {
			return prefix + f.allPath
		}
		return prefix + "/*"
	}
	if f.pk != nil {
		return prefix + "/=" + formatValueExpr(*f.pk)
	}
	if len(f.chain) > 0 {
		var b strings.Builder
		b.WriteString(prefix)
		for _, st := range f.chain {
			b.WriteByte('/')
			switch st.kind {
			case "field":
				b.WriteString(st.key)
			case "any":
				b.WriteString("*")
			case "desc":
				b.WriteString("**")
			case "expr":
				b.WriteByte('[')
				b.WriteString(formatExprNode(st.expr, 0))
				b.WriteByte(']')
			}
		}
		return b.String()
	}
	path := formatPointerPath(f.pathPattern)
	if f.expr != nil {
		path += "/[" + formatExprNode(f.expr, 0) + "]"
	}
	return prefix + path
}

func formatExprNode(n exprNode, parentPrec int) string {
	switch x := n.(type) {
	case exprCmp:
		return formatComparison(x.cmp)
	case exprAnd:
		return wrapFilter(formatExprNode(x.left, 2)+" and "+formatExprNode(x.right, 2), parentPrec, 2)
	case exprOr:
		return wrapFilter(formatExprNode(x.left, 1)+" or "+formatExprNode(x.right, 1), parentPrec, 1)
	case exprNot:
		return "not " + formatExprNode(x.node, 3)
	case exprGroup:
		return "(" + formatExprNode(x.node, 0) + ")"
	default:
		return ""
	}
}

func formatComparison(c comparison) string {
	op := c.op
	switch op {
	case "prefix":
		op = "~"
	case "nprefix":
		op = "!~"
	case "nre":
		op = "not re"
	}
	return c.key + " " + op + " " + formatValueExpr(c.value)
}

func formatValueExpr(v valueExpr) string {
	if v.ph != nil {
		return formatPlaceholder(*v.ph)
	}
	return formatLiteral(v.literal)
}

func formatLiteral(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func formatPlaceholder(ph placeholderRef) string {
	if ph.positional {
		return ":?"
	}
	return ":" + ph.name
}

func formatProjection(p *projectionSpec) string {
	if p == nil || len(p.terms) == 0 {
		return "all"
	}
	var b strings.Builder
	for i := 0; i < len(p.terms); i++ {
		term := p.terms[i]
		if i > 0 {
			if term.include {
				b.WriteString(" + ")
			} else {
				b.WriteString(" - ")
			}
		} else if !term.include {
			b.WriteString("- ")
		}
		if term.all {
			b.WriteString("all")
			continue
		}
		if grouped, next := formatProjectionGroup(p.terms, i); next > i {
			b.WriteString(grouped)
			i = next
			continue
		}
		b.WriteString(formatPathTemplate(term.path))
		if term.join != "" {
			b.WriteByte('<')
			b.WriteString(term.join)
		}
	}
	return b.String()
}

func formatProjectionGroup(terms []projectionTerm, i int) (string, int) {
	if i+1 >= len(terms) || terms[i].all || terms[i].join != "" || len(terms[i].path.parts) < 2 {
		return "", i
	}
	include := terms[i].include
	prefix := terms[i].path.parts[:len(terms[i].path.parts)-1]
	fields := []string{terms[i].path.parts[len(terms[i].path.parts)-1].literal}
	j := i + 1
	for ; j < len(terms); j++ {
		t := terms[j]
		if t.include != include || t.all || t.join != "" || len(t.path.parts) != len(prefix)+1 {
			break
		}
		if !samePathParts(prefix, t.path.parts[:len(t.path.parts)-1]) {
			break
		}
		last := t.path.parts[len(t.path.parts)-1]
		if last.ph != nil {
			break
		}
		fields = append(fields, last.literal)
	}
	if len(fields) < 2 {
		return "", i
	}
	return formatPathTemplate(pathTemplate{parts: prefix}) + "/{" + strings.Join(fields, ",") + "}", j - 1
}

func samePathParts(a, b []pathPart) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		switch {
		case a[i].ph == nil && b[i].ph == nil:
			if a[i].literal != b[i].literal {
				return false
			}
		case a[i].ph != nil && b[i].ph != nil:
			if a[i].ph.positional != b[i].ph.positional || a[i].ph.name != b[i].ph.name || a[i].ph.index != b[i].ph.index {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func formatOptions(pq parsedQuery) string {
	sortParts := make([]string, 0, len(pq.sorts))
	for _, s := range pq.sorts {
		if s.desc {
			sortParts = append(sortParts, "desc "+formatSort(s))
		} else {
			sortParts = append(sortParts, "asc "+formatSort(s))
		}
	}
	otherParts := make([]string, 0, 4)
	if pq.skipSet {
		if pq.skipPH != nil {
			otherParts = append(otherParts, "skip "+formatPlaceholder(*pq.skipPH))
		} else {
			otherParts = append(otherParts, fmt.Sprintf("skip %d", pq.skip))
		}
	}
	if pq.limitSet {
		if pq.limitPH != nil {
			otherParts = append(otherParts, "limit "+formatPlaceholder(*pq.limitPH))
		} else {
			otherParts = append(otherParts, fmt.Sprintf("limit %d", pq.limit))
		}
	}
	if pq.count {
		otherParts = append(otherParts, "count")
	}
	if pq.noidx {
		otherParts = append(otherParts, "noidx")
	}
	if pq.inverse {
		otherParts = append(otherParts, "inverse")
	}
	if len(sortParts) == 0 {
		return strings.Join(otherParts, " ")
	}
	if len(otherParts) > 0 {
		sortParts = append(sortParts, strings.Join(otherParts, " "))
	}
	if len(sortParts) > 1 {
		return strings.Join(sortParts, "\n  ")
	}
	return sortParts[0]
}

func formatSort(s querySort) string {
	if s.placeholder != nil {
		return formatPlaceholder(*s.placeholder)
	}
	return formatPathTemplate(s.path)
}

func formatPathTemplate(p pathTemplate) string {
	if len(p.parts) == 0 {
		return "/"
	}
	parts := make([]string, 0, len(p.parts))
	for _, part := range p.parts {
		if part.ph != nil {
			parts = append(parts, formatPlaceholder(*part.ph))
		} else {
			parts = append(parts, part.literal)
		}
	}
	return "/" + strings.Join(parts, "/")
}

func formatPointerPath(ptr string) string {
	toks, err := pointerTokens(ptr)
	if err != nil || len(toks) == 0 {
		return "/"
	}
	return "/" + strings.Join(toks, "/")
}
