package ejdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Query struct {
	collection string
	text       string
	parsed     parsedQuery
	named      map[string]any
	positional map[int]any
}

func (db *DB) CreateQuery(collection, text string) (*Query, error) {
	return NewQuery(collection, text)
}

func NewQuery(collection, text string) (*Query, error) {
	pq, err := parseQuery(text)
	if err != nil {
		return nil, err
	}
	q := &Query{
		collection: strings.TrimSpace(collection),
		text:       text,
		parsed:     pq,
		named:      make(map[string]any),
		positional: make(map[int]any),
	}
	if pq.collection != "" {
		q.collection = pq.collection
	}
	if q.collection == "" {
		return nil, withCodef(CodeNoCollection, "query has no collection")
	}
	return q, nil
}

func (q *Query) Collection() string {
	return q.collection
}

func (q *Query) Text() string {
	return q.text
}

func (q *Query) SetJSON(name string, index int, val any) error {
	if name != "" {
		q.named[name] = val
		return nil
	}
	if index < 0 {
		return withCodef(CodeInvalidPlaceholder, "negative positional placeholder index: %d", index)
	}
	q.positional[index] = val
	return nil
}

func (q *Query) SetString(name string, index int, val string) error {
	return q.SetJSON(name, index, val)
}

func (q *Query) SetI64(name string, index int, val int64) error {
	return q.SetJSON(name, index, val)
}

func (q *Query) SetF64(name string, index int, val float64) error {
	return q.SetJSON(name, index, val)
}

func (q *Query) SetBool(name string, index int, val bool) error {
	return q.SetJSON(name, index, val)
}

func (q *Query) SetNull(name string, index int) error {
	return q.SetJSON(name, index, nil)
}

func (q *Query) SetRegexp(name string, index int, expr string) error {
	_, err := regexp.Compile(expr)
	if err != nil {
		return withCodef(CodeInvalidQuery, "invalid regexp: %v", err)
	}
	return q.SetJSON(name, index, regexpExpr(expr))
}

func (q *Query) resolvePlaceholder(ph placeholderRef) (any, error) {
	if ph.positional {
		v, ok := q.positional[ph.index]
		if !ok {
			return nil, withCodef(CodeUnsetPlaceholder, "unset positional placeholder :? at index %d", ph.index)
		}
		return v, nil
	}
	v, ok := q.named[ph.name]
	if !ok {
		return nil, withCodef(CodeUnsetPlaceholder, "unset placeholder :%s", ph.name)
	}
	return v, nil
}

type placeholderRef struct {
	positional bool
	name       string
	index      int
}

type valueExpr struct {
	literal any
	ph      *placeholderRef
}

func (v valueExpr) resolve(q *Query) (any, error) {
	if v.ph == nil {
		return v.literal, nil
	}
	return q.resolvePlaceholder(*v.ph)
}

type regexpExpr string

type queryAction int

const (
	actionNone queryAction = iota
	actionDelete
	actionApply
	actionUpsert
)

type parsedQuery struct {
	collection string
	filter     filterNode
	action     queryAction
	actionArg  valueExpr
	projection *projectionSpec
	sort       *querySort
	skip       int
	limit      int
	count      bool
	noidx      bool
	inverse    bool
}

type querySort struct {
	desc bool
	path pathTemplate
}

type filterNode interface {
	match(doc any, id int64, q *Query, st *dbState) (bool, error)
	candidate(col *collectionState, q *Query) ([]int64, bool)
}

type filterAtom struct {
	term queryFilter
}

func (f filterAtom) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	return f.term.match(doc, id, q, st)
}

func (f filterAtom) candidate(col *collectionState, q *Query) ([]int64, bool) {
	return f.term.candidate(col, q)
}

type filterAnd struct {
	left  filterNode
	right filterNode
}

func (f filterAnd) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	lm, err := f.left.match(doc, id, q, st)
	if err != nil || !lm {
		return lm, err
	}
	return f.right.match(doc, id, q, st)
}

func (f filterAnd) candidate(col *collectionState, q *Query) ([]int64, bool) {
	if ids, ok := f.left.candidate(col, q); ok {
		return ids, true
	}
	return f.right.candidate(col, q)
}

type filterOr struct {
	left  filterNode
	right filterNode
}

func (f filterOr) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	lm, err := f.left.match(doc, id, q, st)
	if err != nil {
		return false, err
	}
	if lm {
		return true, nil
	}
	return f.right.match(doc, id, q, st)
}

func (f filterOr) candidate(col *collectionState, q *Query) ([]int64, bool) {
	return nil, false
}

type filterNot struct {
	node filterNode
}

func (f filterNot) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	m, err := f.node.match(doc, id, q, st)
	if err != nil {
		return false, err
	}
	return !m, nil
}

func (f filterNot) candidate(col *collectionState, q *Query) ([]int64, bool) {
	return nil, false
}

type queryFilter struct {
	all         bool
	pk          *valueExpr
	pathPattern string
	expr        exprNode
}

func (f queryFilter) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	if f.all {
		return true, nil
	}
	if f.pk != nil {
		v, err := f.pk.resolve(q)
		if err != nil {
			return false, err
		}
		ids, err := toIDSlice(v)
		if err != nil {
			return false, err
		}
		for _, xid := range ids {
			if xid == id {
				return true, nil
			}
		}
		return false, nil
	}
	nodes := findNodesByPattern(doc, f.pathPattern)
	if f.expr == nil {
		return len(nodes) > 0, nil
	}
	for _, n := range nodes {
		m, err := f.expr.eval(n, q)
		if err != nil {
			return false, err
		}
		if m {
			return true, nil
		}
	}
	return false, nil
}

func (f queryFilter) candidate(col *collectionState, q *Query) ([]int64, bool) {
	cmp, ok := f.expr.(exprCmp)
	if !ok {
		return nil, false
	}
	if strings.Contains(f.pathPattern, "*") {
		return nil, false
	}
	if cmp.cmp.op != "=" && cmp.cmp.op != "in" {
		return nil, false
	}
	if cmp.cmp.key != "*" && cmp.cmp.key != "**" && strings.Contains(cmp.cmp.key, "/") {
		return nil, false
	}
	if cmp.cmp.key == "*" || cmp.cmp.key == "**" {
		return nil, false
	}
	path := pointerJoin(f.pathPattern, cmp.cmp.key)
	val, err := cmp.cmp.value.resolve(q)
	if err != nil {
		return nil, false
	}
	for _, rt := range col.runtime {
		if rt.def.Path != path {
			continue
		}
		keys := make([]string, 0, 1)
		switch cmp.cmp.op {
		case "=":
			k, ok := normalizeIndexValue(val, rt.def.Kind)
			if !ok {
				continue
			}
			keys = append(keys, k)
		case "in":
			arr, ok := val.([]any)
			if !ok {
				continue
			}
			for _, it := range arr {
				if k, ok := normalizeIndexValue(it, rt.def.Kind); ok {
					keys = append(keys, k)
				}
			}
		}
		if len(keys) == 0 {
			continue
		}
		ids := make(map[int64]struct{})
		for _, k := range keys {
			if rt.def.Unique {
				if id, ok := rt.unique[k]; ok {
					ids[id] = struct{}{}
				}
			} else {
				for id := range rt.multi[k] {
					ids[id] = struct{}{}
				}
			}
		}
		res := make([]int64, 0, len(ids))
		for id := range ids {
			res = append(res, id)
		}
		sort.Slice(res, func(i, j int) bool { return res[i] < res[j] })
		return res, true
	}
	return nil, false
}

type exprNode interface {
	eval(base any, q *Query) (bool, error)
}

type exprCmp struct {
	cmp comparison
}

func (e exprCmp) eval(base any, q *Query) (bool, error) {
	return e.cmp.eval(base, q)
}

type exprAnd struct {
	left  exprNode
	right exprNode
}

func (e exprAnd) eval(base any, q *Query) (bool, error) {
	lm, err := e.left.eval(base, q)
	if err != nil || !lm {
		return lm, err
	}
	return e.right.eval(base, q)
}

type exprOr struct {
	left  exprNode
	right exprNode
}

func (e exprOr) eval(base any, q *Query) (bool, error) {
	lm, err := e.left.eval(base, q)
	if err != nil {
		return false, err
	}
	if lm {
		return true, nil
	}
	return e.right.eval(base, q)
}

type exprNot struct {
	node exprNode
}

func (e exprNot) eval(base any, q *Query) (bool, error) {
	m, err := e.node.eval(base, q)
	if err != nil {
		return false, err
	}
	return !m, nil
}

type comparison struct {
	key   string
	op    string
	value valueExpr
}

func (c comparison) eval(base any, q *Query) (bool, error) {
	right, err := c.value.resolve(q)
	if err != nil {
		return false, err
	}
	vals := valuesByKey(base, c.key)
	for _, left := range vals {
		ok, err := compareValues(left, c.op, right)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

type projectionSpec struct {
	terms []projectionTerm
}

type projectionTerm struct {
	include bool
	all     bool
	path    pathTemplate
	join    string
}

type pathTemplate struct {
	parts []pathPart
}

type pathPart struct {
	literal string
	ph      *placeholderRef
}

func (p pathTemplate) resolve(q *Query) (string, error) {
	if len(p.parts) == 0 {
		return "/", nil
	}
	tokens := make([]string, 0, len(p.parts))
	for _, part := range p.parts {
		if part.ph == nil {
			tokens = append(tokens, part.literal)
			continue
		}
		v, err := q.resolvePlaceholder(*part.ph)
		if err != nil {
			return "", err
		}
		tokens = append(tokens, fmt.Sprint(v))
	}
	return "/" + strings.Join(tokens, "/"), nil
}

func parseQuery(q string) (parsedQuery, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		q = "/*"
	}
	parts, err := splitPipeline(q)
	if err != nil {
		return parsedQuery{}, withCodef(CodeInvalidQuery, "%v", err)
	}
	if len(parts) == 0 {
		return parsedQuery{}, withCode(CodeInvalidQuery, "empty query")
	}
	ctx := parseContext{}
	filter, coll, err := parseFilterExpression(parts[0], &ctx)
	if err != nil {
		return parsedQuery{}, err
	}
	out := parsedQuery{filter: filter, collection: coll, limit: -1}
	for _, stage := range parts[1:] {
		s := strings.TrimSpace(stage)
		if s == "" {
			continue
		}
		switch {
		case s == "del":
			out.action = actionDelete
		case strings.HasPrefix(s, "apply "):
			arg, err := parseValueExpr(strings.TrimSpace(strings.TrimPrefix(s, "apply")), &ctx)
			if err != nil {
				return parsedQuery{}, err
			}
			out.action = actionApply
			out.actionArg = arg
		case strings.HasPrefix(s, "upsert "):
			arg, err := parseValueExpr(strings.TrimSpace(strings.TrimPrefix(s, "upsert")), &ctx)
			if err != nil {
				return parsedQuery{}, err
			}
			out.action = actionUpsert
			out.actionArg = arg
		case isProjectionStage(s):
			proj, err := parseProjection(s, &ctx)
			if err != nil {
				return parsedQuery{}, err
			}
			out.projection = proj
		default:
			if strings.HasPrefix(s, "asc ") || strings.HasPrefix(s, "desc ") {
				chunks := strings.Fields(s)
				if len(chunks) != 2 {
					return parsedQuery{}, withCodef(CodeInvalidQuery, "invalid sort stage: %s", s)
				}
				pt, err := parsePathTemplate(chunks[1], &ctx)
				if err != nil {
					return parsedQuery{}, err
				}
				out.sort = &querySort{desc: chunks[0] == "desc", path: pt}
				continue
			}
			toks := strings.Fields(s)
			for i := 0; i < len(toks); i++ {
				switch toks[i] {
				case "skip":
					i++
					if i >= len(toks) {
						return parsedQuery{}, withCode(CodeInvalidQuery, "skip requires value")
					}
					n, err := strconv.Atoi(toks[i])
					if err != nil || n < 0 {
						return parsedQuery{}, withCodef(CodeInvalidQuery, "invalid skip value: %s", toks[i])
					}
					out.skip = n
				case "limit":
					i++
					if i >= len(toks) {
						return parsedQuery{}, withCode(CodeInvalidQuery, "limit requires value")
					}
					n, err := strconv.Atoi(toks[i])
					if err != nil || n < 0 {
						return parsedQuery{}, withCodef(CodeInvalidQuery, "invalid limit value: %s", toks[i])
					}
					out.limit = n
				case "count":
					out.count = true
				case "noidx":
					out.noidx = true
				case "inverse":
					out.inverse = true
				default:
					return parsedQuery{}, withCodef(CodeInvalidQuery, "unsupported stage token %q", toks[i])
				}
			}
		}
	}
	return out, nil
}

type parseContext struct {
	positionalCounter int
}

func splitPipeline(in string) ([]string, error) {
	parts := make([]string, 0, 4)
	var b strings.Builder
	quote := rune(0)
	dSquare := 0
	dCurly := 0
	dParen := 0
	for _, r := range in {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
			b.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			b.WriteRune(r)
		case r == '[':
			dSquare++
			b.WriteRune(r)
		case r == ']':
			if dSquare > 0 {
				dSquare--
			}
			b.WriteRune(r)
		case r == '{':
			dCurly++
			b.WriteRune(r)
		case r == '}':
			if dCurly > 0 {
				dCurly--
			}
			b.WriteRune(r)
		case r == '(':
			dParen++
			b.WriteRune(r)
		case r == ')':
			if dParen > 0 {
				dParen--
			}
			b.WriteRune(r)
		case r == '|' && dSquare == 0 && dCurly == 0 && dParen == 0:
			parts = append(parts, strings.TrimSpace(b.String()))
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if quote != 0 || dSquare != 0 || dCurly != 0 || dParen != 0 {
		return nil, fmt.Errorf("unbalanced query")
	}
	parts = append(parts, strings.TrimSpace(b.String()))
	return parts, nil
}

type tokenKind int

const (
	tkAtom tokenKind = iota
	tkAnd
	tkOr
	tkNot
	tkLParen
	tkRParen
)

type token struct {
	kind tokenKind
	text string
}

func tokenizeLogical(in string) ([]token, error) {
	tokens := make([]token, 0, 8)
	var b strings.Builder
	runes := []rune(strings.TrimSpace(in))
	quote := rune(0)
	dSquare := 0
	dCurly := 0
	flushAtom := func() {
		s := strings.TrimSpace(b.String())
		if s != "" {
			tokens = append(tokens, token{kind: tkAtom, text: s})
		}
		b.Reset()
	}
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
			b.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			b.WriteRune(r)
		case r == '[':
			dSquare++
			b.WriteRune(r)
		case r == ']':
			if dSquare > 0 {
				dSquare--
			}
			b.WriteRune(r)
		case r == '{':
			dCurly++
			b.WriteRune(r)
		case r == '}':
			if dCurly > 0 {
				dCurly--
			}
			b.WriteRune(r)
		case dSquare == 0 && dCurly == 0 && r == '(':
			flushAtom()
			tokens = append(tokens, token{kind: tkLParen, text: "("})
		case dSquare == 0 && dCurly == 0 && r == ')':
			flushAtom()
			tokens = append(tokens, token{kind: tkRParen, text: ")"})
		default:
			if dSquare == 0 && dCurly == 0 {
				if matched, kind, n := matchKeyword(runes[i:]); matched {
					if kind == tkNot && strings.TrimSpace(b.String()) != "" {
						b.WriteString("not")
						i += n - 1
						continue
					}
					flushAtom()
					tokens = append(tokens, token{kind: kind})
					i += n - 1
					continue
				}
			}
			b.WriteRune(r)
		}
	}
	if quote != 0 || dSquare != 0 || dCurly != 0 {
		return nil, withCode(CodeInvalidQuery, "unbalanced logical expression")
	}
	flushAtom()
	return tokens, nil
}

func matchKeyword(in []rune) (bool, tokenKind, int) {
	for _, kw := range []struct {
		w string
		k tokenKind
	}{
		{"and", tkAnd},
		{"or", tkOr},
		{"not", tkNot},
	} {
		wr := []rune(kw.w)
		if len(in) < len(wr) {
			continue
		}
		ok := true
		for i := range wr {
			if in[i] != wr[i] {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		rightOK := true
		if len(in) > len(wr) {
			right := in[len(wr)]
			rightOK = right == ' ' || right == '(' || right == ')'
		}
		if !rightOK {
			continue
		}
		return true, kw.k, len(wr)
	}
	return false, 0, 0
}

type tokenStream struct {
	toks []token
	pos  int
}

func (s *tokenStream) peek() (token, bool) {
	if s.pos >= len(s.toks) {
		return token{}, false
	}
	return s.toks[s.pos], true
}

func (s *tokenStream) next() (token, bool) {
	t, ok := s.peek()
	if ok {
		s.pos++
	}
	return t, ok
}

func parseFilterExpression(in string, ctx *parseContext) (filterNode, string, error) {
	in = strings.TrimSpace(in)
	if in == "" {
		in = "/*"
	}
	toks, err := tokenizeLogical(in)
	if err != nil {
		return nil, "", err
	}
	ts := &tokenStream{toks: toks}
	node, coll, err := parseFilterOr(ts, ctx)
	if err != nil {
		return nil, "", err
	}
	if _, ok := ts.peek(); ok {
		return nil, "", withCode(CodeInvalidQuery, "unexpected tokens in filter")
	}
	return node, coll, nil
}

func parseFilterOr(ts *tokenStream, ctx *parseContext) (filterNode, string, error) {
	left, coll, err := parseFilterAnd(ts, ctx)
	if err != nil {
		return nil, "", err
	}
	for {
		t, ok := ts.peek()
		if !ok || t.kind != tkOr {
			break
		}
		_, _ = ts.next()
		right, rcoll, err := parseFilterAnd(ts, ctx)
		if err != nil {
			return nil, "", err
		}
		if coll == "" {
			coll = rcoll
		}
		left = filterOr{left: left, right: right}
	}
	return left, coll, nil
}

func parseFilterAnd(ts *tokenStream, ctx *parseContext) (filterNode, string, error) {
	left, coll, err := parseFilterUnary(ts, ctx)
	if err != nil {
		return nil, "", err
	}
	for {
		t, ok := ts.peek()
		if !ok || t.kind != tkAnd {
			break
		}
		_, _ = ts.next()
		right, rcoll, err := parseFilterUnary(ts, ctx)
		if err != nil {
			return nil, "", err
		}
		if coll == "" {
			coll = rcoll
		}
		left = filterAnd{left: left, right: right}
	}
	return left, coll, nil
}

func parseFilterUnary(ts *tokenStream, ctx *parseContext) (filterNode, string, error) {
	t, ok := ts.peek()
	if ok && t.kind == tkNot {
		_, _ = ts.next()
		node, coll, err := parseFilterUnary(ts, ctx)
		if err != nil {
			return nil, "", err
		}
		return filterNot{node: node}, coll, nil
	}
	return parseFilterPrimary(ts, ctx)
}

func parseFilterPrimary(ts *tokenStream, ctx *parseContext) (filterNode, string, error) {
	t, ok := ts.next()
	if !ok {
		return nil, "", withCode(CodeInvalidQuery, "unexpected end of filter")
	}
	switch t.kind {
	case tkLParen:
		node, coll, err := parseFilterOr(ts, ctx)
		if err != nil {
			return nil, "", err
		}
		rp, ok := ts.next()
		if !ok || rp.kind != tkRParen {
			return nil, "", withCode(CodeInvalidQuery, "missing closing parenthesis in filter")
		}
		return node, coll, nil
	case tkAtom:
		f, coll, err := parseFilterAtom(t.text, ctx)
		if err != nil {
			return nil, "", err
		}
		return filterAtom{term: f}, coll, nil
	default:
		return nil, "", withCode(CodeInvalidQuery, "unexpected filter token")
	}
}

func parseFilterAtom(spec string, ctx *parseContext) (queryFilter, string, error) {
	spec = strings.TrimSpace(spec)
	coll := ""
	if strings.HasPrefix(spec, "@") {
		slash := strings.Index(spec, "/")
		if slash <= 1 {
			return queryFilter{}, "", withCodef(CodeInvalidQuery, "invalid collection prefix: %s", spec)
		}
		coll = spec[1:slash]
		spec = spec[slash:]
	}
	if spec == "/*" || spec == "/**" {
		return queryFilter{all: true}, coll, nil
	}
	if strings.HasPrefix(spec, "/=") {
		v, err := parseValueExpr(strings.TrimSpace(strings.TrimPrefix(spec, "/=")), ctx)
		if err != nil {
			return queryFilter{}, "", err
		}
		return queryFilter{pk: &v}, coll, nil
	}
	if strings.Contains(spec, "/[") {
		idx := strings.Index(spec, "/[")
		base := strings.TrimSpace(spec[:idx])
		if base == "" {
			base = "/"
		}
		rest := strings.TrimSpace(spec[idx+2:])
		if !strings.HasSuffix(rest, "]") {
			return queryFilter{}, "", withCodef(CodeInvalidQuery, "malformed filter expression: %s", spec)
		}
		exprText := strings.TrimSpace(rest[:len(rest)-1])
		expr, err := parseExpr(exprText, ctx)
		if err != nil {
			return queryFilter{}, "", err
		}
		return queryFilter{pathPattern: base, expr: expr}, coll, nil
	}
	if !strings.HasPrefix(spec, "/") {
		return queryFilter{}, "", withCodef(CodeInvalidQuery, "filter must start with '/': %s", spec)
	}
	return queryFilter{pathPattern: spec}, coll, nil
}

func parseExpr(in string, ctx *parseContext) (exprNode, error) {
	toks, err := tokenizeLogical(in)
	if err != nil {
		return nil, err
	}
	ts := &tokenStream{toks: toks}
	n, err := parseExprOr(ts, ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := ts.peek(); ok {
		return nil, withCode(CodeInvalidQuery, "unexpected tokens in expression")
	}
	return n, nil
}

func parseExprOr(ts *tokenStream, ctx *parseContext) (exprNode, error) {
	left, err := parseExprAnd(ts, ctx)
	if err != nil {
		return nil, err
	}
	for {
		t, ok := ts.peek()
		if !ok || t.kind != tkOr {
			break
		}
		_, _ = ts.next()
		right, err := parseExprAnd(ts, ctx)
		if err != nil {
			return nil, err
		}
		left = exprOr{left: left, right: right}
	}
	return left, nil
}

func parseExprAnd(ts *tokenStream, ctx *parseContext) (exprNode, error) {
	left, err := parseExprUnary(ts, ctx)
	if err != nil {
		return nil, err
	}
	for {
		t, ok := ts.peek()
		if !ok || t.kind != tkAnd {
			break
		}
		_, _ = ts.next()
		right, err := parseExprUnary(ts, ctx)
		if err != nil {
			return nil, err
		}
		left = exprAnd{left: left, right: right}
	}
	return left, nil
}

func parseExprUnary(ts *tokenStream, ctx *parseContext) (exprNode, error) {
	t, ok := ts.peek()
	if ok && t.kind == tkNot {
		_, _ = ts.next()
		n, err := parseExprUnary(ts, ctx)
		if err != nil {
			return nil, err
		}
		return exprNot{node: n}, nil
	}
	return parseExprPrimary(ts, ctx)
}

func parseExprPrimary(ts *tokenStream, ctx *parseContext) (exprNode, error) {
	t, ok := ts.next()
	if !ok {
		return nil, withCode(CodeInvalidQuery, "unexpected end of expression")
	}
	switch t.kind {
	case tkLParen:
		n, err := parseExprOr(ts, ctx)
		if err != nil {
			return nil, err
		}
		rp, ok := ts.next()
		if !ok || rp.kind != tkRParen {
			return nil, withCode(CodeInvalidQuery, "missing closing parenthesis")
		}
		return n, nil
	case tkAtom:
		cmp, err := parseComparison(t.text, ctx)
		if err != nil {
			return nil, err
		}
		return exprCmp{cmp: cmp}, nil
	default:
		return nil, withCode(CodeInvalidQuery, "unexpected token in expression")
	}
}

func parseComparison(s string, ctx *parseContext) (comparison, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return comparison{}, withCode(CodeInvalidQuery, "empty comparison")
	}
	for _, item := range []struct {
		op      string
		normal  string
		isWord  bool
		consume int
	}{
		{" not in ", "ni", true, 8},
		{" ni ", "ni", true, 4},
		{" in ", "in", true, 4},
		{" not re ", "nre", true, 8},
		{" re ", "re", true, 4},
		{" !~ ", "nre", true, 4},
		{" ~ ", "re", true, 3},
	} {
		if idx := findWord(s, item.op); idx >= 0 {
			left := strings.TrimSpace(s[:idx])
			right := strings.TrimSpace(s[idx+item.consume:])
			v, err := parseValueExpr(right, ctx)
			if err != nil {
				return comparison{}, err
			}
			return comparison{key: left, op: item.normal, value: v}, nil
		}
	}
	for _, op := range []string{"!eq", "gte", "lte", "gt", "lt", ">=", "<=", "!=", "=", ">", "<", "eq"} {
		if idx := strings.Index(s, op); idx >= 0 {
			left := strings.TrimSpace(s[:idx])
			right := strings.TrimSpace(s[idx+len(op):])
			if left == "" || right == "" {
				continue
			}
			v, err := parseValueExpr(right, ctx)
			if err != nil {
				return comparison{}, err
			}
			norm := op
			switch op {
			case "eq":
				norm = "="
			case "!eq":
				norm = "!="
			case "gte":
				norm = ">="
			case "lte":
				norm = "<="
			case "gt":
				norm = ">"
			case "lt":
				norm = "<"
			}
			return comparison{key: left, op: norm, value: v}, nil
		}
	}
	return comparison{}, withCodef(CodeInvalidQuery, "malformed comparison: %s", s)
}

func parseValueExpr(s string, ctx *parseContext) (valueExpr, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, ":") {
		ph, err := parsePlaceholder(s, ctx)
		if err != nil {
			return valueExpr{}, err
		}
		return valueExpr{ph: &ph}, nil
	}
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
		return valueExpr{literal: s[1 : len(s)-1]}, nil
	}
	var v any
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		return valueExpr{literal: v}, nil
	}
	if strings.Contains(s, "'") {
		if err := json.Unmarshal([]byte(strings.ReplaceAll(s, "'", `"`)), &v); err == nil {
			return valueExpr{literal: v}, nil
		}
	}
	return valueExpr{literal: s}, nil
}

func parsePlaceholder(s string, ctx *parseContext) (placeholderRef, error) {
	if s == ":?" {
		idx := ctx.positionalCounter
		ctx.positionalCounter++
		return placeholderRef{positional: true, index: idx}, nil
	}
	if !strings.HasPrefix(s, ":") || len(s) < 2 {
		return placeholderRef{}, withCodef(CodeInvalidPlaceholder, "invalid placeholder: %s", s)
	}
	name := strings.TrimPrefix(s, ":")
	if name == "" {
		return placeholderRef{}, withCodef(CodeInvalidPlaceholder, "invalid placeholder: %s", s)
	}
	return placeholderRef{positional: false, name: name}, nil
}

func isProjectionStage(stage string) bool {
	s := strings.TrimSpace(stage)
	if s == "" {
		return false
	}
	if strings.HasPrefix(s, "/") || strings.HasPrefix(s, "all") {
		return true
	}
	return false
}

func parseProjection(stage string, ctx *parseContext) (*projectionSpec, error) {
	parts := splitProjectionTerms(stage)
	spec := &projectionSpec{terms: make([]projectionTerm, 0, len(parts))}
	include := true
	for i, p := range parts {
		t := strings.TrimSpace(p.term)
		if t == "" {
			continue
		}
		if i > 0 {
			include = p.op == '+'
		}
		if t == "all" {
			spec.terms = append(spec.terms, projectionTerm{include: include, all: true})
			continue
		}
		expanded, err := expandProjectionTerm(t)
		if err != nil {
			return nil, err
		}
		for _, e := range expanded {
			join := ""
			path := e
			if lt := strings.Index(path, "<"); lt > 0 {
				join = strings.TrimSpace(path[lt+1:])
				path = strings.TrimSpace(path[:lt])
			}
			pt, err := parsePathTemplate(path, ctx)
			if err != nil {
				return nil, err
			}
			spec.terms = append(spec.terms, projectionTerm{include: include, path: pt, join: join})
		}
	}
	return spec, nil
}

type projPiece struct {
	op   rune
	term string
}

func splitProjectionTerms(in string) []projPiece {
	pieces := make([]projPiece, 0, 4)
	quote := rune(0)
	dCurly := 0
	start := 0
	op := '+'
	runes := []rune(in)
	for i, r := range runes {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == '{':
			dCurly++
		case r == '}':
			if dCurly > 0 {
				dCurly--
			}
		case (r == '+' || r == '-') && dCurly == 0:
			pieces = append(pieces, projPiece{op: op, term: strings.TrimSpace(string(runes[start:i]))})
			op = r
			start = i + 1
		}
	}
	pieces = append(pieces, projPiece{op: op, term: strings.TrimSpace(string(runes[start:]))})
	return pieces
}

func expandProjectionTerm(term string) ([]string, error) {
	term = strings.TrimSpace(term)
	if !strings.Contains(term, "{") {
		return []string{term}, nil
	}
	l := strings.Index(term, "{")
	r := strings.LastIndex(term, "}")
	if l < 0 || r < l {
		return nil, withCodef(CodeInvalidQuery, "invalid projection object term: %s", term)
	}
	prefix := strings.TrimSpace(term[:l])
	inner := term[l+1 : r]
	items := splitCSV(inner)
	res := make([]string, 0, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		path := strings.TrimSuffix(prefix, "/") + "/" + strings.TrimPrefix(it, "/")
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		res = append(res, path)
	}
	if len(res) == 0 {
		return nil, withCodef(CodeInvalidQuery, "empty projection object term: %s", term)
	}
	return res, nil
}

func splitCSV(in string) []string {
	parts := make([]string, 0, 4)
	quote := rune(0)
	dCurly := 0
	start := 0
	runes := []rune(in)
	for i, r := range runes {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == '{':
			dCurly++
		case r == '}':
			if dCurly > 0 {
				dCurly--
			}
		case r == ',' && dCurly == 0:
			parts = append(parts, strings.TrimSpace(string(runes[start:i])))
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(string(runes[start:])))
	return parts
}

func parsePathTemplate(path string, ctx *parseContext) (pathTemplate, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return pathTemplate{}, nil
	}
	if !strings.HasPrefix(path, "/") {
		return pathTemplate{}, withCodef(CodeInvalidQuery, "path must start with '/': %s", path)
	}
	tokens, err := pointerTokens(path)
	if err != nil {
		return pathTemplate{}, withCodef(CodeInvalidQuery, "%v", err)
	}
	parts := make([]pathPart, 0, len(tokens))
	for _, t := range tokens {
		if strings.HasPrefix(t, ":") {
			ph, err := parsePlaceholder(t, ctx)
			if err != nil {
				return pathTemplate{}, err
			}
			parts = append(parts, pathPart{ph: &ph})
			continue
		}
		parts = append(parts, pathPart{literal: t})
	}
	return pathTemplate{parts: parts}, nil
}

func findWord(s, sub string) int {
	depthSquare := 0
	quote := rune(0)
	runes := []rune(s)
	target := []rune(sub)
	for i := 0; i+len(target) <= len(runes); i++ {
		r := runes[i]
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
			continue
		case r == '\'' || r == '"':
			quote = r
			continue
		case r == '[':
			depthSquare++
			continue
		case r == ']':
			if depthSquare > 0 {
				depthSquare--
			}
			continue
		}
		if depthSquare != 0 {
			continue
		}
		ok := true
		for j := range target {
			if runes[i+j] != target[j] {
				ok = false
				break
			}
		}
		if ok {
			return i
		}
	}
	return -1
}

func findNodesByPattern(doc any, pattern string) []any {
	tokens, err := pointerTokens(pattern)
	if err != nil {
		return nil
	}
	if len(tokens) == 0 {
		return []any{doc}
	}
	res := make([]any, 0)
	var walk func(cur any, idx int)
	walk = func(cur any, idx int) {
		if idx >= len(tokens) {
			res = append(res, cur)
			return
		}
		tok := tokens[idx]
		switch tok {
		case "**":
			walk(cur, idx+1)
			for _, child := range immediateChildren(cur) {
				walk(child, idx)
			}
		case "*":
			for _, child := range immediateChildren(cur) {
				walk(child, idx+1)
			}
		default:
			switch v := cur.(type) {
			case map[string]any:
				if n, ok := v[tok]; ok {
					walk(n, idx+1)
				}
			case []any:
				i, err := strconv.Atoi(tok)
				if err == nil && i >= 0 && i < len(v) {
					walk(v[i], idx+1)
				}
			}
		}
	}
	walk(doc, 0)
	return res
}

func immediateChildren(v any) []any {
	switch x := v.(type) {
	case map[string]any:
		out := make([]any, 0, len(x))
		for _, it := range x {
			out = append(out, it)
		}
		return out
	case []any:
		return append([]any(nil), x...)
	default:
		return nil
	}
}

func valuesByKey(base any, key string) []any {
	key = strings.TrimSpace(key)
	switch key {
	case "*":
		switch x := base.(type) {
		case map[string]any:
			out := make([]any, 0, len(x))
			for k := range x {
				out = append(out, k)
			}
			return out
		case []any:
			out := make([]any, 0, len(x))
			for i := range x {
				out = append(out, strconv.Itoa(i))
			}
			return out
		default:
			return nil
		}
	case "**":
		out := make([]any, 0, 8)
		collectDesc(base, &out)
		return out
	}
	if strings.HasPrefix(key, "/") {
		v, ok := pointerGet(base, key)
		if !ok {
			return nil
		}
		return []any{v}
	}
	switch x := base.(type) {
	case map[string]any:
		v, ok := x[key]
		if !ok {
			return nil
		}
		return []any{v}
	case []any:
		i, err := strconv.Atoi(key)
		if err != nil || i < 0 || i >= len(x) {
			return nil
		}
		return []any{x[i]}
	default:
		return nil
	}
}

func collectDesc(v any, out *[]any) {
	*out = append(*out, v)
	for _, c := range immediateChildren(v) {
		collectDesc(c, out)
	}
}

func compareValues(left any, op string, right any) (bool, error) {
	switch op {
	case "=":
		return equalValue(left, right), nil
	case "!=":
		return !equalValue(left, right), nil
	case ">", ">=", "<", "<=":
		ord, ok := compareOrdered(left, right)
		if !ok {
			return false, nil
		}
		switch op {
		case ">":
			return ord > 0, nil
		case ">=":
			return ord >= 0, nil
		case "<":
			return ord < 0, nil
		case "<=":
			return ord <= 0, nil
		}
	case "in":
		arr, ok := right.([]any)
		if !ok {
			return false, nil
		}
		for _, it := range arr {
			if equalValue(left, it) {
				return true, nil
			}
		}
		return false, nil
	case "ni":
		ok, err := compareValues(left, "in", right)
		if err != nil {
			return false, err
		}
		return !ok, nil
	case "re", "nre":
		expr := ""
		switch rv := right.(type) {
		case regexpExpr:
			expr = string(rv)
		case string:
			expr = rv
		default:
			expr = fmt.Sprint(rv)
		}
		rx, err := regexp.Compile(expr)
		if err != nil {
			return false, withCodef(CodeInvalidQuery, "invalid regexp: %v", err)
		}
		ok := rx.MatchString(fmt.Sprint(left))
		if op == "nre" {
			return !ok, nil
		}
		return ok, nil
	}
	return false, nil
}

func equalValue(a, b any) bool {
	if an, ok := toFloat64(a); ok {
		if bn, ok := toFloat64(b); ok {
			return an == bn
		}
		if bs, ok := b.(string); ok {
			if bn, err := strconv.ParseFloat(bs, 64); err == nil {
				return an == bn
			}
		}
	}
	if as, ok := a.(string); ok {
		if bn, ok := toFloat64(b); ok {
			if an, err := strconv.ParseFloat(as, 64); err == nil {
				return an == bn
			}
		}
	}
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

func compareOrdered(a, b any) (int, bool) {
	if an, ok := toFloat64(a); ok {
		if bn, ok := toFloat64(b); ok {
			switch {
			case an < bn:
				return -1, true
			case an > bn:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		switch {
		case as < bs:
			return -1, true
		case as > bs:
			return 1, true
		default:
			return 0, true
		}
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case int16:
		return float64(x), true
	case int8:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint8:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		i := int64(x)
		return i, float64(i) == x
	case int:
		return int64(x), true
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case uint:
		return int64(x), x <= uint(^uint64(0)>>1)
	case uint64:
		if x > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(x), true
	case json.Number:
		i, err := x.Int64()
		if err == nil {
			return i, true
		}
	}
	return 0, false
}

func toIDSlice(v any) ([]int64, error) {
	switch x := v.(type) {
	case []any:
		out := make([]int64, 0, len(x))
		for _, it := range x {
			i, ok := toInt64(it)
			if !ok {
				return nil, withCodef(CodeInvalidQuery, "invalid id value: %v", it)
			}
			out = append(out, i)
		}
		return out, nil
	default:
		i, ok := toInt64(v)
		if !ok {
			return nil, withCodef(CodeInvalidQuery, "invalid id value: %v", v)
		}
		return []int64{i}, nil
	}
}
