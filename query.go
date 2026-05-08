package ejdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Query struct {
	collection     string
	text           string
	parsed         parsedQuery
	named          map[string]any
	positional     map[int]any
	pathNamed      map[string]struct{}
	pathPositional map[int]struct{}
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
	q.pathNamed, q.pathPositional = pathPlaceholderSets(pq.pathPlaceholders)
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
		if _, ok := q.pathNamed[name]; ok {
			if _, ok := val.(string); !ok {
				return withCodef(CodeInvalidPlaceholder, "path placeholder :%s must be a string, got %T", name, val)
			}
		}
		q.named[name] = val
		return nil
	}
	if index < 0 {
		return withCodef(CodeInvalidPlaceholder, "negative positional placeholder index: %d", index)
	}
	if _, ok := q.pathPositional[index]; ok {
		if _, ok := val.(string); !ok {
			return withCodef(CodeInvalidPlaceholder, "path placeholder :? at index %d must be a string, got %T", index, val)
		}
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
	collection       string
	filter           filterNode
	action           queryAction
	actionArg        valueExpr
	projection       *projectionSpec
	sorts            []querySort
	skip             int
	skipPH           *placeholderRef
	skipSet          bool
	limit            int
	limitPH          *placeholderRef
	limitSet         bool
	count            bool
	noidx            bool
	inverse          bool
	pathPlaceholders []placeholderRef
}

type querySort struct {
	desc        bool
	path        pathTemplate
	placeholder *placeholderRef
}

func (s querySort) resolve(q *Query) (string, error) {
	if s.placeholder == nil {
		return s.path.resolve(q)
	}
	v, err := q.resolvePlaceholder(*s.placeholder)
	if err != nil {
		return "", err
	}
	path, ok := v.(string)
	if !ok {
		return "", withCodef(CodeInvalidPlaceholder, "orderby placeholder must resolve to a path string, got %T", v)
	}
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "/") {
		return "", withCodef(CodeInvalidQuery, "orderby path must start with '/': %s", path)
	}
	return path, nil
}

type filterNode interface {
	match(doc any, id int64, q *Query, st *dbState) (bool, error)
	candidate(col *collectionState, q *Query) (candidatePlan, bool)
}

type candidatePlan struct {
	index      *indexRuntime
	idx        indexState
	op         string
	value      any
	op2        string
	value2     any
	weight     int
	explain    string
	explain2   string
	cursorInit string
	cursorStep string
	rnum       int
	pathCnt    int
}

func betterCandidate(a candidatePlan, aok bool, b candidatePlan, bok bool) (candidatePlan, bool) {
	switch {
	case !aok:
		return b, bok
	case !bok:
		return a, aok
	case b.weight > a.weight:
		return b, true
	case b.weight == a.weight && (b.op2 != "") != (a.op2 != ""):
		return b, b.op2 != ""
	case b.weight == a.weight && b.rnum != a.rnum:
		return b, b.rnum < a.rnum
	case b.weight == a.weight && b.pathCnt != a.pathCnt:
		return b, b.pathCnt < a.pathCnt
	case b.weight == a.weight && b.index != nil && a.index != nil && b.index.def.Unique && !a.index.def.Unique:
		return b, true
	default:
		return a, true
	}
}

type filterAtom struct {
	term queryFilter
}

func (f filterAtom) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	return f.term.match(doc, id, q, st)
}

func (f filterAtom) candidate(col *collectionState, q *Query) (candidatePlan, bool) {
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

func (f filterAnd) candidate(col *collectionState, q *Query) (candidatePlan, bool) {
	lp, lok := f.left.candidate(col, q)
	rp, rok := f.right.candidate(col, q)
	if merged, ok := mergeRangeCandidates(lp, lok, rp, rok); ok {
		return merged, true
	}
	return betterCandidate(lp, lok, rp, rok)
}

func mergeRangeCandidates(a candidatePlan, aok bool, b candidatePlan, bok bool) (candidatePlan, bool) {
	if !aok || !bok || a.index == nil || b.index == nil {
		return candidatePlan{}, false
	}
	if a.idx.Path != b.idx.Path || a.idx.Kind != b.idx.Kind || a.idx.Unique != b.idx.Unique {
		return candidatePlan{}, false
	}
	if !isLowerBoundOp(a.op) && !isUpperBoundOp(a.op) {
		return candidatePlan{}, false
	}
	if !isLowerBoundOp(b.op) && !isUpperBoundOp(b.op) {
		return candidatePlan{}, false
	}
	if (isLowerBoundOp(a.op) && isLowerBoundOp(b.op)) || (isUpperBoundOp(a.op) && isUpperBoundOp(b.op)) {
		return candidatePlan{}, false
	}
	out := a
	if isUpperBoundOp(a.op) {
		out = b
		out.op2 = a.op
		out.value2 = a.value
		out.explain2 = a.explain
	} else {
		out.op2 = b.op
		out.value2 = b.value
		out.explain2 = b.explain
	}
	out.weight = 75
	return out, true
}

func isLowerBoundOp(op string) bool {
	return op == ">" || op == ">="
}

func isUpperBoundOp(op string) bool {
	return op == "<" || op == "<="
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

func (f filterOr) candidate(col *collectionState, q *Query) (candidatePlan, bool) {
	return candidatePlan{}, false
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

func (f filterNot) candidate(col *collectionState, q *Query) (candidatePlan, bool) {
	return candidatePlan{}, false
}

type filterGroup struct {
	node filterNode
}

func (f filterGroup) match(doc any, id int64, q *Query, st *dbState) (bool, error) {
	return f.node.match(doc, id, q, st)
}

func (f filterGroup) candidate(col *collectionState, q *Query) (candidatePlan, bool) {
	return f.node.candidate(col, q)
}

type queryFilter struct {
	anchor      string
	all         bool
	allPath     string
	pk          *valueExpr
	pathPattern string
	expr        exprNode
	chain       []filterStep
}

type filterStep struct {
	kind string
	key  string
	expr exprNode
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
	if len(f.chain) > 0 {
		nodes, err := matchFilterChain([]any{doc}, f.chain, q)
		return len(nodes) > 0, err
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

func (f queryFilter) candidate(col *collectionState, q *Query) (candidatePlan, bool) {
	if len(f.chain) > 0 {
		return candidatePlan{}, false
	}
	if strings.Contains(f.pathPattern, "*") {
		return candidatePlan{}, false
	}
	return candidateFromExpr(col, q, f.pathPattern, f.expr)
}

func candidateFromExpr(col *collectionState, q *Query, pathPattern string, expr exprNode) (candidatePlan, bool) {
	switch e := expr.(type) {
	case exprCmp:
		return candidateFromComparison(col, q, pathPattern, e.cmp)
	case exprAnd:
		lp, lok := candidateFromExpr(col, q, pathPattern, e.left)
		rp, rok := candidateFromExpr(col, q, pathPattern, e.right)
		if merged, ok := mergeRangeCandidates(lp, lok, rp, rok); ok {
			return merged, true
		}
		return betterCandidate(lp, lok, rp, rok)
	default:
		return candidatePlan{}, false
	}
}

func candidateFromComparison(col *collectionState, q *Query, pathPattern string, cmp comparison) (candidatePlan, bool) {
	if !isIndexableOp(cmp.op) {
		return candidatePlan{}, false
	}
	if cmp.key != "*" && cmp.key != "**" && strings.Contains(cmp.key, "/") {
		return candidatePlan{}, false
	}
	if cmp.key == "*" {
		return candidatePlan{}, false
	}
	path := pathPattern
	if cmp.key != "**" {
		path = pointerJoin(pathPattern, cmp.key)
	}
	val, err := cmp.value.resolve(q)
	if err != nil {
		return candidatePlan{}, false
	}
	best := candidatePlan{}
	bestOK := false
	for _, rt := range col.runtime {
		if rt.def.Path != path {
			continue
		}
		weight, cursorInit, cursorStep, ok := indexOpPlan(rt.def, cmp.op, val, len(col.Docs))
		if !ok {
			continue
		}
		plan := candidatePlan{
			index:      rt,
			idx:        rt.def,
			op:         cmp.op,
			value:      val,
			weight:     weight,
			cursorInit: cursorInit,
			cursorStep: cursorStep,
			rnum:       len(col.Docs),
			pathCnt:    indexPathTokenCount(rt.def.Path),
		}
		plan.explain = fmt.Sprintf("%s %s %v", path, cmp.op, val)
		best, bestOK = betterCandidate(best, bestOK, plan, true)
	}
	return best, bestOK
}

func isIndexableOp(op string) bool {
	switch op {
	case "=", "in", ">", ">=", "<", "<=", "prefix":
		return true
	default:
		return false
	}
}

func indexOpPlan(idx indexState, op string, val any, rnum int) (weight int, cursorInit string, cursorStep string, ok bool) {
	switch op {
	case "=":
		if _, ok := normalizeIndexValue(val, idx.Kind); !ok {
			return 0, "", "", false
		}
		return 100, "IWKV_CURSOR_EQ", "IWKV_CURSOR_NEXT", true
	case "in":
		arr, ok := toAnySlice(val)
		if !ok {
			return 0, "", "", false
		}
		if len(arr) > 10 && (len(arr) > 500 || rnum < len(arr)*200) {
			return 0, "", "", false
		}
		for _, it := range arr {
			if _, ok := normalizeIndexValue(it, idx.Kind); ok {
				return 90, "IWKV_CURSOR_EQ", "IWKV_CURSOR_NEXT", true
			}
		}
		return 0, "", "", false
	case "prefix":
		if idx.Kind != IndexString {
			return 0, "", "", false
		}
		prefix, ok := jqPrefixString(toJQValue(val))
		if !ok || prefix == "" {
			return 0, "", "", false
		}
		return 60, "IWKV_CURSOR_GE", "IWKV_CURSOR_NEXT", true
	case ">", ">=", "<", "<=":
		if _, ok := normalizeIndexValue(val, idx.Kind); !ok {
			return 0, "", "", false
		}
		if op == ">" || op == ">=" {
			return 70, "IWKV_CURSOR_GE", "IWKV_CURSOR_PREV", true
		}
		return 50, "IWKV_CURSOR_GE", "IWKV_CURSOR_NEXT", true
	default:
		return 0, "", "", false
	}
}

func indexPathTokenCount(path string) int {
	toks, err := pointerTokens(path)
	if err != nil {
		return 0
	}
	return len(toks)
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

type exprGroup struct {
	node exprNode
}

func (e exprGroup) eval(base any, q *Query) (bool, error) {
	return e.node.eval(base, q)
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
			tokens = append(tokens, pointerEscapeToken(part.literal))
			continue
		}
		v, err := q.resolvePlaceholder(*part.ph)
		if err != nil {
			return "", err
		}
		s, ok := v.(string)
		if !ok {
			if part.ph.positional {
				return "", withCodef(CodeInvalidPlaceholder, "path placeholder :? at index %d must resolve to a string, got %T", part.ph.index, v)
			}
			return "", withCodef(CodeInvalidPlaceholder, "path placeholder :%s must resolve to a string, got %T", part.ph.name, v)
		}
		tokens = append(tokens, pointerEscapeToken(s))
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
			if err := parseOptionsStage(s, &out, &ctx); err != nil {
				return parsedQuery{}, err
			}
		}
	}
	out.pathPlaceholders = append([]placeholderRef(nil), ctx.pathPlaceholders...)
	return out, nil
}

func pathPlaceholderSets(refs []placeholderRef) (map[string]struct{}, map[int]struct{}) {
	named := make(map[string]struct{})
	positional := make(map[int]struct{})
	for _, ph := range refs {
		if ph.positional {
			positional[ph.index] = struct{}{}
		} else {
			named[ph.name] = struct{}{}
		}
	}
	return named, positional
}

const maxOrderByClauses = 64

func parseOptionsStage(stage string, out *parsedQuery, ctx *parseContext) error {
	toks, err := splitOptionTokens(stage)
	if err != nil {
		return err
	}
	for i := 0; i < len(toks); i++ {
		switch toks[i] {
		case "asc", "desc":
			desc := toks[i] == "desc"
			i++
			if i >= len(toks) {
				return withCodef(CodeInvalidQuery, "%s requires an order path", toks[i-1])
			}
			if strings.HasPrefix(toks[i], ":") {
				sortSpec, err := parseOrderByToken(toks[i], desc, ctx)
				if err != nil {
					return err
				}
				if len(out.sorts) >= maxOrderByClauses {
					return withCode(CodeOrderByMaxLimit, "too many orderby clauses")
				}
				out.sorts = append(out.sorts, sortSpec)
				continue
			}
			if !strings.HasPrefix(toks[i], "/") {
				return withCodef(CodeInvalidQuery, "%s requires an order path", toks[i-1])
			}
			for i < len(toks) && strings.HasPrefix(toks[i], "/") {
				sortSpec, err := parseOrderByToken(toks[i], desc, ctx)
				if err != nil {
					return err
				}
				if len(out.sorts) >= maxOrderByClauses {
					return withCode(CodeOrderByMaxLimit, "too many orderby clauses")
				}
				out.sorts = append(out.sorts, sortSpec)
				i++
			}
			i--
		case "skip":
			if out.skipSet {
				return withCode(CodeSkipAlreadySet, "skip clause already specified")
			}
			i++
			if i >= len(toks) {
				return withCode(CodeInvalidQuery, "skip requires value")
			}
			out.skipSet = true
			if strings.HasPrefix(toks[i], ":") {
				ph, err := parsePlaceholder(toks[i], ctx)
				if err != nil {
					return err
				}
				out.skipPH = &ph
				continue
			}
			n, err := strconv.Atoi(toks[i])
			if err != nil || n < 0 {
				return withCodef(CodeInvalidQuery, "invalid skip value: %s", toks[i])
			}
			out.skip = n
			out.skipPH = nil
		case "limit":
			if out.limitSet {
				return withCode(CodeLimitAlreadySet, "limit clause already specified")
			}
			i++
			if i >= len(toks) {
				return withCode(CodeInvalidQuery, "limit requires value")
			}
			out.limitSet = true
			if strings.HasPrefix(toks[i], ":") {
				ph, err := parsePlaceholder(toks[i], ctx)
				if err != nil {
					return err
				}
				out.limitPH = &ph
				continue
			}
			n, err := strconv.Atoi(toks[i])
			if err != nil || n < 0 {
				return withCodef(CodeInvalidQuery, "invalid limit value: %s", toks[i])
			}
			out.limit = n
			out.limitPH = nil
		case "count":
			out.count = true
		case "noidx":
			out.noidx = true
		case "inverse":
			out.inverse = true
		default:
			return withCodef(CodeInvalidQuery, "unsupported stage token %q", toks[i])
		}
	}
	return nil
}

func parseOrderByToken(tok string, desc bool, ctx *parseContext) (querySort, error) {
	if strings.HasPrefix(tok, ":") {
		ph, err := parsePlaceholder(tok, ctx)
		if err != nil {
			return querySort{}, err
		}
		ctx.pathPlaceholders = append(ctx.pathPlaceholders, ph)
		return querySort{desc: desc, placeholder: &ph}, nil
	}
	pt, err := parsePathTemplate(tok, ctx)
	if err != nil {
		return querySort{}, err
	}
	return querySort{desc: desc, path: pt}, nil
}

func splitOptionTokens(in string) ([]string, error) {
	tokens := make([]string, 0, 8)
	var b strings.Builder
	quote := rune(0)
	escaped := false
	flush := func() {
		if s := strings.TrimSpace(b.String()); s != "" {
			tokens = append(tokens, s)
		}
		b.Reset()
	}
	for _, r := range in {
		switch {
		case quote != 0:
			b.WriteRune(r)
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
			b.WriteRune(r)
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			flush()
		default:
			b.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, withCode(CodeInvalidQuery, "unbalanced option string")
	}
	flush()
	return tokens, nil
}

type parseContext struct {
	positionalCounter int
	pathPlaceholders  []placeholderRef
}

func splitPipeline(in string) ([]string, error) {
	parts := make([]string, 0, 4)
	var b strings.Builder
	quote := rune(0)
	escaped := false
	dSquare := 0
	dCurly := 0
	dParen := 0
	for _, r := range in {
		switch {
		case quote != 0:
			b.WriteRune(r)
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
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
	escaped := false
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
			b.WriteRune(r)
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
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
		return filterGroup{node: node}, coll, nil
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
		return queryFilter{anchor: coll, all: true, allPath: spec}, coll, nil
	}
	if strings.HasPrefix(spec, "/=") {
		v, err := parseValueExpr(strings.TrimSpace(strings.TrimPrefix(spec, "/=")), ctx)
		if err != nil {
			return queryFilter{}, "", err
		}
		return queryFilter{anchor: coll, pk: &v}, coll, nil
	}
	chainCtx := *ctx
	if chain, ok, err := parseFilterChain(spec, &chainCtx); err != nil {
		return queryFilter{}, "", err
	} else if ok && isComplexFilterChain(chain) {
		*ctx = chainCtx
		return queryFilter{anchor: coll, chain: chain}, coll, nil
	}
	if strings.Contains(spec, "/[") {
		idx := strings.Index(spec, "/[")
		base := strings.TrimSpace(spec[:idx])
		if base == "" {
			base = "/"
		}
		base, err := normalizeJQLPath(base, ctx)
		if err != nil {
			return queryFilter{}, "", err
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
		return queryFilter{anchor: coll, pathPattern: base, expr: expr}, coll, nil
	}
	if !strings.HasPrefix(spec, "/") {
		return queryFilter{}, "", withCodef(CodeInvalidQuery, "filter must start with '/': %s", spec)
	}
	path, err := normalizeJQLPath(spec, ctx)
	if err != nil {
		return queryFilter{}, "", err
	}
	return queryFilter{anchor: coll, pathPattern: path}, coll, nil
}

func isComplexFilterChain(chain []filterStep) bool {
	exprCount := 0
	for i, st := range chain {
		if st.kind == "expr" {
			exprCount++
			if i != len(chain)-1 || exprCount > 1 {
				return true
			}
		}
	}
	return false
}

func parseFilterChain(spec string, ctx *parseContext) ([]filterStep, bool, error) {
	if !strings.HasPrefix(spec, "/") {
		return nil, false, nil
	}
	steps := make([]filterStep, 0, 4)
	for i := 1; i < len(spec); {
		if spec[i] == '/' {
			i++
			continue
		}
		if spec[i] == '[' {
			end, err := findBalancedEnd(spec, i, '[', ']')
			if err != nil {
				return nil, false, err
			}
			expr, err := parseExpr(strings.TrimSpace(spec[i+1:end]), ctx)
			if err != nil {
				return nil, false, err
			}
			steps = append(steps, filterStep{kind: "expr", expr: expr})
			i = end + 1
			continue
		}
		start := i
		quote := byte(0)
		escaped := false
		for i < len(spec) {
			c := spec[i]
			if quote != 0 {
				if escaped {
					escaped = false
				} else if c == '\\' {
					escaped = true
				} else if c == quote {
					quote = 0
				}
				i++
				continue
			}
			if c == '\'' || c == '"' {
				quote = c
				i++
				continue
			}
			if c == '\\' {
				i += 2
				continue
			}
			if c == '/' {
				break
			}
			i++
		}
		raw := spec[start:i]
		key, err := normalizeJQLPathToken(raw)
		if err != nil {
			return nil, false, err
		}
		switch key {
		case "*":
			steps = append(steps, filterStep{kind: "any"})
		case "**":
			steps = append(steps, filterStep{kind: "desc"})
		default:
			steps = append(steps, filterStep{kind: "field", key: key})
		}
	}
	return steps, true, nil
}

func findBalancedEnd(s string, start int, open, close byte) (int, error) {
	depth := 0
	quote := byte(0)
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if c == open {
			depth++
			continue
		}
		if c == close {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, withCode(CodeInvalidQuery, "unbalanced filter node expression")
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
		return exprGroup{node: n}, nil
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
	if idx, end, norm, ok := findBangNegatedOperator(s); ok {
		left := strings.TrimSpace(s[:idx])
		right := strings.TrimSpace(s[end:])
		if left != "" && right != "" {
			v, err := parseValueExpr(right, ctx)
			if err != nil {
				return comparison{}, err
			}
			return comparison{key: left, op: norm, value: v}, nil
		}
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
		{" !~ ", "nprefix", true, 4},
		{" ~ ", "prefix", true, 3},
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
		if idx := findTopLevelOperator(s, op); idx >= 0 {
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

func findBangNegatedOperator(s string) (idx int, end int, norm string, ok bool) {
	runes := []rune(s)
	quote := rune(0)
	depthSquare := 0
	depthCurly := 0
	for i, r := range runes {
		switch {
		case quote != 0:
			if r == '\\' {
				i++
				continue
			}
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
		case r == '{':
			depthCurly++
			continue
		case r == '}':
			if depthCurly > 0 {
				depthCurly--
			}
			continue
		}
		if depthSquare != 0 || depthCurly != 0 || r != '!' {
			continue
		}
		j := i + 1
		for j < len(runes) && (runes[j] == ' ' || runes[j] == '\t' || runes[j] == '\n' || runes[j] == '\r') {
			j++
		}
		switch {
		case j < len(runes) && runes[j] == '=':
			return len(string(runes[:i])), len(string(runes[:j+1])), "!=", true
		case j < len(runes) && runes[j] == '~':
			return len(string(runes[:i])), len(string(runes[:j+1])), "nprefix", true
		case j+2 <= len(runes) && string(runes[j:j+2]) == "eq" && wordOpBoundary(runes, j, j+2):
			return len(string(runes[:i])), len(string(runes[:j+2])), "!=", true
		}
	}
	return 0, 0, "", false
}

func findTopLevelOperator(s, op string) int {
	runes := []rune(s)
	target := []rune(op)
	quote := rune(0)
	depthSquare := 0
	depthCurly := 0
	for i := 0; i+len(target) <= len(runes); i++ {
		r := runes[i]
		switch {
		case quote != 0:
			if r == '\\' {
				i++
				continue
			}
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
		case r == '{':
			depthCurly++
			continue
		case r == '}':
			if depthCurly > 0 {
				depthCurly--
			}
			continue
		}
		if depthSquare != 0 || depthCurly != 0 {
			continue
		}
		matched := true
		for j := range target {
			if runes[i+j] != target[j] {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if isAlphaOp(op) && !wordOpBoundary(runes, i, i+len(target)) {
			continue
		}
		return len(string(runes[:i]))
	}
	return -1
}

func isAlphaOp(op string) bool {
	for _, r := range op {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && r != '!' {
			return false
		}
	}
	return true
}

func wordOpBoundary(runes []rune, start, end int) bool {
	if start > 0 && isIdentRune(runes[start-1]) {
		return false
	}
	return end >= len(runes) || !isIdentRune(runes[end])
}

func isIdentRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
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
		return valueExpr{literal: unquoteSingleJQL(s)}, nil
	}
	var v any
	if err := decodeJSONValue([]byte(s), &v); err == nil {
		return valueExpr{literal: v}, nil
	}
	if strings.Contains(s, "'") {
		if err := decodeJSONValue([]byte(strings.ReplaceAll(s, "'", `"`)), &v); err == nil {
			return valueExpr{literal: v}, nil
		}
	}
	return valueExpr{literal: s}, nil
}

func decodeJSONValue(raw []byte, v *any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	return dec.Decode(v)
}

func unquoteSingleJQL(s string) string {
	if len(s) < 2 {
		return s
	}
	body := strings.ReplaceAll(s[1:len(s)-1], `\'`, `'`)
	body = strings.ReplaceAll(body, `\\`, `\`)
	return body
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
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return placeholderRef{}, withCodef(CodeInvalidPlaceholder, "invalid placeholder: %s", s)
		}
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
	tokens, err := parseJQLPathTokens(path)
	if err != nil {
		return pathTemplate{}, withCodef(CodeInvalidQuery, "%v", err)
	}
	parts := make([]pathPart, 0, len(tokens))
	for _, t := range tokens {
		t, err = normalizeJQLPathToken(t)
		if err != nil {
			return pathTemplate{}, err
		}
		if strings.HasPrefix(t, ":") {
			ph, err := parsePlaceholder(t, ctx)
			if err != nil {
				return pathTemplate{}, err
			}
			ctx.pathPlaceholders = append(ctx.pathPlaceholders, ph)
			parts = append(parts, pathPart{ph: &ph})
			continue
		}
		parts = append(parts, pathPart{literal: t})
	}
	return pathTemplate{parts: parts}, nil
}

func normalizeJQLPathToken(tok string) (string, error) {
	tok = strings.TrimSpace(tok)
	if len(tok) >= 2 && ((tok[0] == '"' && tok[len(tok)-1] == '"') || (tok[0] == '\'' && tok[len(tok)-1] == '\'')) {
		if tok[0] == '\'' {
			return unquoteSingleJQL(tok), nil
		}
		u, err := strconv.Unquote(tok)
		if err != nil {
			return "", withCodef(CodeInvalidQuery, "invalid quoted path token %s: %v", tok, err)
		}
		return u, nil
	}
	return tok, nil
}

func normalizeJQLPath(path string, ctx *parseContext) (string, error) {
	pt, err := parsePathTemplate(path, ctx)
	if err != nil {
		return "", err
	}
	for _, part := range pt.parts {
		if part.ph != nil {
			return "", withCode(CodeInvalidPlaceholder, "filter path placeholders require query execution context")
		}
	}
	return pt.resolve(&Query{named: map[string]any{}, positional: map[int]any{}})
}

func parseJQLPathTokens(path string) ([]string, error) {
	if path == "" || path == "/" {
		return nil, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("path must start with '/': %s", path)
	}
	tokens := make([]string, 0, 4)
	var b strings.Builder
	escaped := false
	quote := rune(0)
	runes := []rune(path[1:])
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if escaped {
			if quote == '"' {
				b.WriteRune('\\')
				b.WriteRune(r)
			} else {
				switch r {
				case '\\', '/', '{', '}', ',', ' ', '\t', '\n', '\r', '"', '\'':
					b.WriteRune(r)
				case 'b':
					b.WriteByte('\b')
				case 'f':
					b.WriteByte('\f')
				case 'n':
					b.WriteByte('\n')
				case 'r':
					b.WriteByte('\r')
				case 't':
					b.WriteByte('\t')
				case 'u':
					if i+4 >= len(runes) {
						return nil, fmt.Errorf("short unicode path escape")
					}
					hex := string(runes[i+1 : i+5])
					u, err := strconv.ParseInt(hex, 16, 32)
					if err != nil {
						return nil, fmt.Errorf("invalid unicode path escape: %s", hex)
					}
					b.WriteRune(rune(u))
					i += 4
				default:
					b.WriteRune(r)
				}
			}
			escaped = false
			continue
		}
		if quote != 0 {
			if r == '\\' {
				escaped = true
				continue
			}
			b.WriteRune(r)
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '\'', '"':
			quote = r
			b.WriteRune(r)
		case '/':
			tokens = append(tokens, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	if escaped {
		return nil, fmt.Errorf("unterminated path escape")
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quoted path token")
	}
	tokens = append(tokens, b.String())
	return tokens, nil
}

func decodePathEscape(runes []rune, i *int, r rune) (rune, bool, error) {
	switch r {
	case '\\', '/', '{', '}', ',', ' ', '\t', '\n', '\r', '"', '\'':
		return r, false, nil
	case 'b':
		return '\b', false, nil
	case 'f':
		return '\f', false, nil
	case 'n':
		return '\n', false, nil
	case 'r':
		return '\r', false, nil
	case 't':
		return '\t', false, nil
	case 'u':
		if *i+4 >= len(runes) {
			return 0, false, fmt.Errorf("short unicode path escape")
		}
		hex := string(runes[*i+1 : *i+5])
		u, err := strconv.ParseInt(hex, 16, 32)
		if err != nil {
			return 0, false, fmt.Errorf("invalid unicode path escape: %s", hex)
		}
		*i += 4
		return rune(u), false, nil
	default:
		return r, false, nil
	}
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
	if strings.HasPrefix(key, "[") && strings.HasSuffix(key, "]") {
		return valuesByNestedExpr(base, strings.TrimSpace(key[1:len(key)-1]))
	}
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

func valuesByNestedExpr(base any, exprText string) []any {
	cmp, err := parseComparison(exprText, &parseContext{})
	if err != nil {
		return nil
	}
	right, err := cmp.value.resolve(&Query{named: map[string]any{}, positional: map[int]any{}})
	if err != nil {
		return nil
	}
	out := make([]any, 0)
	addIfMatched := func(selector any, value any) {
		ok, err := compareValues(selector, cmp.op, right)
		if err == nil && ok {
			out = append(out, value)
		}
	}
	switch x := base.(type) {
	case map[string]any:
		switch cmp.key {
		case "*":
			for k, v := range x {
				addIfMatched(k, v)
			}
		case "**":
			for _, v := range x {
				addIfMatched(v, v)
			}
		default:
			if v, ok := x[cmp.key]; ok {
				addIfMatched(v, v)
			}
		}
	case []any:
		switch cmp.key {
		case "*":
			for i, v := range x {
				addIfMatched(strconv.Itoa(i), v)
			}
		case "**":
			for _, v := range x {
				addIfMatched(v, v)
			}
		default:
			if i, err := strconv.Atoi(cmp.key); err == nil && i >= 0 && i < len(x) {
				addIfMatched(x[i], x[i])
			}
		}
	}
	return out
}

func matchFilterChain(nodes []any, chain []filterStep, q *Query) ([]any, error) {
	cur := nodes
	for _, step := range chain {
		next := make([]any, 0)
		for _, node := range cur {
			switch step.kind {
			case "field":
				switch x := node.(type) {
				case map[string]any:
					if v, ok := x[step.key]; ok {
						next = append(next, v)
					}
				case []any:
					i, err := strconv.Atoi(step.key)
					if err == nil && i >= 0 && i < len(x) {
						next = append(next, x[i])
					}
				}
			case "any":
				next = append(next, immediateChildren(node)...)
			case "desc":
				collectDesc(node, &next)
			case "expr":
				selected, err := selectByExprNode(node, step.expr, q)
				if err != nil {
					return nil, err
				}
				next = append(next, selected...)
			}
		}
		cur = next
		if len(cur) == 0 {
			return nil, nil
		}
	}
	return cur, nil
}

func selectByExprNode(base any, expr exprNode, q *Query) ([]any, error) {
	if cmp, ok := expr.(exprCmp); ok {
		return selectByComparison(base, cmp.cmp, q)
	}
	ok, err := expr.eval(base, q)
	if err != nil || !ok {
		return nil, err
	}
	return []any{base}, nil
}

func selectByComparison(base any, cmp comparison, q *Query) ([]any, error) {
	right, err := cmp.value.resolve(q)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0)
	addIfMatched := func(selector, value any) error {
		ok, err := compareValues(selector, cmp.op, right)
		if err != nil || !ok {
			return err
		}
		out = append(out, value)
		return nil
	}
	switch x := base.(type) {
	case map[string]any:
		switch cmp.key {
		case "*":
			for k, v := range x {
				if err := addIfMatched(k, v); err != nil {
					return nil, err
				}
			}
			return out, nil
		case "**":
			for _, v := range x {
				if err := addIfMatched(v, v); err != nil {
					return nil, err
				}
			}
			return out, nil
		}
	case []any:
		switch cmp.key {
		case "*":
			for i, v := range x {
				if err := addIfMatched(strconv.Itoa(i), v); err != nil {
					return nil, err
				}
			}
			return out, nil
		case "**":
			for _, v := range x {
				if err := addIfMatched(v, v); err != nil {
					return nil, err
				}
			}
			return out, nil
		}
	}
	for _, left := range valuesByKey(base, cmp.key) {
		if err := addIfMatched(left, base); err != nil {
			return nil, err
		}
	}
	return out, nil
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
		cmp, ok := jqCompare(left, right)
		return ok && cmp == 0, nil
	case "!=":
		cmp, ok := jqCompare(left, right)
		return !ok || cmp != 0, nil
	case ">", ">=", "<", "<=":
		if ls, lok := left.(string); lok {
			if rs, rok := right.(string); rok {
				ord := strings.Compare(ls, rs)
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
			}
		}
		ord, ok := jqCompare(left, right)
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
		arr, ok := toAnySlice(right)
		if !ok {
			return false, nil
		}
		for _, it := range arr {
			cmp, ok := jqCompare(left, it)
			if ok && cmp == 0 {
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
	case "prefix", "nprefix":
		ok, supported := jqPrefixMatch(left, right)
		if !supported {
			return false, nil
		}
		if op == "nprefix" {
			return !ok, nil
		}
		return ok, nil
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

func toAnySlice(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case []string:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = v
		}
		return out, true
	case []int:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = v
		}
		return out, true
	case []int64:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = v
		}
		return out, true
	case []float64:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = v
		}
		return out, true
	case []json.Number:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = v
		}
		return out, true
	default:
		return nil, false
	}
}
