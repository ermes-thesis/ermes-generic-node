package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Query struct {
	Select string            `json:"SELECT"`
	As     string            `json:"AS"`
	Where  *ConditionWrapper `json:"WHERE,omitempty"`
}

var allowedSelectValues = map[string]struct{}{
	"ALL":    {},
	"UNIQUE": {},
	"MAX":    {},
	"MIN":    {},
}

func (q *Query) Validate() error {
	if strings.TrimSpace(q.Select) == "" {
		return errors.New("SELECT field is required")
	}

	_, allowed := allowedSelectValues[q.Select]
	if !allowed {
		return errors.New("invalid select value: " + q.Select)
	}

	if strings.TrimSpace(q.As) == "" {
		return errors.New("AS field is required")
	}

	if q.Where != nil {
		if err := q.Where.Condition.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (q *Query) ToRedisSearchCommand() (string, error) {
	if err := q.Validate(); err != nil {
		return "", err
	}

	query := "*"
	if q.Where != nil && q.Where.Condition != nil {
		rq, err := q.Where.Condition.ToRedisQuery()
		if err != nil {
			return "", err
		}
		query = rq
	}

	return query, nil
}

func (q *Query) ReplacePlaceholder(vars map[string]any) error {
	if q.Where != nil && q.Where.Condition != nil {
		return q.Where.Condition.ReplacePlaceholder(vars)
	}
	return nil
}

type Condition interface {
	Validate() error
	ToRedisQuery() (string, error)
	ReplacePlaceholder(vars map[string]any) error
}

type AndCondition struct {
	Conditions []Condition `json:"AND"`
}

func (a *AndCondition) Validate() error {
	if len(a.Conditions) == 0 {
		return errors.New("AND condition requires at least one sub-condition")
	}
	for _, c := range a.Conditions {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (a *AndCondition) ToRedisQuery() (string, error) {
	parts := make([]string, 0, len(a.Conditions))
	for _, c := range a.Conditions {
		part, err := c.ToRedisQuery()
		if err != nil {
			return "", err
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, " "), nil
}

func (a *AndCondition) ReplacePlaceholder(vars map[string]any) error {
	for _, c := range a.Conditions {
		if err := c.ReplacePlaceholder(vars); err != nil {
			return err
		}
	}
	return nil
}

func (a *AndCondition) MarshalJSON() ([]byte, error) {
	return Marshal(map[string]any{"AND": a.Conditions})
}

type OrCondition struct {
	Conditions []Condition `json:"OR"`
}

func (o *OrCondition) Validate() error {
	if len(o.Conditions) == 0 {
		return errors.New("OR condition requires at least one sub-condition")
	}
	for _, c := range o.Conditions {
		if err := c.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (o *OrCondition) ToRedisQuery() (string, error) {
	parts := make([]string, 0, len(o.Conditions))
	for _, c := range o.Conditions {
		part, err := c.ToRedisQuery()
		if err != nil {
			return "", err
		}
		parts = append(parts, part)
	}
	return fmt.Sprintf("(|%s)", strings.Join(parts, " ")), nil
}

func (o *OrCondition) ReplacePlaceholder(vars map[string]any) error {
	for _, c := range o.Conditions {
		if err := c.ReplacePlaceholder(vars); err != nil {
			return err
		}
	}
	return nil
}

func (o *OrCondition) MarshalJSON() ([]byte, error) {
	return Marshal(map[string]any{"OR": o.Conditions})
}

type NotCondition struct {
	Condition Condition `json:"NOT"`
}

func (n *NotCondition) Validate() error {
	if n.Condition == nil {
		return errors.New("NOT condition requires a sub-condition")
	}
	return n.Condition.Validate()
}

func (n *NotCondition) ToRedisQuery() (string, error) {
	part, err := n.Condition.ToRedisQuery()
	if err != nil {
		return "", err
	}
	return "-" + part, nil
}

func (n *NotCondition) ReplacePlaceholder(vars map[string]any) error {
	if n.Condition != nil {
		return n.Condition.ReplacePlaceholder(vars)
	}
	return nil
}

func (n *NotCondition) MarshalJSON() ([]byte, error) {
	return Marshal(map[string]any{"NOT": n.Condition})
}

type EqCondition struct {
	Field string `json:"field"`
	Value any    `json:"eq"`
}

func (e *EqCondition) Validate() error {
	if e.Field == "" {
		return errors.New("eq condition requires a field")
	}
	return nil
}

func (e *EqCondition) ToRedisQuery() (string, error) {
	switch v := e.Value.(type) {
	case string:
		return fmt.Sprintf(`@%s:"%s"`, e.Field, escapeString(v)), nil
	case float64, int:
		return fmt.Sprintf(`@%s:[%v %v]`, e.Field, v, v), nil
	default:
		return "", fmt.Errorf("unsupported value type for eq: %T", v)
	}
}

func (e *EqCondition) ReplacePlaceholder(vars map[string]any) error {
	if str, ok := e.Value.(string); ok {
		val, err := replacePlaceholderOrError(str, vars)
		if err != nil {
			return fmt.Errorf("field %q: %w", e.Field, err)
		}
		e.Value = val
	}
	return nil
}

type GtCondition struct {
	Field string `json:"field"`
	Value any    `json:"gt"`
}

func (g *GtCondition) Validate() error {
	if g.Field == "" {
		return errors.New("gt condition requires a field")
	}
	return nil
}
func (g *GtCondition) ToRedisQuery() (string, error) {
	switch v := g.Value.(type) {
	case float64, int:
		return fmt.Sprintf(`@%s:[(%v +inf]`, g.Field, v), nil
	default:
		return "", fmt.Errorf("unsupported value type for gt: %T", v)
	}
}
func (g *GtCondition) ReplacePlaceholder(vars map[string]any) error {
	if str, ok := g.Value.(string); ok {
		val, err := replacePlaceholderOrError(str, vars)
		if err != nil {
			return fmt.Errorf("field %q: %w", g.Field, err)
		}
		g.Value = val
	}
	return nil
}

type rawCondition struct {
	And *[]json.RawMessage `json:"AND"`
	Or  *[]json.RawMessage `json:"OR"`
	Not *json.RawMessage   `json:"NOT"`

	Field *string          `json:"field"`
	Eq    *json.RawMessage `json:"eq"`
	Gt    *json.RawMessage `json:"gt"`
}

func UnmarshalCondition(data []byte) (Condition, error) {
	var rc rawCondition
	if err := json.Unmarshal(data, &rc); err != nil {
		return nil, err
	}

	switch {
	case rc.And != nil:
		conds := make([]Condition, len(*rc.And))
		for i, raw := range *rc.And {
			c, err := UnmarshalCondition(raw)
			if err != nil {
				return nil, err
			}
			conds[i] = c
		}
		return &AndCondition{Conditions: conds}, nil

	case rc.Or != nil:
		conds := make([]Condition, len(*rc.Or))
		for i, raw := range *rc.Or {
			c, err := UnmarshalCondition(raw)
			if err != nil {
				return nil, err
			}
			conds[i] = c
		}
		return &OrCondition{Conditions: conds}, nil

	case rc.Not != nil:
		c, err := UnmarshalCondition(*rc.Not)
		if err != nil {
			return nil, err
		}
		return &NotCondition{Condition: c}, nil

	case rc.Eq != nil:
		if rc.Field == nil {
			return nil, errors.New("eq operator requires field")
		}
		var val any
		if err := json.Unmarshal(*rc.Eq, &val); err != nil {
			return nil, err
		}
		return &EqCondition{
			Field: *rc.Field,
			Value: val,
		}, nil

	case rc.Gt != nil:
		if rc.Field == nil {
			return nil, errors.New("gt operator requires field")
		}
		var val any
		if err := json.Unmarshal(*rc.Gt, &val); err != nil {
			return nil, err
		}
		return &GtCondition{
			Field: *rc.Field,
			Value: val,
		}, nil

	default:
		return nil, errors.New("unknown condition type or empty condition")
	}
}

type ConditionWrapper struct {
	Condition Condition
}

func (cw *ConditionWrapper) UnmarshalJSON(data []byte) error {
	cond, err := UnmarshalCondition(data)
	if err != nil {
		return err
	}
	cw.Condition = cond
	return nil
}

func (cw *ConditionWrapper) MarshalJSON() ([]byte, error) {
	if cw.Condition == nil {
		return []byte("null"), nil
	}

	switch cond := cw.Condition.(type) {
	case *AndCondition:
		return Marshal(map[string]any{"AND": cond.Conditions})
	case *OrCondition:
		return Marshal(map[string]any{"OR": cond.Conditions})
	case *NotCondition:
		return Marshal(map[string]any{"NOT": cond.Condition})
	case *EqCondition:
		return Marshal(map[string]any{
			"field": cond.Field,
			"eq":    cond.Value,
		})
	case *GtCondition:
		return Marshal(map[string]any{
			"field": cond.Field,
			"gt":    cond.Value,
		})
	default:
		return nil, errors.New("unsupported condition type")
	}
}

func replacePlaceholderOrError(s string, vars map[string]any) (any, error) {
	if strings.HasPrefix(s, "<") && strings.HasSuffix(s, ">") {
		key := s[1 : len(s)-1]
		if val, ok := vars[key]; ok {
			return val, nil
		}
		return "", fmt.Errorf("unresolved placeholder: <%s>", key)
	}
	return s, nil
}
