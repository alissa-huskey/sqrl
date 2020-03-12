package sqrl

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
)

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder struct {
	StatementBuilderType

	returning

	prefixes exprs
	options  []string
	into     string
	columns  []string
	rows     [][]interface{}
	suffixes exprs
	iselect  *SelectBuilder
}

// NewInsertBuilder creates new instance of InsertBuilder
func NewInsertBuilder(b StatementBuilderType) *InsertBuilder {
	return &InsertBuilder{StatementBuilderType: b}
}

// RunWith sets a Runner (like database/sql.DB) to be used with e.g. Exec.
func (b *InsertBuilder) RunWith(runner BaseRunner) *InsertBuilder {
	b.runWith = wrapRunner(runner)
	return b
}

// Exec builds and Execs the query with the Runner set by RunWith.
func (b *InsertBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

// ExecContext builds and Execs the query with the Runner set by RunWith using given context.
func (b *InsertBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return ExecWithContext(ctx, b.runWith, b)
}

// Query builds and Querys the query with the Runner set by RunWith.
func (b *InsertBuilder) Query() (*sql.Rows, error) {
	return b.QueryContext(context.Background())
}

// QueryContext builds and runs the query using given context and Query command.
func (b *InsertBuilder) QueryContext(ctx context.Context) (*sql.Rows, error) {
	if b.runWith == nil {
		return nil, ErrRunnerNotSet
	}
	return QueryWithContext(ctx, b.runWith, b)
}

// QueryRow builds and QueryRows the query with the Runner set by RunWith.
func (b *InsertBuilder) QueryRow() RowScanner {
	return b.QueryRowContext(context.Background())
}

// QueryRowContext builds and runs the query using given context.
func (b *InsertBuilder) QueryRowContext(ctx context.Context) RowScanner {
	if b.runWith == nil {
		return &Row{err: ErrRunnerNotSet}
	}
	queryRower, ok := b.runWith.(QueryRowerContext)
	if !ok {
		return &Row{err: ErrRunnerNotQueryRunnerContext}
	}
	return QueryRowWithContext(ctx, queryRower, b)
}

// Scan is a shortcut for QueryRow().Scan.
func (b *InsertBuilder) Scan(dest ...interface{}) error {
	return b.QueryRow().Scan(dest...)
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b *InsertBuilder) PlaceholderFormat(f PlaceholderFormat) *InsertBuilder {
	b.placeholderFormat = f
	return b
}

// emptyok allows empty b.values
func (b *InsertBuilder) AllowEmptyVals() *InsertBuilder {
	(*b).emptyok = true
	return b
}

// ToSql builds the query into a SQL string and bound args.
func (b *InsertBuilder) ToSql() (sqlStr string, args []interface{}, err error) {
	if len(b.into) == 0 {
		err = fmt.Errorf("insert statements must specify a table")
		return
	}

	if b.failEmpty() && b.iselect == nil {
		err = fmt.Errorf("insert statements must have at least one set of values or select clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, _ = b.prefixes.AppendToSql(sql, " ", args)
		sql.WriteString(" ")
	}

	sql.WriteString("INSERT")

	if len(b.options) > 0 {
		sql.WriteString(" ")
		sql.WriteString(strings.Join(b.options, " "))
	}

	sql.WriteString(" INTO ")
	sql.WriteString(b.into)

	if len(b.columns) > 0 {
		sql.WriteString(" (")
		sql.WriteString(strings.Join(b.columns, ","))
		sql.WriteString(")")
	}

	if b.iselect != nil {
		args, err = b.appendSelectToSQL(sql, args)
	} else {
		args, err = b.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return
	}

	if len(b.returning) > 0 {
		args, err = b.returning.AppendToSql(sql, args)
		if err != nil {
			return
		}
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, _ = b.suffixes.AppendToSql(sql, " ", args)
	}

	sqlStr, err = b.placeholderFormat.ReplacePlaceholders(sql.String(), b.columns)
	return
}

func (b *InsertBuilder) failEmpty() bool {
	return !b.emptyok && len(b.rows) == 0
}

func (b *InsertBuilder) appendValuesToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if b.failEmpty() {
		return args, errors.New("values for insert statements are not set")
	}

	rCount := len(b.rows)
	if rCount == 0 && len(b.columns) > 0 {
		rCount = 1
	}
	if rCount == 0 {
		return args, nil
	}

	cCount := len(b.columns)
	if len(b.rows) > 0 && len(b.rows[0]) > cCount {
		cCount = len(b.rows[0])
	}

	io.WriteString(w, " VALUES ")

	rowsStrings := make([]string, rCount)
	for r := 0; r < rCount; r++ {
		valuesStrings := make([]string, cCount)

		for v := 0; v < cCount; v++ {
			if r >= len(b.rows) {
				b.fillPlaceholders(0, &valuesStrings)
				break
			}

			row := b.rows[r]
			if v >= len(row) {
				b.fillPlaceholders(v, &valuesStrings)
				break
			}

			sql, err := b.buildValueString(row[v], &args)
			if err != nil {
				return args, err
			}
			valuesStrings[v] = sql

		}
		rowsStrings[r] = fmt.Sprintf("(%s)", strings.Join(valuesStrings, ","))
	}

	io.WriteString(w, strings.Join(rowsStrings, ","))

	return args, nil
}

func (b *InsertBuilder) fillPlaceholders(start int, arr *[]string) {
	for i := start; i < len((*arr)); i++ {
		(*arr)[i] = "?"
	}
}

func (b *InsertBuilder) buildValueString(val interface{}, args *[]interface{}) (sql string, err error) {
	var a []interface{}

	switch typedVal := val.(type) {

	case expr:
		sql = typedVal.sql
		a = typedVal.args

	case Sqlizer:
		sql, a, err = typedVal.ToSql()
		if err != nil {
			return
		}

	default:
		sql = "?"
		a = []interface{}{val}
	}

	(*args) = append((*args), a...)
	return
}

func (b *InsertBuilder) appendSelectToSQL(w io.Writer, args []interface{}) ([]interface{}, error) {
	if b.iselect == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := b.iselect.ToSql()
	if err != nil {
		return args, err
	}

	io.WriteString(w, " ")
	io.WriteString(w, selectClause)
	args = append(args, sArgs...)

	return args, nil
}

// Prefix adds an expression to the beginning of the query
func (b *InsertBuilder) Prefix(sql string, args ...interface{}) *InsertBuilder {
	b.prefixes = append(b.prefixes, Expr(sql, args...))
	return b
}

// Options adds keyword options before the INTO clause of the query.
func (b *InsertBuilder) Options(options ...string) *InsertBuilder {
	b.options = append(b.options, options...)
	return b
}

// Into sets the INTO clause of the query.
func (b *InsertBuilder) Into(into string) *InsertBuilder {
	b.into = into
	return b
}

// Columns adds insert columns to the query.
func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Values adds a single row's values to the query.
func (b *InsertBuilder) Values(values ...interface{}) *InsertBuilder {
	b.rows = append(b.rows, values)
	return b
}

// Returning adds columns to RETURNING clause of the query
//
// INSERT ... RETURNING is PostgreSQL specific extension
func (b *InsertBuilder) Returning(columns ...string) *InsertBuilder {
	b.returning.Returning(columns...)
	return b
}

// ReturningSelect adds subquery to RETURNING clause of the query
//
// INSERT ... RETURNING is PostgreSQL specific extension
func (b *InsertBuilder) ReturningSelect(from *SelectBuilder, alias string) *InsertBuilder {
	b.returning.ReturningSelect(from, alias)
	return b
}

// Suffix adds an expression to the end of the query
func (b *InsertBuilder) Suffix(sql string, args ...interface{}) *InsertBuilder {
	b.suffixes = append(b.suffixes, Expr(sql, args...))
	return b
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b *InsertBuilder) SetMap(clauses map[string]interface{}) *InsertBuilder {
	// TODO: replace resetting previous values with extending existing ones?
	cols := make([]string, 0, len(clauses))
	vals := make([]interface{}, 0, len(clauses))

	for col, val := range clauses {
		cols = append(cols, col)
		vals = append(vals, val)
	}

	b.columns = cols
	b.rows = [][]interface{}{vals}
	return b
}

// Select set Select clause for insert query
// If Values and Select are used, then Select has higher priority
func (b *InsertBuilder) Select(sb *SelectBuilder) *InsertBuilder {
	b.iselect = sb
	return b
}
