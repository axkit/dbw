package dbw

const (
	ValidateLen int = iota << 1
	ValidateNotNull
	ValidateIntRange
)

type RowValidationResult map[string]int

// ValidateRow
func (t *Table) ValidateRow(row interface{}, mask int) RowValidationResult {
	return nil
}
