package shakti

// lower tables take precedence over higher tables as they are newer
func MergeSSTables(lowerTables []SSTable, higherTables []SSTable) ([]SSTable, error) {
	// TODO create a merging iterator and output to one or more SSTables
	return nil, nil
}
