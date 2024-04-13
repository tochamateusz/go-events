package backgroundworkers

type Task int

const (
	TaskIssueReceipt Task = iota
	TaskAppendToTracker
)
