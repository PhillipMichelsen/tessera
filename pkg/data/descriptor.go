package data

type Descriptor struct {
	Type       string
	Key        string
	Schema     string
	Attributes map[string]string
	Tags       []string
}
