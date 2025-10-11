package node

type Type string

const (
	Source    Type = "source"
	Processor Type = "processor"
	Sink      Type = "sink"
)

type Template struct {
	Kind    Type
	Type    string
	Version string
	Config  string
}
