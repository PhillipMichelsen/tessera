package processor

type Registry struct{}

type Factory interface {
	New() Processor
	Type() string
	Version() string

	TemplateFingerprint(cfg string) (string, error)
}
