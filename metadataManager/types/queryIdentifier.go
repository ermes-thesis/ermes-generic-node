package types

import "github.com/mitchellh/hashstructure/v2"

type QueryIdentifier struct {
	Name   string
	Params map[string]string
}

func NewQueryIdentifier(name string, params map[string]string) QueryIdentifier {
	return QueryIdentifier{
		Name:   name,
		Params: params,
	}
}

func (qi QueryIdentifier) Hash() uint64 {
	hash, _ := hashstructure.Hash(qi, hashstructure.FormatV2, nil)
	return hash
}
