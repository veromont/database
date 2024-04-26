package SysCatalog

import (
	"task1/types"
)

var Relations []types.RelationListElement
var Datasets []types.DsListElement

func NewDB() {
	Relations = make([]types.RelationListElement, 0)
	Datasets = make([]types.DsListElement, 0)
}

func GetRelationByName(name string) *types.Relation {
	for _, rle := range Relations {
		for _, relation := range rle.Relations {
			if relation.Name == name {
				return &relation
			}
		}
	}
	return nil
}

func GetDatasetByName(name string) *types.DsListElement {
	for _, ds := range Datasets {
		if ds.Name == name {
			return &ds
		}
	}
	return nil
}
