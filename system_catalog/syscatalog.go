package SysCatalog

import (
	"myDb/types"
)

type RelationListSort []types.RelationListElement

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

func (a RelationListSort) Len() int           { return len(a) }
func (a RelationListSort) Less(i, j int) bool { return a[i].Type.Size < a[j].Type.Size }
func (a RelationListSort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
