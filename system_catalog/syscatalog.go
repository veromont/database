package SysCatalog

import (
	"fmt"
	"myDb/types"
)

type RelationListSort []types.RelationListElement

var Relations []types.RelationListElement
var Datasets []types.DsListElement

func NewDB() {
	Relations = make([]types.RelationListElement, 0)
	Datasets = make([]types.DsListElement, 0)
}

func GetRelationByName(name string) (*types.RelationListElement, *types.Relation) {
	for _, rle := range Relations {
		for _, relation := range rle.Relations {
			if relation.Name == name {
				return &rle, &relation
			}
		}
	}
	return nil, nil
}

func GetDatasetByName(name string) *types.DsListElement {
	for _, ds := range Datasets {
		if ds.Name == name {
			return &ds
		}
	}
	return nil
}

func DeleteRelationByName(name string) error {
	for i, rle := range Relations {
		for j, relation := range rle.Relations {
			if relation.Name == name {
				if len(rle.Relations) == 1 {
					ds := isDatasetDependency(relation)
					if ds != nil {
						return fmt.Errorf("таблицю %s не видалено, помилка: набір даних %s залежить від таблиці",
							name, ds.Name)
					}
					// delete relation list elment
					Relations = append(Relations[:j], Relations[j+1:]...)
					return nil
				} else {
					// delete relation
					Relations[i].Relations = append(rle.Relations[:j], rle.Relations[j+1:]...)
					return nil
				}
			}
		}
	}
	return fmt.Errorf("таблицю %s не видалено, помилка: таблицю не знайдено", name)
}

func DeleteDatasetByName(name string) error {
	for i, ds := range Datasets {
		if ds.Name == name {
			Datasets = append(Datasets[:i], Datasets[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("набір даних %s не видалено, помилка: набір даних не знайдено", name)
}

func isDatasetDependency(relation types.Relation) *types.DsListElement {
	for _, ds := range Datasets {
		if ds.OwnerTableInfo.Table.Name == relation.Name || ds.MemberTableInfo.Table.Name == relation.Name {
			return &ds
		}
	}
	return nil
}

func (a RelationListSort) Len() int           { return len(a) }
func (a RelationListSort) Less(i, j int) bool { return a[i].Type.Size < a[j].Type.Size }
func (a RelationListSort) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
