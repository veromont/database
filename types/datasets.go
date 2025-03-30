package types

import "strings"

type Dataset struct {
	Name            string
	OwnerTableInfo  TableInfo
	MemberTableInfo TableInfo
	DatasetElements *[]DatasetElement
}

type DatasetElement struct {
	OwnerKV     string
	TupleNumber int32
	MemberKVs   *[]string
}

type TableInfo struct {
	Table    *Relation
	IsSingle bool
}

func (ds *Dataset) ToString(delimeter string) string {
	result := ""
	result += "Dataset name: " + ds.Name + delimeter
	result += "Owner table name: " + ds.OwnerTableInfo.Table.Name + delimeter
	result += "owner single: " + boolToString(ds.OwnerTableInfo.IsSingle) + delimeter
	result += "member name: " + ds.MemberTableInfo.Table.Name + delimeter
	result += "member single: " + boolToString(ds.MemberTableInfo.IsSingle) + delimeter
	result += "----------------------DATASET ELEMENTS-------------------------------------" + delimeter
	for _, elem := range *ds.DatasetElements {
		result += "Dataset element----------" + delimeter
		result += elem.OwnerKV + delimeter
		result += strings.Join(*elem.MemberKVs, " ") + delimeter
		result += "-------------------" + delimeter
	}
	result += delimeter
	return result
}
func boolToString(t bool) string {
	if t {
		return "true"
	}
	return "false"
}

func NewDataset() *DatasetElement {
	kvs := make([]string, 0)
	return &DatasetElement{OwnerKV: "", MemberKVs: &kvs, TupleNumber: 0}
}
