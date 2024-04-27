package types

type DsListElement struct {
	Name            string
	OwnerTableInfo  TableInfo
	MemberTableInfo TableInfo
	Datasets        []Dataset
}

type Dataset struct {
	OwnerKV     string
	TupleNumber int32
	MemberKVs   []MemberKV
}

type MemberKV struct {
	KV          string
	TupleNumber int32
}

type TableInfo struct {
	Table    *Relation
	IsSingle bool
}

func (ds *DsListElement) ToString(delimeter string) string {
	result := ""
	result += "Dataset name: " + ds.Name + delimeter
	result += "Owner table name: " + ds.OwnerTableInfo.Table.Name + delimeter
	result += "owner single: " + boolToString(ds.OwnerTableInfo.IsSingle) + delimeter
	result += "member name: " + ds.MemberTableInfo.Table.Name + delimeter
	result += "member single: " + boolToString(ds.MemberTableInfo.IsSingle) + delimeter
	return result
}
func boolToString(t bool) string {
	if t {
		return "true"
	}
	return "false"
}
