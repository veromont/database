package operations

import (
	"fmt"
	SysCatalog "myDb/system_catalog"
	"myDb/types"
)

func operationAddElement(dsName string, pk1 interface{}, pk2 interface{}) error {
	ds := SysCatalog.GetDatasetByName(dsName)
	if ds == nil {
		return fmt.Errorf("ds with the name %s doesn`t exist", dsName)
	}

	newDsElement := types.NewDataset()
	newDsElement.OwnerKV = fmt.Sprintf("%v", pk1)
	newMemberKv := fmt.Sprintf("%v", pk2)

	dsElementIndex := -1
	memberElementIndexes := make([]int, 0)
	if ds.DatasetElements == nil {
		t := make([]types.DatasetElement, 0)
		ds.DatasetElements = &t
	}
	for i, dsElement := range *ds.DatasetElements {
		if dsElement.OwnerKV == newDsElement.OwnerKV {
			dsElementIndex = i
			for _, mkv := range *dsElement.MemberKVs {
				if mkv == newMemberKv {
					return fmt.Errorf("dataset owner member relation between %s and %s already exists", newDsElement.OwnerKV, newMemberKv)
				}
			}
		}
		for _, memberKV := range *dsElement.MemberKVs {
			if memberKV == newMemberKv {
				memberElementIndexes = append(memberElementIndexes, i)
			}
		}
	}

	// new element
	if dsElementIndex == -1 {
		t := []string{fmt.Sprintf("%v", pk2)}
		newDsElement.MemberKVs = &t
		*ds.DatasetElements = append(*ds.DatasetElements, *newDsElement)
		return nil
	}

	// appending element
	if ds.OwnerTableInfo.IsSingle && len(*(*ds.DatasetElements)[dsElementIndex].MemberKVs) >= 1 {
		return fmt.Errorf("owner is single and member already appended")
	}

	if ds.MemberTableInfo.IsSingle && len(memberElementIndexes) >= 1 {
		return fmt.Errorf("member is single and owner already exists")
	}

	*(*ds.DatasetElements)[dsElementIndex].MemberKVs = append(*(*ds.DatasetElements)[dsElementIndex].MemberKVs, newMemberKv)
	return nil
}

func operationUnionDS() error {
	// ds := SysCatalog.GetDatasetByName(dsName)
	// if ds == nil {
	// 	return fmt.Errorf("ds with the name %s doesn`t exist", dsName)
	// }

	// newDsElement := types.NewDataset()
	// newDsElement.OwnerKV = fmt.Sprintf("%v", pk1)
	// newMemberKv := fmt.Sprintf("%v", pk2)

	// dsElementIndex := -1
	// memberElementIndexes := make([]int, 0)
	// if ds.DatasetElements == nil {
	// 	t := make([]types.DatasetElement, 0)
	// 	ds.DatasetElements = &t
	// }
	// for i, dsElement := range *ds.DatasetElements {
	// 	if dsElement.OwnerKV == newDsElement.OwnerKV {
	// 		dsElementIndex = i
	// 		for _, mkv := range *dsElement.MemberKVs {
	// 			if mkv == newMemberKv {
	// 				return fmt.Errorf("dataset owner member relation between %s and %s already exists", newDsElement.OwnerKV, newMemberKv)
	// 			}
	// 		}
	// 	}
	// 	for _, memberKV := range *dsElement.MemberKVs {
	// 		if memberKV == newMemberKv {
	// 			memberElementIndexes = append(memberElementIndexes, i)
	// 		}
	// 	}
	// }

	// // new element
	// if dsElementIndex == -1 {
	// 	t := []string{fmt.Sprintf("%v", pk2)}
	// 	newDsElement.MemberKVs = &t
	// 	*ds.DatasetElements = append(*ds.DatasetElements, *newDsElement)
	// 	return nil
	// }

	// // appending element
	// if ds.OwnerTableInfo.IsSingle && len(*(*ds.DatasetElements)[dsElementIndex].MemberKVs) >= 1 {
	// 	return fmt.Errorf("owner is single and member already appended")
	// }

	// if ds.MemberTableInfo.IsSingle && len(memberElementIndexes) >= 1 {
	// 	return fmt.Errorf("member is single and owner already exists")
	// }

	// *(*ds.DatasetElements)[dsElementIndex].MemberKVs = append(*(*ds.DatasetElements)[dsElementIndex].MemberKVs, newMemberKv)

	return nil
}

func operation() error {
	// ds := SysCatalog.GetDatasetByName(dsName)
	// if ds == nil {
	// 	return fmt.Errorf("ds with the name %s doesn`t exist", dsName)
	// }

	// newDsElement := types.NewDataset()
	// newDsElement.OwnerKV = fmt.Sprintf("%v", pk1)
	// newMemberKv := fmt.Sprintf("%v", pk2)

	// dsElementIndex := -1
	// memberElementIndexes := make([]int, 0)
	// if ds.DatasetElements == nil {
	// 	t := make([]types.DatasetElement, 0)
	// 	ds.DatasetElements = &t
	// }
	// for i, dsElement := range *ds.DatasetElements {
	// 	if dsElement.OwnerKV == newDsElement.OwnerKV {
	// 		dsElementIndex = i
	// 		for _, mkv := range *dsElement.MemberKVs {
	// 			if mkv == newMemberKv {
	// 				return fmt.Errorf("dataset owner member relation between %s and %s already exists", newDsElement.OwnerKV, newMemberKv)
	// 			}
	// 		}
	// 	}
	// 	for _, memberKV := range *dsElement.MemberKVs {
	// 		if memberKV == newMemberKv {
	// 			memberElementIndexes = append(memberElementIndexes, i)
	// 		}
	// 	}
	// }

	// // new element
	// if dsElementIndex == -1 {
	// 	t := []string{fmt.Sprintf("%v", pk2)}
	// 	newDsElement.MemberKVs = &t
	// 	*ds.DatasetElements = append(*ds.DatasetElements, *newDsElement)
	// 	return nil
	// }

	// // appending element
	// if ds.OwnerTableInfo.IsSingle && len(*(*ds.DatasetElements)[dsElementIndex].MemberKVs) >= 1 {
	// 	return fmt.Errorf("owner is single and member already appended")
	// }

	// if ds.MemberTableInfo.IsSingle && len(memberElementIndexes) >= 1 {
	// 	return fmt.Errorf("member is single and owner already exists")
	// }

	// *(*ds.DatasetElements)[dsElementIndex].MemberKVs = append(*(*ds.DatasetElements)[dsElementIndex].MemberKVs, newMemberKv)

	return nil
}

func IntersectDS() error {
	// ds := SysCatalog.GetDatasetByName(dsName)
	// if ds == nil {
	// 	return fmt.Errorf("ds with the name %s doesn`t exist", dsName)
	// }

	// newDsElement := types.NewDataset()
	// newDsElement.OwnerKV = fmt.Sprintf("%v", pk1)
	// newMemberKv := fmt.Sprintf("%v", pk2)

	// dsElementIndex := -1
	// memberElementIndexes := make([]int, 0)
	// if ds.DatasetElements == nil {
	// 	t := make([]types.DatasetElement, 0)
	// 	ds.DatasetElements = &t
	// }
	// for i, dsElement := range *ds.DatasetElements {
	// 	if dsElement.OwnerKV == newDsElement.OwnerKV {
	// 		dsElementIndex = i
	// 		for _, mkv := range *dsElement.MemberKVs {
	// 			if mkv == newMemberKv {
	// 				return fmt.Errorf("dataset owner member relation between %s and %s already exists", newDsElement.OwnerKV, newMemberKv)
	// 			}
	// 		}
	// 	}
	// 	for _, memberKV := range *dsElement.MemberKVs {
	// 		if memberKV == newMemberKv {
	// 			memberElementIndexes = append(memberElementIndexes, i)
	// 		}
	// 	}
	// }

	// // new element
	// if dsElementIndex == -1 {
	// 	t := []string{fmt.Sprintf("%v", pk2)}
	// 	newDsElement.MemberKVs = &t
	// 	*ds.DatasetElements = append(*ds.DatasetElements, *newDsElement)
	// 	return nil
	// }

	// // appending element
	// if ds.OwnerTableInfo.IsSingle && len(*(*ds.DatasetElements)[dsElementIndex].MemberKVs) >= 1 {
	// 	return fmt.Errorf("owner is single and member already appended")
	// }

	// if ds.MemberTableInfo.IsSingle && len(memberElementIndexes) >= 1 {
	// 	return fmt.Errorf("member is single and owner already exists")
	// }

	// *(*ds.DatasetElements)[dsElementIndex].MemberKVs = append(*(*ds.DatasetElements)[dsElementIndex].MemberKVs, newMemberKv)

	return nil
}

func operationDeleteDS() error {
	// ds := SysCatalog.GetDatasetByName(dsName)
	// if ds == nil {
	// 	return fmt.Errorf("ds with the name %s doesn`t exist", dsName)
	// }

	// newDsElement := types.NewDataset()
	// newDsElement.OwnerKV = fmt.Sprintf("%v", pk1)
	// newMemberKv := fmt.Sprintf("%v", pk2)

	// dsElementIndex := -1
	// memberElementIndexes := make([]int, 0)
	// if ds.DatasetElements == nil {
	// 	t := make([]types.DatasetElement, 0)
	// 	ds.DatasetElements = &t
	// }
	// for i, dsElement := range *ds.DatasetElements {
	// 	if dsElement.OwnerKV == newDsElement.OwnerKV {
	// 		dsElementIndex = i
	// 		for _, mkv := range *dsElement.MemberKVs {
	// 			if mkv == newMemberKv {
	// 				return fmt.Errorf("dataset owner member relation between %s and %s already exists", newDsElement.OwnerKV, newMemberKv)
	// 			}
	// 		}
	// 	}
	// 	for _, memberKV := range *dsElement.MemberKVs {
	// 		if memberKV == newMemberKv {
	// 			memberElementIndexes = append(memberElementIndexes, i)
	// 		}
	// 	}
	// }

	// // new element
	// if dsElementIndex == -1 {
	// 	t := []string{fmt.Sprintf("%v", pk2)}
	// 	newDsElement.MemberKVs = &t
	// 	*ds.DatasetElements = append(*ds.DatasetElements, *newDsElement)
	// 	return nil
	// }

	// // appending element
	// if ds.OwnerTableInfo.IsSingle && len(*(*ds.DatasetElements)[dsElementIndex].MemberKVs) >= 1 {
	// 	return fmt.Errorf("owner is single and member already appended")
	// }

	// if ds.MemberTableInfo.IsSingle && len(memberElementIndexes) >= 1 {
	// 	return fmt.Errorf("member is single and owner already exists")
	// }

	// *(*ds.DatasetElements)[dsElementIndex].MemberKVs = append(*(*ds.DatasetElements)[dsElementIndex].MemberKVs, newMemberKv)
	return nil
}
