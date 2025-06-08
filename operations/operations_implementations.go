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

func operationUnionDS(dsName1, dsName2, newDsName string) error {
	// Fetch datasets
	ds1 := SysCatalog.GetDatasetByName(dsName1)
	ds2 := SysCatalog.GetDatasetByName(dsName2)

	if ds1 == nil || ds2 == nil {
		return fmt.Errorf("one or both source datasets do not exist")
	}

	newDs := types.Dataset{}
	newDs.Name = newDsName
	newDs.OwnerTableInfo = ds1.OwnerTableInfo
	newDs.MemberTableInfo = ds1.MemberTableInfo
	newElements := make([]types.DatasetElement, 0)
	uniquePairs := make(map[string]map[string]bool)

	addPair := func(owner, member string) {
		if _, ok := uniquePairs[owner]; !ok {
			uniquePairs[owner] = make(map[string]bool)
		}
		uniquePairs[owner][member] = true
	}

	processDataset := func(ds *types.Dataset) {
		if ds.DatasetElements == nil {
			return
		}
		for _, element := range *ds.DatasetElements {
			if element.MemberKVs == nil {
				continue
			}
			for _, member := range *element.MemberKVs {
				addPair(element.OwnerKV, member)
			}
		}
	}

	processDataset(ds1)
	processDataset(ds2)

	for owner, members := range uniquePairs {
		element := types.DatasetElement{
			OwnerKV:   owner,
			MemberKVs: new([]string),
		}
		for member := range members {
			*element.MemberKVs = append(*element.MemberKVs, member)
		}
		newElements = append(newElements, element)
	}
	newDs.DatasetElements = &newElements

	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)

	return nil
}

func operationCrossDS(dsName1, dsName2, newDsName string) error {
	ds1 := SysCatalog.GetDatasetByName(dsName1)
	ds2 := SysCatalog.GetDatasetByName(dsName2)

	if ds1 == nil || ds2 == nil {
		return fmt.Errorf("one or both source datasets do not exist")
	}

	newDs := types.Dataset{}
	newDs.Name = newDsName
	newDs.OwnerTableInfo = ds1.OwnerTableInfo
	newDs.MemberTableInfo = ds2.MemberTableInfo
	newElements := make([]types.DatasetElement, 0)

	for _, el1 := range *ds1.DatasetElements {
		if el1.MemberKVs == nil {
			continue
		}
		for _, member1 := range *el1.MemberKVs {
			for _, el2 := range *ds2.DatasetElements {
				if el2.MemberKVs == nil {
					continue
				}
				for _, member2 := range *el2.MemberKVs {
					owner := fmt.Sprintf("%s", member1)
					member := fmt.Sprintf("%s", member2)
					newElement := types.DatasetElement{
						OwnerKV:   owner,
						MemberKVs: &[]string{member},
					}
					newElements = append(newElements, newElement)
				}
			}
		}
	}

	newDs.DatasetElements = &newElements
	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)

	return nil
}

func operationIntersectDS(dsName1, dsName2, newDsName string) error {
	ds1 := SysCatalog.GetDatasetByName(dsName1)
	ds2 := SysCatalog.GetDatasetByName(dsName2)

	if ds1 == nil || ds2 == nil {
		return fmt.Errorf("one or both source datasets do not exist")
	}

	newDs := types.Dataset{}
	newDs.Name = newDsName
	newDs.OwnerTableInfo = ds1.OwnerTableInfo
	newDs.MemberTableInfo = ds1.MemberTableInfo
	newElements := make([]types.DatasetElement, 0)

	pairs1 := make(map[string]map[string]bool)
	pairs2 := make(map[string]map[string]bool)

	collectPairs := func(ds *types.Dataset, store map[string]map[string]bool) {
		if ds.DatasetElements == nil {
			return
		}
		for _, element := range *ds.DatasetElements {
			if element.MemberKVs == nil {
				continue
			}
			if _, ok := store[element.OwnerKV]; !ok {
				store[element.OwnerKV] = make(map[string]bool)
			}
			for _, member := range *element.MemberKVs {
				store[element.OwnerKV][member] = true
			}
		}
	}

	collectPairs(ds1, pairs1)
	collectPairs(ds2, pairs2)

	for owner, members1 := range pairs1 {
		members2, exists := pairs2[owner]
		if !exists {
			continue
		}

		intersected := make([]string, 0)
		for member := range members1 {
			if members2[member] {
				intersected = append(intersected, member)
			}
		}

		if len(intersected) > 0 {
			element := types.DatasetElement{
				OwnerKV:   owner,
				MemberKVs: &intersected,
			}
			newElements = append(newElements, element)
		}
	}

	newDs.DatasetElements = &newElements
	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)

	return nil
}

func operationDeleteDS(dsName string) error {
	index := -1
	for i, ds := range SysCatalog.Datasets {
		if ds.Name == dsName {
			index = i
			break
		}
	}

	if index == -1 {
		return fmt.Errorf("dataset with name %s does not exist", dsName)
	}

	SysCatalog.Datasets = append(SysCatalog.Datasets[:index], SysCatalog.Datasets[index+1:]...)
	return nil
}

func operationReverseDS(dsName string, newDsName string) error {
	ds := SysCatalog.GetDatasetByName(dsName)
	if ds == nil {
		return fmt.Errorf("dataset with name %s does not exist", dsName)
	}

	newDs := types.Dataset{
		Name:            newDsName,
		OwnerTableInfo:  ds.MemberTableInfo, // reversed
		MemberTableInfo: ds.OwnerTableInfo,  // reversed
	}
	newElements := make([]types.DatasetElement, 0)
	ownerToMembers := make(map[string][]string)

	if ds.DatasetElements == nil {
		return fmt.Errorf("dataset %s is empty", dsName)
	}

	for _, element := range *ds.DatasetElements {
		if element.MemberKVs == nil {
			continue
		}
		for _, member := range *element.MemberKVs {
			ownerToMembers[member] = append(ownerToMembers[member], element.OwnerKV)
		}
	}

	for owner, members := range ownerToMembers {
		copyMembers := make([]string, len(members))
		copy(copyMembers, members)
		newElements = append(newElements, types.DatasetElement{
			OwnerKV:   owner,
			MemberKVs: &copyMembers,
		})
	}
	newDs.DatasetElements = &newElements
	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)
	return nil
}

func operationComposeDS(dsName, relation1, relation2, fieldName1, fieldValue1, fieldName2, fieldValue2 string) error {
	ds := SysCatalog.GetDatasetByName(dsName)
	if ds == nil || ds.DatasetElements == nil {
		return fmt.Errorf("dataset %s not found or empty", dsName)
	}

	intermediateMembers := make(map[string]bool)
	for _, element := range *ds.DatasetElements {
		if element.OwnerKV == fieldValue1 {
			if element.MemberKVs != nil {
				for _, member := range *element.MemberKVs {
					intermediateMembers[member] = true
				}
			}
			break
		}
	}

	if len(intermediateMembers) == 0 {
		return fmt.Errorf("no members found for %s=%s", fieldName1, fieldValue1)
	}

	finalMembers := make(map[string]bool)
	for _, element := range *ds.DatasetElements {
		if intermediateMembers[element.OwnerKV] && element.MemberKVs != nil {
			for _, member := range *element.MemberKVs {
				finalMembers[member] = true
			}
		}
	}

	newDsName := fmt.Sprintf("%s_Composed", dsName)
	newDs := types.Dataset{
		Name:            newDsName,
		OwnerTableInfo:  ds.OwnerTableInfo,
		MemberTableInfo: ds.MemberTableInfo,
		DatasetElements: &[]types.DatasetElement{},
	}
	membersSlice := make([]string, 0, len(finalMembers))
	for member := range finalMembers {
		membersSlice = append(membersSlice, member)
	}
	*newDs.DatasetElements = append(*newDs.DatasetElements, types.DatasetElement{
		OwnerKV:   fieldValue1,
		MemberKVs: &membersSlice,
	})

	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)
	return nil
}

func operationBFilter(ds1Name, ds2Name, newDsName string) error {
	ds1 := SysCatalog.GetDatasetByName(ds1Name)
	ds2 := SysCatalog.GetDatasetByName(ds2Name)

	if ds1 == nil || ds2 == nil {
		return fmt.Errorf("one or both datasets not found")
	}

	filterOwners := make(map[string]bool)
	for _, el := range *ds2.DatasetElements {
		filterOwners[el.OwnerKV] = true
	}

	filteredElements := make([]types.DatasetElement, 0)
	for _, el := range *ds1.DatasetElements {
		if filterOwners[el.OwnerKV] {
			filteredElements = append(filteredElements, el)
		}
	}

	newDs := types.Dataset{
		Name:            newDsName,
		OwnerTableInfo:  ds1.OwnerTableInfo,
		MemberTableInfo: ds1.MemberTableInfo,
		DatasetElements: &filteredElements,
	}
	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)
	return nil
}

func operationJoinDS(ds1Name, ds2Name, newDsName string) error {
	ds1 := SysCatalog.GetDatasetByName(ds1Name)
	ds2 := SysCatalog.GetDatasetByName(ds2Name)

	if ds1 == nil || ds2 == nil {
		return fmt.Errorf("one or both datasets not found")
	}

	ds2Map := make(map[string][]string)
	for _, el := range *ds2.DatasetElements {
		if el.MemberKVs != nil {
			ds2Map[el.OwnerKV] = append(ds2Map[el.OwnerKV], *el.MemberKVs...)
		}
	}

	joinedElements := make([]types.DatasetElement, 0)
	for _, el := range *ds1.DatasetElements {
		newMembers := make([]string, 0)
		for _, intermediate := range *el.MemberKVs {
			newMembers = append(newMembers, ds2Map[intermediate]...)
		}
		if len(newMembers) > 0 {
			joinedElements = append(joinedElements, types.DatasetElement{
				OwnerKV:   el.OwnerKV,
				MemberKVs: &newMembers,
			})
		}
	}

	newDs := types.Dataset{
		Name:            newDsName,
		OwnerTableInfo:  ds1.OwnerTableInfo,
		MemberTableInfo: ds2.MemberTableInfo,
		DatasetElements: &joinedElements,
	}
	SysCatalog.Datasets = append(SysCatalog.Datasets, newDs)
	return nil
}
