package types

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination item_mock.go -package types github.com/uber/cadence/common/mapq/types Item

import "fmt"

type Item interface {
	// GetAttribute returns the value of the attribute key.
	// Value can be any type. It's up to the client to interpret the value.
	// Attribute keys used as partition keys will be converted to string because they are used as node
	// identifiers in the queue tree.
	GetAttribute(key string) any

	// Offset returns the offset of the item in the queue. e.g. monotonically increasing sequence number or a timestamp
	Offset() int64

	// String returns a human friendly representation of the item for logging purposes
	String() string
}

type ItemToPersist interface {
	Item
	ItemPartitions
}

func NewItemToPersist(item Item, itemPartitions ItemPartitions) ItemToPersist {
	return &defaultItemToPersist{
		item:           item,
		itemPartitions: itemPartitions,
	}
}

type ItemPartitions interface {
	// GetPartitionKeys returns the partition keys ordered by their level in the tree
	GetPartitionKeys() []string

	// GetPartitionValue returns the partition value to determine the target queue
	// e.g.
	//  Below example demonstrates that item is in a catch-all queue for sub-type
	//  	Item.GetAttribute("sub-type") returns "timer"
	//  	ItemPartitions.GetPartitionValue("sub-type") returns "*"
	//
	GetPartitionValue(key string) any

	// String returns a human friendly representation of the item for logging purposes
	String() string
}

func NewItemPartitions(partitionKeys []string, partitionMap map[string]any) ItemPartitions {
	return &defaultItemPartitions{
		partitionKeys: partitionKeys,
		partitionMap:  partitionMap,
	}
}

type defaultItemPartitions struct {
	partitionKeys []string
	partitionMap  map[string]any
}

func (i *defaultItemPartitions) GetPartitionKeys() []string {
	return i.partitionKeys
}

func (i *defaultItemPartitions) GetPartitionValue(key string) any {
	return i.partitionMap[key]
}

func (i *defaultItemPartitions) String() string {
	return fmt.Sprintf("ItemPartitions{partitionKeys:%v, partitionMap:%v}", i.partitionKeys, i.partitionMap)
}

type defaultItemToPersist struct {
	item           Item
	itemPartitions ItemPartitions
}

func (i *defaultItemToPersist) String() string {
	return fmt.Sprintf("ItemToPersist{item:%v, itemPartitions:%v}", i.item, i.itemPartitions)
}

func (i *defaultItemToPersist) Offset() int64 {
	return i.item.Offset()
}

func (i *defaultItemToPersist) GetAttribute(key string) any {
	return i.item.GetAttribute(key)
}

func (i *defaultItemToPersist) GetPartitionKeys() []string {
	return i.itemPartitions.GetPartitionKeys()
}

func (i *defaultItemToPersist) GetPartitionValue(key string) any {
	return i.itemPartitions.GetPartitionValue(key)
}
