package db

import (
	"metadataManager/types"

	"github.com/ermes-thesis/ermes-common/model"
)

// GenericDB provides a generic database abstraction for managing collections of record
// with metadata, group-based access control, and search capabilities. It can be used
// to interact with on-node database instances. It is not responsible for propagating
// changes to remote nodes.
//
// The interface supports hierarchical data organization where records are grouped into
// collections, collections can have associated metadata and group memberships, and
// the system provides full search capabilities across this metadata
//
// All operations are designed to be atomic and thread-safe.
type GenericDB interface {
	// GetRecord retrieves a single record from the specified collection.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordName: the name/key of the record to retrieve
	//
	// Returns:
	//	- string: the record value
	//	- error: non-nil if the record does not exist or if the operation fails
	GetRecord(collectionId string, recordName string) (string, error)
	// SetRecord stores a record in the specified collection with the given value.
	// The record key is automatically tracked in the collection's information.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordName: the name/key of the record to store
	//	- value: the record value to store
	//
	// Returns:
	//	- error: non-nil if the operation fails or if the collection does not exist
	SetRecord(collectionId string, recordName string, value string, timestamp string) error
	// GetRecords retrieves multiple records from a collection in a single operation.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordNames: variable number of record names to retrieve
	//
	// Returns:
	//	- []any: slice where each element corresponds to the requested record value.
	//			 Non-existent records will have nil values in the returned slice.
	//	- error: non-nil if the operation fails
	GetRecords(collectionId string, recordNames ...string) ([]any, error)
	// SetRecords stores multiple records in the specified collection in a single operation.
	// The record keys are automatically tracked in the collection's information.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordNames: slice of record names/keys to store
	//	- recordValues: slice of record values to store (must match recordNames length)
	//
	// Returns:
	//	- error: non-nil if the operation fails or if recordNames and recordValues
	//			 have different lengths or if the collection does not exist
	SetRecords(collectionId string, recordNames []string, recordValues []string, timestamp string) error
	// RecordExists checks whether a specific record exists in the given collection.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordName: the name/key of the record to check
	//
	// Returns:
	//	- bool: true if the record exists, false if the record or collection doesn't exist
	//	- error: non-nil if the check operation fails
	RecordExists(collectionId string, recordName string) (bool, error)
	// DeleteRecords removes the specified records from a collection.
	// The operation automatically updates the collection's key tracking information.
	// Non-existent records are silently ignored.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordNames: variable number of record names to delete
	//
	// Returns:
	//	- error: non-nil if any deletion operation fails
	DeleteRecords(collectionId string, timestamp string, recordNames ...string) error

	// GetCollection retrieves all records and associated groups for a collection.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//
	// Returns:
	//	- map[string]string: map of record names to values (empty if collection doesn't exist)
	//	- []string: slice of group names associated with the collection
	//	- error: non-nil if the operation fails
	GetCollection(collectionId string) (map[string]RecordValue, []string, string, uint64, error)
	// SetCollection creates or completely replaces a collection with the provided
	// records and groups. This operation will overwrite any existing data.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- records: map of record names to values to store in the collection
	//	- groups: slice of group names to associate with the collection
	//
	// Returns:
	//	- error: non-nil if the operation fails
	SetCollection(collectionId string, records map[string]RecordValue, groups []string, owner string, groupsSequence uint64) error
	// DeleteCollection removes a collection and all its associated data including
	// records, and groups. This operation is irreversible.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//
	// Returns:
	//	- error: non-nil if the deletion operation fails
	DeleteCollection(collectionId string) error
	// CollectionExists checks whether a collection exists in the database.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//
	// Returns:
	//	- bool: true if the collection exists, false otherwise
	//	- error: non-nil if the check operation fails
	CollectionExists(collectionId string) (bool, error)

	// GetMetadata retrieves the metadata associated with a collection.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//
	// Returns:
	//	- *Metadata: pointer to the metadata object, nil if no metadata exists
	//	- error: non-nil if the retrieval operation fails
	GetMetadata(collectionId string) (*model.Metadata, error)
	// SetMetadata associates metadata with a collection for search purposes.
	// Existing metadata will be completely replaced.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- metadata: the Metadata object to associate with the collection
	//
	// Returns:
	//	- error: non-nil if the operation fails
	SetMetadata(collectionId string, metadata model.Metadata) error

	MetadataExists(collectionId string) (bool, error)

	IsUniqueForOwner(collectionName string, owner string) (bool, error)

	// DeleteMetadata removes all metadata associated with a collection.
	// The collection itself and its records remain unchanged. They must
	// be explicitly deleted using the DeleteCollection method.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//
	// Returns:
	//	- error: non-nil if the deletion operation fails
	DeleteMetadata(collectionId string, deleteExistenceRecord bool) error

	UpdateGroupsInMetadata(collectionId string, groupsToAdd []string, groupsToRemove []string) ([]string, uint64, error)

	SetGroupsInMetadata(collectionId string, sequenceGroups uint64, groups ...string) error
	IsAuthorizedInMetadata(collectionId string, owner string, groups ...string) (bool, error)

	SetGroups(collectionId string, sequenceGroups uint64, groups ...string) error
	// IsAuthorized checks if any of the specified members belong to the collection's
	// groups. Returns true if al least one member is found.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- groups: variable number of group names to check
	//
	// Returns:
	//	- bool: true if at least one group is found, false if no groups are
	//			provided or none are found in the collection's groups
	//	- error: non-nil if the check operation fails
	IsAuthorized(collectionId string, owner string, groups ...string) (bool, error)

	// CreateIndex creates a search index on collection metadata fields.
	// The index automatically includes standard fields (name, owner, created_at,
	// groups) plus any custom metadata keys specified. Only one index can exist at a time.
	//
	// Parameters:
	//	- customMetadataKeys: variable number of additional metadata field names to index
	//
	// Returns:
	//	- error: non-nil if the index creation fails or when the method is called
	//			 more than 1 time without calling DropIndex method.
	CreateIndex(customMetadataKeys ...string) error
	// SearchOnMetadata performs a full search on collection metadata.
	// Results are automatically filtered to only include collections where the user
	// belongs to at least one of the specified groups.
	//
	// Parameters:
	//	- query: the model.QueryTemplate object containing search criteria
	//	- groups: slice of group names for access control filtering
	//
	// Returns:
	//	- []db.CollectionInfo: slice of matching collection information (empty if no matches)
	//	- error: non-nil if the search operation fails
	SearchOnMetadata(query model.QueryTemplate, owner string, groups []string) ([]types.CollectionInfo, error)
	// DropIndex removes the metadata search index, disabling search functionality.
	// This operation is irreversible and will require CreateIndex to be called
	// again to restore search capabilities.
	//
	// Returns:
	//	- error: non-nil if the index removal fails
	DropIndex() error
	// AlterIndex modifies the existing search index to include additional custom metadata fields.
	// The index must exist before calling this method.
	// Existing indexed fields remain unchanged.
	AlterIndex(customMetadataKeys ...string) error
	// GetMaxMemory retrieves the maximum memory limit set for the database.
	//
	// Returns:
	//	- uint64: maximum memory limit in bytes (0 if no limit is set)
	//	- error: non-nil if the memory limit retrieval fails
	GetMaxMemory() (uint64, error)
	// GetAvailableMemory retrieves the currently available memory for the database.
	//
	// Returns:
	//	- uint64: available memory in bytes (math.MaxUint64 if no limit is set or if the retrieval fails)
	//	- error: non-nil if the memory retrieval operation fails
	GetAvailableMemory() (uint64, error)
	// GetMemoryStats retrieves detailed memory usage statistics from the underlying
	// database system. The exact statistics returned depend on the implementation.
	//
	// Returns:
	//	- *MemStats: pointer to memory statistics object
	//	- error: non-nil if the statistics retrieval fails
	GetMemoryStats() (*MemStats, error)
	// GetCollectionMemoryUsage calculates the total memory usage of a collection
	// including all its records and associated information (keys tracking, groups, and
	// collection existence).
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//
	// Returns:
	//	- uint64: total memory usage in bytes (0 if collection doesn't exist)
	//	- error: non-nil if the memory calculation operation fails
	GetCollectionMemoryUsage(collectionId string) (uint64, error)
	// GetRecordsMemoryUsage calculates the memory usage of a specific record
	// within a collection.
	//
	// Parameters:
	//	- collectionId: the unique identifier of the collection
	//	- recordName: the name/key of the record to measure
	//
	// Returns:
	//	- uint64: memory usage of the record in bytes (0 if record doesn't exist)
	//	- error: non-nil if the memory calculation operation fails
	GetRecordsMemoryUsage(collectionId string, recordName ...string) (uint64, error)

	// Close closes the database connection and releases all associated resources.
	// The GenericDB instance should not be used after calling Close.
	// Multiple calls to Close should be safe and idempotent.
	//
	// Returns:
	//	- error: non-nil if the close operation fails
	Close() error
}
