package datastreamer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// StorageProviderTestSuite defines the interface for testing storage providers
type StorageProviderTestSuite struct {
	t                  *testing.T
	provider           StreamStore
	comparisonProvider StreamStore
}

// NewStorageProviderTestSuite creates a new test suite for storage providers
func NewStorageProviderTestSuite(t *testing.T, provider StreamStore, comparisonProvider StreamStore) *StorageProviderTestSuite {
	return &StorageProviderTestSuite{
		t:                  t,
		provider:           provider,
		comparisonProvider: comparisonProvider,
	}
}

// TestBasicOperations tests basic stream operations
func (s *StorageProviderTestSuite) TestBasicOperations(t *testing.T) {
	// Start atomic operations
	err := s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in comparison provider")

	// Test adding entries
	entryType := EntryType(1)
	data := []byte("test data")

	entryNum1, err := s.provider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry")

	entryNum2, err := s.comparisonProvider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry to comparison provider")

	assert.Equal(t, entryNum1, entryNum2, "Entry numbers don't match")

	// Commit atomic operations
	err = s.provider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation")

	err = s.comparisonProvider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in comparison provider")

	// Test getting entries
	entry1, err := s.provider.GetEntry(entryNum1)
	assert.NoError(t, err, "Failed to get entry")

	entry2, err := s.comparisonProvider.GetEntry(entryNum2)
	assert.NoError(t, err, "Failed to get entry from comparison provider")

	assert.True(t, compareEntries(entry1, entry2), "Entries don't match")
}

// TestAtomicOperations tests atomic operation functionality
func (s *StorageProviderTestSuite) TestAtomicOperations(t *testing.T) {
	// Start atomic operation
	err := s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in comparison provider")

	// Add entries in atomic operation
	entryType := EntryType(1)
	data := []byte("atomic test data")

	_, err = s.provider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry in atomic operation")

	_, err = s.comparisonProvider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry in atomic operation to comparison provider")

	// Commit atomic operation
	err = s.provider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation")

	err = s.comparisonProvider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in comparison provider")

	// Verify entries after commit
	header1 := s.provider.GetHeader()
	header2 := s.comparisonProvider.GetHeader()

	assert.Equal(t, header1.TotalEntries, header2.TotalEntries, "Total entries don't match after atomic operation")
}

// TestBookmarkOperations tests bookmark functionality
func (s *StorageProviderTestSuite) TestBookmarkOperations(t *testing.T) {
	// Start atomic operations
	err := s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in comparison provider")

	bookmark := []byte("test-bookmark")

	// Add bookmark
	entryNum1, err := s.provider.AddStreamBookmark(bookmark)
	assert.NoError(t, err, "Failed to add bookmark")

	entryNum2, err := s.comparisonProvider.AddStreamBookmark(bookmark)
	assert.NoError(t, err, "Failed to add bookmark to comparison provider")

	assert.Equal(t, entryNum1, entryNum2, "Bookmark entry numbers don't match")

	// Commit atomic operations
	err = s.provider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation")

	err = s.comparisonProvider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in comparison provider")

	// Get bookmark
	retrievedEntryNum1, err := s.provider.GetBookmark(bookmark)
	assert.NoError(t, err, "Failed to get bookmark")

	retrievedEntryNum2, err := s.comparisonProvider.GetBookmark(bookmark)
	assert.NoError(t, err, "Failed to get bookmark from comparison provider")

	assert.Equal(t, retrievedEntryNum1, retrievedEntryNum2, "Retrieved bookmark entry numbers don't match")
}

// TestIteratorOperations tests iterator functionality
func (s *StorageProviderTestSuite) TestIteratorOperations(t *testing.T) {
	// Start atomic operation
	err := s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in comparison provider")

	// Add some test entries
	entryType := EntryType(1)
	data1 := []byte("test data 1")
	data2 := []byte("test data 2")
	data3 := []byte("test data 3")

	firstEntryNum, err := s.provider.AddStreamEntry(entryType, data1)
	assert.NoError(t, err, "Failed to add first stream entry")

	_, err = s.comparisonProvider.AddStreamEntry(entryType, data1)
	assert.NoError(t, err, "Failed to add first stream entry to comparison provider")

	_, err = s.provider.AddStreamEntry(entryType, data2)
	assert.NoError(t, err, "Failed to add second stream entry")

	_, err = s.comparisonProvider.AddStreamEntry(entryType, data2)
	assert.NoError(t, err, "Failed to add second stream entry to comparison provider")

	_, err = s.provider.AddStreamEntry(entryType, data3)
	assert.NoError(t, err, "Failed to add third stream entry")

	_, err = s.comparisonProvider.AddStreamEntry(entryType, data3)
	assert.NoError(t, err, "Failed to add third stream entry to comparison provider")

	// Commit atomic operation
	err = s.provider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation")

	err = s.comparisonProvider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in comparison provider")

	// Test iterator
	iterator, err := s.provider.GetIterator(firstEntryNum, true)
	assert.NoError(t, err, "Failed to get iterator")
	defer iterator.End()

	// Read first entry
	end, err := iterator.Next()
	assert.NoError(t, err, "Failed to get next entry")
	assert.False(t, end, "Iterator unexpectedly ended after first entry")

	entry1 := iterator.GetEntry()
	assert.True(t, compareBytes(entry1.Data, data1), "First entry data doesn't match")

	// Read second entry
	end, err = iterator.Next()
	assert.NoError(t, err, "Failed to get next entry")
	assert.False(t, end, "Expected second entry to exist, but iterator ended")

	entry2 := iterator.GetEntry()
	assert.True(t, compareBytes(entry2.Data, data2), "Second entry data doesn't match")

	// Read third entry (last entry)
	end, err = iterator.Next()
	assert.NoError(t, err, "Failed to get next entry")
	assert.False(t, end, "Expected third entry to exist, but iterator ended")

	entry3 := iterator.GetEntry()
	assert.True(t, compareBytes(entry3.Data, data3), "Third entry data doesn't match")

	// Try to read past the end
	end, err = iterator.Next()
	assert.NoError(t, err, "Failed to get next entry")
	assert.True(t, end, "Expected iterator to end after the third entry")

	// The last successful entry should still be available via GetEntry
	lastEntry := iterator.GetEntry()
	assert.True(t, compareBytes(lastEntry.Data, data3), "Last entry data doesn't match after reaching end")
}

// TestAtomicOperationRollback tests atomic operation rollback functionality
func (s *StorageProviderTestSuite) TestAtomicOperationRollback(t *testing.T) {
	// Get initial state
	initialHeader1 := s.provider.GetHeader()
	initialHeader2 := s.comparisonProvider.GetHeader()

	// Start atomic operation
	err := s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in comparison provider")

	// Add entries in atomic operation
	entryType := EntryType(1)
	data := []byte("rollback test data")

	_, err = s.provider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry in atomic operation")

	_, err = s.comparisonProvider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry in atomic operation to comparison provider")

	// Rollback atomic operation
	err = s.provider.RollbackAtomicOp()
	assert.NoError(t, err, "Failed to rollback atomic operation")

	err = s.comparisonProvider.RollbackAtomicOp()
	assert.NoError(t, err, "Failed to rollback atomic operation in comparison provider")

	// Verify state after rollback
	finalHeader1 := s.provider.GetHeader()
	finalHeader2 := s.comparisonProvider.GetHeader()

	assert.Equal(t, initialHeader1.TotalEntries, finalHeader1.TotalEntries, "Total entries changed after rollback")
	assert.Equal(t, initialHeader2.TotalEntries, finalHeader2.TotalEntries, "Total entries changed after rollback in comparison provider")
}

// Helper function to compare entries
func compareEntries(entry1, entry2 FileEntry) bool {
	if entry1.Type != entry2.Type {
		return false
	}
	if entry1.Number != entry2.Number {
		return false
	}
	if len(entry1.Data) != len(entry2.Data) {
		return false
	}
	for i := range entry1.Data {
		if entry1.Data[i] != entry2.Data[i] {
			return false
		}
	}
	return true
}

// Helper function to compare byte slices
func compareBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
