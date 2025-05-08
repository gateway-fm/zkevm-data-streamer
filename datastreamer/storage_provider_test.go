package datastreamer

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

const (
	StSequencer StreamType = 1
)

func TestStorageProviders(t *testing.T) {
	// Create a dedicated temp directory for this test
	tempDir, err := os.MkdirTemp("", "storage_provider_test_")
	assert.NoError(t, err, "Failed to create temp directory")

	// Clean up everything at once after test
	defer os.RemoveAll(tempDir)

	file1 := filepath.Join(tempDir, "test_stream1.bin")
	file2 := filepath.Join(tempDir, "test_stream2.bin")

	// Create storage providers
	provider1, err := NewStreamFile(file1, 1, 1, StSequencer)
	if err != nil {
		t.Fatalf("Failed to create first storage provider: %v", err)
	}

	provider2, err := NewStreamFile(file2, 1, 1, StSequencer)
	if err != nil {
		t.Fatalf("Failed to create second storage provider: %v", err)
	}

	// Create test suite
	suite := NewStorageProviderTestSuite(t, provider1, provider2)

	// Run tests
	t.Run("BasicOperations", suite.TestBasicOperations)
	t.Run("AtomicOperations", suite.TestAtomicOperations)
	t.Run("BookmarkOperations", suite.TestBookmarkOperations)
	t.Run("IteratorOperations", suite.TestIteratorOperations)
}

// TestMultipleStorageProviders tests operations across different storage providers
func TestMultipleStorageProviders(t *testing.T) {
	// Create a dedicated temp directory for this test
	tempDir, err := os.MkdirTemp("", "multi_storage_provider_test_")
	assert.NoError(t, err, "Failed to create temp directory")

	// Clean up everything at once after test
	defer os.RemoveAll(tempDir)

	file1 := filepath.Join(tempDir, "test_stream1.bin")
	file2 := filepath.Join(tempDir, "test_stream2.bin")

	// Clean up after test
	defer func() {
		os.Remove(file1)
		os.Remove(file2)
	}()

	// Create different storage providers
	provider1, err := NewStreamFile(file1, 1, 1, StSequencer)
	if err != nil {
		t.Fatalf("Failed to create first storage provider: %v", err)
	}

	provider2, err := NewStreamFile(file2, 1, 1, StSequencer)
	if err != nil {
		t.Fatalf("Failed to create second storage provider: %v", err)
	}

	// Test suite for multiple providers
	suite := NewStorageProviderTestSuite(t, provider1, provider2)

	// Run tests
	t.Run("BasicOperations", suite.TestBasicOperations)
	t.Run("AtomicOperations", suite.TestAtomicOperations)
	t.Run("BookmarkOperations", suite.TestBookmarkOperations)
	t.Run("CrossProviderOperations", suite.TestCrossProviderOperations)
	t.Run("IteratorOperations", suite.TestIteratorOperations)
}

// TestCrossProviderOperations tests operations between different storage providers
func (s *StorageProviderTestSuite) TestCrossProviderOperations(t *testing.T) {
	// Start atomic operations
	err := s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in first provider")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in second provider")

	// Test data
	entryType := EntryType(1)
	data := []byte("cross-provider test data")
	bookmark := []byte("cross-provider-bookmark")

	// Add entry to first provider
	entryNum1, err := s.provider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry to first provider")

	// Add same entry to second provider
	entryNum2, err := s.comparisonProvider.AddStreamEntry(entryType, data)
	assert.NoError(t, err, "Failed to add stream entry to second provider")

	// Verify entry numbers match
	assert.Equal(t, entryNum1, entryNum2, "Entry numbers don't match across providers")

	// Commit atomic operations
	err = s.provider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in first provider")

	err = s.comparisonProvider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in second provider")

	// Start new atomic operations for bookmark testing
	err = s.provider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in first provider")

	err = s.comparisonProvider.StartAtomicOp()
	assert.NoError(t, err, "Failed to start atomic operation in second provider")

	// Test bookmark operations across providers
	bookmarkNum1, err := s.provider.AddStreamBookmark(bookmark)
	assert.NoError(t, err, "Failed to add bookmark to first provider")

	bookmarkNum2, err := s.comparisonProvider.AddStreamBookmark(bookmark)
	assert.NoError(t, err, "Failed to add bookmark to second provider")

	assert.Equal(t, bookmarkNum1, bookmarkNum2, "Bookmark numbers don't match across providers")

	// Commit atomic operations
	err = s.provider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in first provider")

	err = s.comparisonProvider.CommitAtomicOp()
	assert.NoError(t, err, "Failed to commit atomic operation in second provider")

	// Verify bookmark retrieval
	retrievedNum1, err := s.provider.GetBookmark(bookmark)
	assert.NoError(t, err, "Failed to get bookmark from first provider")

	retrievedNum2, err := s.comparisonProvider.GetBookmark(bookmark)
	assert.NoError(t, err, "Failed to get bookmark from second provider")

	assert.Equal(t, retrievedNum1, retrievedNum2, "Retrieved bookmark numbers don't match across providers")
}

// TestStorageProviderWithCustomImplementation tests a custom storage provider implementation
func TestStorageProviderWithCustomImplementation(t *testing.T) {
	// Create temporary files
	refFile := "test_ref.bin"
	customFile := "test_custom.bin"
	refDB := "test_ref_bookmarks"
	customDB := "test_custom_bookmarks"

	// Clean up after test
	defer func() {
		os.Remove(refFile)
		os.Remove(customFile)
		os.RemoveAll(refDB)
		os.RemoveAll(customDB)
	}()

	// Create reference provider
	refProvider, err := NewStreamFile(refFile, 1, 1, StSequencer)
	if err != nil {
		t.Fatalf("Failed to create reference storage provider: %v", err)
	}

	// Create custom provider
	customProvider, err := NewCustomStreamStore(customFile, 1, 1, StSequencer)
	if err != nil {
		t.Fatalf("Failed to create custom storage provider: %v", err)
	}

	// Create test suite
	suite := NewStorageProviderTestSuite(t, refProvider, customProvider)

	// Run tests
	t.Run("BasicOperations", suite.TestBasicOperations)
	t.Run("AtomicOperations", suite.TestAtomicOperations)
	t.Run("BookmarkOperations", suite.TestBookmarkOperations)
	t.Run("CrossProviderOperations", suite.TestCrossProviderOperations)
	t.Run("IteratorOperations", suite.TestIteratorOperations)
}

// CustomStreamStore is an example of a custom storage provider implementation
type CustomStreamStore struct {
	*StreamFile // Embed the reference implementation
	// Add custom fields and methods here
}

func NewCustomStreamStore(fileName string, version uint8, systemID uint64, streamType StreamType) (*CustomStreamStore, error) {
	base, err := NewStreamFile(fileName, version, systemID, streamType)
	if err != nil {
		return nil, err
	}

	return &CustomStreamStore{
		StreamFile: base,
	}, nil
}

// Override methods as needed
func (c *CustomStreamStore) AddStreamEntry(entryType EntryType, data []byte) (uint64, error) {
	// Add custom logic here
	return c.StreamFile.AddStreamEntry(entryType, data)
}
