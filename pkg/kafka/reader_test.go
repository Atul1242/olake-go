package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// offlineDialer points readers at an unreachable broker; clients are created lazily, so no connection is needed.
var offlineDialer = []kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}

func newOfflineManager(membership GroupMembership) *ReaderManager {
	r := NewReaderManager(ReaderConfig{
		ConsumerGroupID: "olake-test-group",
		Dialer:          offlineDialer,
		Membership:      membership,
	})
	r.topics = []string{"orders"}
	return r
}

func TestReaderInstanceID(t *testing.T) {
	t.Run("default uses group ULID", func(t *testing.T) {
		r := newOfflineManager(GroupMembership{})
		seen := map[string]struct{}{}

		first, err := r.readerInstanceID(0, seen)
		require.NoError(t, err)
		second, err := r.readerInstanceID(1, seen)
		require.NoError(t, err)

		assert.True(t, strings.HasPrefix(first, "group_"))
		assert.True(t, strings.HasPrefix(second, "group_"))
		assert.NotEqual(t, first, second)
	})

	t.Run("custom function is used", func(t *testing.T) {
		r := newOfflineManager(GroupMembership{
			InstanceID: func(readerIndex int) string { return fmt.Sprintf("job-1-run-2-task-%d", readerIndex) },
		})

		readerID, err := r.readerInstanceID(3, map[string]struct{}{})
		require.NoError(t, err)
		assert.Equal(t, "job-1-run-2-task-3", readerID)
	})

	t.Run("empty id is rejected", func(t *testing.T) {
		r := newOfflineManager(GroupMembership{
			InstanceID: func(int) string { return "" },
		})

		_, err := r.readerInstanceID(0, map[string]struct{}{})
		require.ErrorContains(t, err, "empty instance id")
	})

	t.Run("duplicate id is rejected", func(t *testing.T) {
		r := newOfflineManager(GroupMembership{
			InstanceID: func(int) string { return "same-seat" },
		})
		seen := map[string]struct{}{}

		_, err := r.readerInstanceID(0, seen)
		require.NoError(t, err)
		_, err = r.readerInstanceID(1, seen)
		require.ErrorContains(t, err, "duplicate instance id")
	})
}

func TestCreateReaderMembershipOptions(t *testing.T) {
	t.Run("zero value keeps franz-go default session timeout", func(t *testing.T) {
		r := newOfflineManager(GroupMembership{})

		client, err := r.CreateReader("group_abc", "olake-test-group-group_abc", false)
		require.NoError(t, err)
		defer client.Close()

		assert.Equal(t, 45*time.Second, client.OptValue(kgo.SessionTimeout))
		assert.Equal(t, "group_abc", client.OptValue(kgo.InstanceID))
	})

	t.Run("session timeout is applied", func(t *testing.T) {
		r := newOfflineManager(GroupMembership{SessionTimeout: 30 * time.Minute})

		client, err := r.CreateReader("job-1-run-2-task-0", "olake-test-group-task-0", true)
		require.NoError(t, err)
		defer client.Close()

		assert.Equal(t, 30*time.Minute, client.OptValue(kgo.SessionTimeout))
		assert.Equal(t, "job-1-run-2-task-0", client.OptValue(kgo.InstanceID))
	})
}

func TestReleaseReaders(t *testing.T) {
	r := newOfflineManager(GroupMembership{})
	for readerIndex := range 2 {
		readerID := fmt.Sprintf("task-%d", readerIndex)
		client, err := r.CreateReader(readerID, "client-"+readerID, false)
		require.NoError(t, err)
		r.readers = append(r.readers, &kafkaReader{id: readerID, clientID: "client-" + readerID, reader: client})
	}

	r.ReleaseReaders()

	assert.Equal(t, 2, r.GetReaderCount(), "reader count must stay so MaxConnections is unchanged")
	for readerIndex := range 2 {
		assert.Nil(t, r.GetReader(readerIndex))
		readerID, _ := r.GetReaderIDAndClientID(readerIndex)
		assert.Equal(t, fmt.Sprintf("task-%d", readerIndex), readerID)
	}

	// a second release (e.g. from Close) must be a no-op
	assert.NotPanics(t, r.ReleaseReaders)

	// a released reader cannot be restarted for reading
	_, err := r.RestartReader(0)
	require.ErrorContains(t, err, "reader not found")
}

func TestSamePartitions(t *testing.T) {
	planned := []types.PartitionKey{{Topic: "orders", Partition: 1}, {Topic: "payments", Partition: 0}}

	assert.True(t, samePartitions(planned, []types.PartitionKey{{Topic: "payments", Partition: 0}, {Topic: "orders", Partition: 1}}), "order must not matter")
	assert.True(t, samePartitions(nil, []types.PartitionKey{}))
	assert.False(t, samePartitions(planned, planned[:1]))
	assert.False(t, samePartitions(planned, []types.PartitionKey{{Topic: "orders", Partition: 2}, {Topic: "payments", Partition: 0}}))
	assert.Equal(t, "orders", planned[0].Topic, "input must not be reordered")
}

func TestCheckExpectedAssignmentDisabled(t *testing.T) {
	// no admin client is set, so this passes only if the check skips the group lookup when ExpectedAssignment is nil
	r := newOfflineManager(GroupMembership{})
	r.readers = append(r.readers, &kafkaReader{id: "task-0"})

	require.NoError(t, r.checkExpectedAssignment(context.Background()))
}
