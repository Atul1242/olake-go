package kafka

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// These tests need a running broker, e.g. `make olake.kafka.start` and
// OLAKE_KAFKA_TEST_BROKERS=127.0.0.1:29092. They are skipped otherwise.
const (
	testBrokersEnv      = "OLAKE_KAFKA_TEST_BROKERS"
	testTopicPartitions = 4
)

type brokerEnv struct {
	dialer  []kgo.Opt
	client  *kgo.Client
	admin   *kadm.Client
	topics  []string
	groupID string
	streams []types.StreamInterface
}

// newBrokerEnv creates one topic per entry in partitionsPerTopic (default: one topic with testTopicPartitions).
func newBrokerEnv(t *testing.T, partitionsPerTopic ...int32) *brokerEnv {
	t.Helper()
	if len(partitionsPerTopic) == 0 {
		partitionsPerTopic = []int32{testTopicPartitions}
	}
	brokers := os.Getenv(testBrokersEnv)
	if brokers == "" {
		t.Skipf("%s not set, skipping broker test", testBrokersEnv)
	}

	ctx := context.Background()
	dialer := []kgo.Opt{kgo.SeedBrokers(strings.Split(brokers, ",")...)}
	client, err := kgo.NewClient(append(dialer, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	require.NoError(t, err)
	admin := kadm.NewClient(client)

	suffix := strings.ToLower(utils.ULID())
	env := &brokerEnv{
		dialer:  dialer,
		client:  client,
		admin:   admin,
		groupID: "olake-membership-test-" + suffix,
	}

	for topicIndex, partitions := range partitionsPerTopic {
		topic := fmt.Sprintf("membership_test_%s_%d", suffix, topicIndex)
		_, err = admin.CreateTopic(ctx, partitions, 1, nil, topic)
		require.NoError(t, err)
		env.topics = append(env.topics, topic)

		// two records per partition so every partition is active
		for partition := range partitions {
			for i := range 2 {
				record := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(fmt.Sprintf(`{"i":%d}`, i))}
				require.NoError(t, client.ProduceSync(ctx, record).FirstErr())
			}
		}
	}
	env.streams = streamsFor(env.topics...)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		// remove all seats (default cleanup path), then the group and the topic
		cleanup := env.manager(1, GroupMembership{})
		_ = cleanup.RemoveExistingConsumers(cleanupCtx, client)
		_, _ = admin.DeleteGroups(cleanupCtx, env.groupID)
		_, _ = admin.DeleteTopics(cleanupCtx, env.topics...)
		client.Close()
	})
	return env
}

func streamsFor(topics ...string) []types.StreamInterface {
	streams := make([]types.StreamInterface, 0, len(topics))
	for _, topic := range topics {
		streams = append(streams, types.NewStream(topic, "membership_test", nil).Wrap(0))
	}
	return streams
}

// topicsOf returns the distinct topics in a seat's assignment.
func topicsOf(partitions []types.PartitionKey) []string {
	var topics []string
	for _, partition := range partitions {
		if !slices.Contains(topics, partition.Topic) {
			topics = append(topics, partition.Topic)
		}
	}
	return topics
}

func (e *brokerEnv) manager(maxThreads int, membership GroupMembership) *ReaderManager {
	return NewReaderManager(ReaderConfig{
		MaxThreads:      maxThreads,
		ConsumerGroupID: e.groupID,
		Dialer:          e.dialer,
		AdminClient:     e.admin,
		Membership:      membership,
	})
}

// seats returns instance id -> sorted assigned partitions, plus the group state.
func (e *brokerEnv) seats(t *testing.T) (map[string][]int32, string) {
	t.Helper()
	described, err := e.admin.DescribeGroups(context.Background(), e.groupID)
	require.NoError(t, err)
	group := described[e.groupID]
	require.NoError(t, group.Err)

	seats := make(map[string][]int32)
	for _, member := range group.Members {
		require.NotNil(t, member.InstanceID, "every member must be static")
		var partitions []int32
		if assignment, ok := member.Assigned.AsConsumer(); ok {
			for _, topic := range assignment.Topics {
				partitions = append(partitions, topic.Partitions...)
			}
		}
		slices.Sort(partitions)
		seats[*member.InstanceID] = partitions
	}
	return seats, group.State
}

func seatID(groupID string, readerIndex int) string {
	return fmt.Sprintf("%s-task-%d", groupID, readerIndex)
}

// TestMembershipReleaseAndRejoin covers the J2 flow: the master joins N seats and releases them,
// then one process per seat rejoins and gets the same partitions without a rebalance.
func TestMembershipReleaseAndRejoin(t *testing.T) {
	env := newBrokerEnv(t)
	ctx := context.Background()

	master := env.manager(2, GroupMembership{
		InstanceID:       func(readerIndex int) string { return seatID(env.groupID, readerIndex) },
		SessionTimeout:   2 * time.Minute,
		ReleaseAfterJoin: true,
	})
	require.NoError(t, master.CreateReaders(ctx, env.streams))
	masterGeneration := master.generationID.Load()
	planned, err := master.SeatAssignments(ctx)
	require.NoError(t, err)

	var masterSeats map[string][]int32
	t.Run("release keeps seats and assignment", func(t *testing.T) {
		var state string
		masterSeats, state = env.seats(t)

		assert.Equal(t, "Stable", state)
		require.Len(t, masterSeats, 2)
		require.Len(t, planned, 2)
		var all []int32
		for readerIndex := range 2 {
			partitions, ok := masterSeats[seatID(env.groupID, readerIndex)]
			require.True(t, ok, "seat %d missing after release", readerIndex)
			assert.NotEmpty(t, partitions)
			assert.Len(t, planned[seatID(env.groupID, readerIndex)], len(partitions), "SeatAssignments must match the group")
			all = append(all, partitions...)
		}
		slices.Sort(all)
		assert.Equal(t, []int32{0, 1, 2, 3}, all, "every partition assigned exactly once")

		assert.Equal(t, 2, master.GetReaderCount(), "reader count kept for MaxConnections")
		assert.Nil(t, master.GetReader(0))
		assert.Nil(t, master.GetReader(1))
	})

	// rejoin every seat one by one; one of them was the group leader
	for readerIndex := range 2 {
		seat := seatID(env.groupID, readerIndex)
		t.Run(fmt.Sprintf("pod rejoins %s without rebalance", seat), func(t *testing.T) {
			pod := env.manager(1, GroupMembership{
				InstanceID:         func(int) string { return seat },
				SessionTimeout:     2 * time.Minute,
				KeepMembers:        true,
				ExpectedAssignment: func(int) []types.PartitionKey { return planned[seat] },
			})
			require.NoError(t, pod.CreateReaders(ctx, env.streams))
			defer pod.ReleaseReaders()

			assert.Equal(t, masterGeneration, pod.generationID.Load(), "generation changed: a rebalance happened")

			seats, state := env.seats(t)
			assert.Equal(t, "Stable", state)
			assert.Equal(t, masterSeats, seats, "assignment changed after rejoin")

			// the reader keeps its id across a restart (StreamChanges does this on every attempt)
			restarted, err := pod.RestartReader(0)
			require.NoError(t, err)
			assert.Equal(t, seat, restarted.OptValue(kgo.InstanceID))
			assert.Equal(t, 2*time.Minute, restarted.OptValue(kgo.SessionTimeout))
		})
	}

	t.Run("generation unchanged after all rejoins", func(t *testing.T) {
		probe := env.manager(1, GroupMembership{
			InstanceID:     func(int) string { return seatID(env.groupID, 0) },
			SessionTimeout: 2 * time.Minute,
			KeepMembers:    true,
		})
		require.NoError(t, probe.CreateReaders(ctx, env.streams))
		defer probe.ReleaseReaders()
		assert.Equal(t, masterGeneration, probe.generationID.Load())
	})
}

// TestMembershipRejoinWithOwnTopics checks a pod can rejoin its seat subscribed only to the topics that seat holds,
// and that ExpectedAssignment fails fast when the seat no longer has the planned partitions.
func TestMembershipRejoinWithOwnTopics(t *testing.T) {
	env := newBrokerEnv(t, 1, 1) // two topics with one partition each, so each seat holds one topic
	ctx := context.Background()

	master := env.manager(2, GroupMembership{
		InstanceID:       func(readerIndex int) string { return seatID(env.groupID, readerIndex) },
		SessionTimeout:   2 * time.Minute,
		ReleaseAfterJoin: true,
	})
	require.NoError(t, master.CreateReaders(ctx, env.streams))
	masterGeneration := master.generationID.Load()
	planned, err := master.SeatAssignments(ctx)
	require.NoError(t, err)
	require.Len(t, planned, 2)

	pod := func(seat string, expected []types.PartitionKey) *ReaderManager {
		return env.manager(1, GroupMembership{
			InstanceID:         func(int) string { return seat },
			SessionTimeout:     2 * time.Minute,
			KeepMembers:        true,
			ExpectedAssignment: func(int) []types.PartitionKey { return expected },
		})
	}

	// one of the two seats was the group leader
	for readerIndex := range 2 {
		seat := seatID(env.groupID, readerIndex)
		t.Run(fmt.Sprintf("pod rejoins %s with only its own topics", seat), func(t *testing.T) {
			ownTopics := topicsOf(planned[seat])
			require.Len(t, ownTopics, 1, "each seat should hold one of the two topics")

			seatPod := pod(seat, planned[seat])
			require.NoError(t, seatPod.CreateReaders(ctx, streamsFor(ownTopics...)))
			defer seatPod.ReleaseReaders()

			assert.Equal(t, masterGeneration, seatPod.generationID.Load(), "generation changed: a rebalance happened")
			seats, err := seatPod.SeatAssignments(ctx)
			require.NoError(t, err)
			assert.Equal(t, planned, seats, "assignment changed after rejoin")
		})
	}

	t.Run("unexpected assignment fails fast", func(t *testing.T) {
		seat0, seat1 := seatID(env.groupID, 0), seatID(env.groupID, 1)
		wrongPod := pod(seat0, planned[seat1])
		defer wrongPod.ReleaseReaders()

		err := wrongPod.CreateReaders(ctx, env.streams)
		require.ErrorContains(t, err, "assignment changed after the seats were planned")
		assert.Equal(t, masterGeneration, wrongPod.generationID.Load(), "the failed check must not cause a rebalance")
	})
}

// TestMembershipDefaultBehavior checks the zero value still behaves as before:
// group_<ULID> ids, readers stay open, restart keeps the id, cleanup removes all members.
func TestMembershipDefaultBehavior(t *testing.T) {
	env := newBrokerEnv(t)
	ctx := context.Background()

	manager := env.manager(2, GroupMembership{})
	require.NoError(t, manager.CreateReaders(ctx, env.streams))
	require.Equal(t, 2, manager.GetReaderCount())

	for readerIndex := range 2 {
		readerID, _ := manager.GetReaderIDAndClientID(readerIndex)
		assert.True(t, strings.HasPrefix(readerID, "group_"), "default id must be group_<ULID>, got %s", readerID)
		require.NotNil(t, manager.GetReader(readerIndex), "readers must stay open without ReleaseAfterJoin")
		assert.Equal(t, 45*time.Second, manager.GetReader(readerIndex).OptValue(kgo.SessionTimeout))
	}

	readerID, _ := manager.GetReaderIDAndClientID(0)
	restarted, err := manager.RestartReader(0)
	require.NoError(t, err)
	assert.Equal(t, readerID, restarted.OptValue(kgo.InstanceID))

	seats, _ := env.seats(t)
	assert.Len(t, seats, 2)

	// the previous run's readers are gone (closed static members keep their seats);
	// the next run's PreCDC cleanup must remove those leftover seats
	manager.ReleaseReaders()
	nextRun := env.manager(2, GroupMembership{})
	require.NoError(t, nextRun.RemoveExistingConsumers(ctx, env.client))
	seats, _ = env.seats(t)
	assert.Empty(t, seats, "default cleanup must remove every member")
}
