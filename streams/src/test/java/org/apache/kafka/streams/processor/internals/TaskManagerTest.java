/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.easymock.EasyMock.checkOrder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EasyMockRunner.class)
public class TaskManagerTest {

    private final TaskId taskId0 = new TaskId(0, 0);
    private final TopicPartition t1p0 = new TopicPartition("t1", 0);
    private final Set<TopicPartition> taskId0Partitions = Utils.mkSet(t1p0);
    private final Map<TaskId, Set<TopicPartition>> taskId0Assignment = Collections.singletonMap(taskId0, taskId0Partitions);

    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder.SubscriptionUpdates subscriptionUpdates;
    @Mock(type = MockType.STRICT)
    private InternalTopologyBuilder topologyBuilder;
    @Mock(type = MockType.STRICT)
    private StateDirectory stateDirectory;
    @Mock(type = MockType.NICE)
    private ChangelogReader changeLogReader;
    @Mock(type = MockType.NICE)
    private StreamsMetadataState streamsMetadataState;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> restoreConsumer;
    @Mock(type = MockType.NICE)
    private Consumer<byte[], byte[]> consumer;
    @Mock(type = MockType.NICE)
    private StreamThread.AbstractTaskCreator<StreamTask> activeTaskCreator;
    @Mock(type = MockType.NICE)
    private StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;
    @Mock(type = MockType.NICE)
    private AdminClient adminClient;
    @Mock(type = MockType.NICE)
    private StreamTask streamTask;
    @Mock(type = MockType.NICE)
    private StandbyTask standbyTask;
    @Mock(type = MockType.NICE)
    private AssignedStreamsTasks active;
    @Mock(type = MockType.NICE)
    private AssignedStandbyTasks standby;

    private TaskManager taskManager;

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final TopicPartition t1p1 = new TopicPartition(topic1, 1);
    private final TopicPartition t1p2 = new TopicPartition(topic1, 2);
    private final TopicPartition t1p3 = new TopicPartition(topic1, 3);
    private final TopicPartition t2p1 = new TopicPartition(topic2, 1);
    private final TopicPartition t2p2 = new TopicPartition(topic2, 2);
    private final TopicPartition t2p3 = new TopicPartition(topic2, 3);

    private final TaskId task01 = new TaskId(0, 1);
    private final TaskId task02 = new TaskId(0, 2);
    private final TaskId task03 = new TaskId(0, 3);
    private final TaskId task11 = new TaskId(1, 1);

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        taskManager = new TaskManager(changeLogReader,
                                      UUID.randomUUID(),
                                      "",
                                      restoreConsumer,
                                      streamsMetadataState,
                                      activeTaskCreator,
                                      standbyTaskCreator,
                                      adminClient,
                                      active,
                                      standby);
        taskManager.setConsumer(consumer);
    }

    private void replay() {
        EasyMock.replay(changeLogReader,
                        restoreConsumer,
                        consumer,
                        activeTaskCreator,
                        standbyTaskCreator,
                        active,
                        standby,
                        adminClient);
    }

    @Test
    public void shouldUpdateSubscriptionFromAssignment() {
        mockTopologyBuilder();
        expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));
        topologyBuilder.updateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), EasyMock.anyString());
        expectLastCall().once();

        EasyMock.replay(activeTaskCreator,
                        topologyBuilder,
                        subscriptionUpdates);

        taskManager.updateSubscriptionsFromAssignment(asList(t1p1, t2p1));

        EasyMock.verify(activeTaskCreator,
                        topologyBuilder,
                        subscriptionUpdates);
    }

    @Test
    public void shouldNotUpdateSubscriptionFromAssignment() {
        mockTopologyBuilder();
        expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1, topic2));

        EasyMock.replay(activeTaskCreator,
                        topologyBuilder,
                        subscriptionUpdates);

        taskManager.updateSubscriptionsFromAssignment(asList(t1p1));

        EasyMock.verify(activeTaskCreator,
                        topologyBuilder,
                        subscriptionUpdates);
    }

    @Test
    public void shouldUpdateSubscriptionFromMetadata() {
        mockTopologyBuilder();
        expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));
        topologyBuilder.updateSubscribedTopics(EasyMock.eq(Utils.mkSet(topic1, topic2)), EasyMock.anyString());
        expectLastCall().once();

        EasyMock.replay(activeTaskCreator,
                topologyBuilder,
                subscriptionUpdates);

        taskManager.updateSubscriptionsFromMetadata(Utils.mkSet(topic1, topic2));

        EasyMock.verify(activeTaskCreator,
                topologyBuilder,
                subscriptionUpdates);
    }

    @Test
    public void shouldNotUpdateSubscriptionFromMetadata() {
        mockTopologyBuilder();
        expect(subscriptionUpdates.getUpdates()).andReturn(Utils.mkSet(topic1));

        EasyMock.replay(activeTaskCreator,
                topologyBuilder,
                subscriptionUpdates);

        taskManager.updateSubscriptionsFromMetadata(Utils.mkSet(topic1));

        EasyMock.verify(activeTaskCreator,
                topologyBuilder,
                subscriptionUpdates);
    }

    @Test
    public void shouldReturnCachedTaskIdsFromDirectory() throws IOException {
        final File[] taskFolders = asList(testFolder.newFolder("0_1"),
                                                testFolder.newFolder("0_2"),
                                                testFolder.newFolder("0_3"),
                                                testFolder.newFolder("1_1"),
                                                testFolder.newFolder("dummy")).toArray(new File[0]);

        assertTrue((new File(taskFolders[0], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());
        assertTrue((new File(taskFolders[1], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());
        assertTrue((new File(taskFolders[3], StateManagerUtil.CHECKPOINT_FILE_NAME)).createNewFile());

        expect(activeTaskCreator.stateDirectory()).andReturn(stateDirectory).once();
        expect(stateDirectory.listTaskDirectories()).andReturn(taskFolders).once();

        EasyMock.replay(activeTaskCreator, stateDirectory);

        final Set<TaskId> tasks = taskManager.cachedTasksIds();

        EasyMock.verify(activeTaskCreator, stateDirectory);

        assertThat(tasks, equalTo(Utils.mkSet(task01, task02, task11)));
    }

    @Test
    public void shouldCloseActiveUnAssignedSuspendedTasksWhenCreatingNewTasks() {
        mockSingleActiveTask();
        active.closeNonAssignedSuspendedTasks(taskId0Assignment);
        expectLastCall();
        replay();

        taskManager.setAssignmentMetadata(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        taskManager.createTasks(taskId0Partitions);

        verify(active);
    }

    @Test
    public void shouldCloseStandbyUnAssignedSuspendedTasksWhenCreatingNewTasks() {
        mockSingleActiveTask();
        standby.closeNonAssignedSuspendedTasks(taskId0Assignment);
        expectLastCall();
        replay();

        taskManager.setAssignmentMetadata(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        taskManager.createTasks(taskId0Partitions);

        verify(active);
    }

    @Test
    public void shouldAddNonResumedActiveTasks() {
        mockSingleActiveTask();
        expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
        active.addNewTask(EasyMock.same(streamTask));
        replay();

        taskManager.setAssignmentMetadata(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        taskManager.createTasks(taskId0Partitions);

        verify(activeTaskCreator, active);
    }

    @Test
    public void shouldNotAddResumedActiveTasks() {
        checkOrder(active, true);
        expect(active.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
        replay();

        taskManager.setAssignmentMetadata(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        taskManager.createTasks(taskId0Partitions);

        // should be no calls to activeTaskCreator and no calls to active.addNewTasks(..)
        verify(active, activeTaskCreator);
    }

    @Test
    public void shouldAddNonResumedStandbyTasks() {
        mockStandbyTaskExpectations();
        expect(standby.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(false);
        standby.addNewTask(EasyMock.same(standbyTask));
        replay();

        taskManager.setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), taskId0Assignment);
        taskManager.createTasks(taskId0Partitions);

        verify(standbyTaskCreator, active);
    }

    @Test
    public void shouldNotAddResumedStandbyTasks() {
        checkOrder(active, true);
        expect(standby.maybeResumeSuspendedTask(taskId0, taskId0Partitions)).andReturn(true);
        replay();

        taskManager.setAssignmentMetadata(Collections.<TaskId, Set<TopicPartition>>emptyMap(), taskId0Assignment);
        taskManager.createTasks(taskId0Partitions);

        // should be no calls to standbyTaskCreator and no calls to standby.addNewTasks(..)
        verify(standby, standbyTaskCreator);
    }

    @Test
    public void shouldPauseActivePartitions() {
        mockSingleActiveTask();
        expect(consumer.assignment()).andReturn(taskId0Partitions).times(2);
        consumer.pause(taskId0Partitions);
        expectLastCall();
        replay();

        taskManager.setAssignmentMetadata(taskId0Assignment, Collections.<TaskId, Set<TopicPartition>>emptyMap());
        taskManager.createTasks(taskId0Partitions);
        verify(consumer);
    }

    @Test
    public void shouldSuspendActiveTasks() {
        expect(active.suspend()).andReturn(null);
        replay();

        taskManager.suspendTasksAndState();
        verify(active);
    }

    @Test
    public void shouldSuspendStandbyTasks() {
        expect(standby.suspend()).andReturn(null);
        replay();

        taskManager.suspendTasksAndState();
        verify(standby);
    }

    @Test
    public void shouldUnassignChangelogPartitionsOnSuspend() {
        restoreConsumer.unsubscribe();
        expectLastCall();
        replay();

        taskManager.suspendTasksAndState();
        verify(restoreConsumer);
    }

    @Test
    public void shouldThrowStreamsExceptionAtEndIfExceptionDuringSuspend() {
        expect(active.suspend()).andReturn(new RuntimeException(""));
        expect(standby.suspend()).andReturn(new RuntimeException(""));
        expectLastCall();
        restoreConsumer.unsubscribe();

        replay();
        try {
            taskManager.suspendTasksAndState();
            fail("Should have thrown streams exception");
        } catch (final StreamsException e) {
            // expected
        }
        verify(restoreConsumer, active, standby);
    }

    @Test
    public void shouldCloseActiveTasksOnShutdown() {
        active.close(true);
        expectLastCall();
        replay();

        taskManager.shutdown(true);
        verify(active);
    }

    @Test
    public void shouldCloseStandbyTasksOnShutdown() {
        standby.close(false);
        expectLastCall();
        replay();

        taskManager.shutdown(false);
        verify(standby);
    }

    @Test
    public void shouldUnassignChangelogPartitionsOnShutdown() {
        restoreConsumer.unsubscribe();
        expectLastCall();
        replay();

        taskManager.shutdown(true);
        verify(restoreConsumer);
    }

    @Test
    public void shouldInitializeNewActiveTasks() {
        active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject());
        expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(active);
    }

    @Test
    public void shouldInitializeNewStandbyTasks() {
        active.updateRestored(EasyMock.<Collection<TopicPartition>>anyObject());
        expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(standby);
    }

    @Test
    public void shouldRestoreStateFromChangeLogReader() {
        expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
        active.updateRestored(taskId0Partitions);
        expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(changeLogReader, active);
    }

    @Test
    public void shouldResumeRestoredPartitions() {
        expect(changeLogReader.restore(active)).andReturn(taskId0Partitions);
        expect(active.allTasksRunning()).andReturn(true);
        expect(consumer.assignment()).andReturn(taskId0Partitions);
        expect(standby.running()).andReturn(Collections.<StandbyTask>emptySet());

        consumer.resume(taskId0Partitions);
        expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(consumer);
    }

    @Test
    public void shouldAssignStandbyPartitionsWhenAllActiveTasksAreRunning() {
        mockAssignStandbyPartitions(1L);
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(restoreConsumer);
    }

    @Test
    public void shouldReturnTrueWhenActiveAndStandbyTasksAreRunning() {
        mockAssignStandbyPartitions(1L);
        expect(standby.allTasksRunning()).andReturn(true);
        replay();

        assertTrue(taskManager.updateNewAndRestoringTasks());
    }

    @Test
    public void shouldReturnFalseWhenOnlyActiveTasksAreRunning() {
        mockAssignStandbyPartitions(1L);
        expect(standby.allTasksRunning()).andReturn(false);
        replay();

        assertFalse(taskManager.updateNewAndRestoringTasks());
    }

    @Test
    public void shouldReturnFalseWhenThereAreStillNonRunningTasks() {
        expect(active.allTasksRunning()).andReturn(false);
        replay();

        assertFalse(taskManager.updateNewAndRestoringTasks());
    }

    @Test
    public void shouldSeekToCheckpointedOffsetOnStandbyPartitionsWhenOffsetGreaterThanEqualTo0() {
        mockAssignStandbyPartitions(1L);
        restoreConsumer.seek(t1p0, 1L);
        expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(restoreConsumer);
    }

    @Test
    public void shouldSeekToBeginningIfOffsetIsLessThan0() {
        mockAssignStandbyPartitions(-1L);
        restoreConsumer.seekToBeginning(taskId0Partitions);
        expectLastCall();
        replay();

        taskManager.updateNewAndRestoringTasks();
        verify(restoreConsumer);
    }

    @Test
    public void shouldCommitActiveAndStandbyTasks() {
        expect(active.commit()).andReturn(1);
        expect(standby.commit()).andReturn(2);

        replay();

        assertThat(taskManager.commitAll(), equalTo(3));
        verify(active, standby);
    }

    @Test
    public void shouldPropagateExceptionFromActiveCommit() {
        // upgrade to strict mock to ensure no calls
        checkOrder(standby, true);
        active.commit();
        expectLastCall().andThrow(new RuntimeException(""));
        replay();

        try {
            taskManager.commitAll();
            fail("should have thrown first exception");
        } catch (final Exception e) {
            // ok
        }
        verify(active, standby);
    }

    @Test
    public void shouldPropagateExceptionFromStandbyCommit() {
        expect(standby.commit()).andThrow(new RuntimeException(""));
        replay();

        try {
            taskManager.commitAll();
            fail("should have thrown exception");
        } catch (final Exception e) {
            // ok
        }
        verify(standby);
    }

    @Test
    public void shouldSendPurgeData() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final Map<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>) futureDeletedRecords));

        futureDeletedRecords.complete(null);

        expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
        expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).times(2);
        replay();

        taskManager.maybePurgeCommitedRecords();
        taskManager.maybePurgeCommitedRecords();
        verify(active, adminClient);
    }

    @Test
    public void shouldNotSendPurgeDataIfPreviousNotDone() {
        final KafkaFuture<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final Map<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, futureDeletedRecords));

        expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).once();
        expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).once();
        replay();

        taskManager.maybePurgeCommitedRecords();
        // second call should be no-op as the previous one is not done yet
        taskManager.maybePurgeCommitedRecords();
        verify(active, adminClient);
    }

    @Test
    public void shouldIgnorePurgeDataErrors() {
        final KafkaFutureImpl<DeletedRecords> futureDeletedRecords = new KafkaFutureImpl<>();
        final Map<TopicPartition, RecordsToDelete> recordsToDelete = Collections.singletonMap(t1p1, RecordsToDelete.beforeOffset(5L));
        final DeleteRecordsResult deleteRecordsResult = new DeleteRecordsResult(Collections.singletonMap(t1p1, (KafkaFuture<DeletedRecords>) futureDeletedRecords));

        futureDeletedRecords.completeExceptionally(new Exception("KABOOM!"));

        expect(active.recordsToDelete()).andReturn(Collections.singletonMap(t1p1, 5L)).times(2);
        expect(adminClient.deleteRecords(recordsToDelete)).andReturn(deleteRecordsResult).times(2);
        replay();

        taskManager.maybePurgeCommitedRecords();
        taskManager.maybePurgeCommitedRecords();
        verify(active, adminClient);
    }

    @Test
    public void shouldMaybeCommitActiveTasks() {
        expect(active.maybeCommitPerUserRequested()).andReturn(5);
        replay();

        assertThat(taskManager.maybeCommitActiveTasksPerUserRequested(), equalTo(5));
        verify(active);
    }

    @Test
    public void shouldProcessActiveTasks() {
        expect(active.process(0L)).andReturn(10);
        replay();

        assertThat(taskManager.process(0L), equalTo(10));
        verify(active);
    }

    @Test
    public void shouldPunctuateActiveTasks() {
        expect(active.punctuate()).andReturn(20);
        replay();

        assertThat(taskManager.punctuate(), equalTo(20));
        verify(active);
    }

    @Test
    public void shouldNotResumeConsumptionUntilAllStoresRestored() {
        expect(active.allTasksRunning()).andReturn(false);
        final Consumer<byte[], byte[]> consumer = EasyMock.createStrictMock(Consumer.class);
        taskManager.setConsumer(consumer);
        EasyMock.replay(active, consumer);

        // shouldn't invoke `resume` method in consumer
        taskManager.updateNewAndRestoringTasks();
        EasyMock.verify(consumer);
    }

    @Test
    public void shouldUpdateTasksFromPartitionAssignment() {
        final Map<TaskId, Set<TopicPartition>> activeTasks = new HashMap<>();
        final Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        taskManager.setAssignmentMetadata(activeTasks, standbyTasks);
        assertTrue(taskManager.assignedActiveTasks().isEmpty());

        // assign two active tasks with two partitions each
        activeTasks.put(task01, new HashSet<>(asList(t1p1, t2p1)));
        activeTasks.put(task02, new HashSet<>(asList(t1p2, t2p2)));

        // assign one standby task with two partitions
        standbyTasks.put(task03, new HashSet<>(asList(t1p3, t2p3)));
        taskManager.setAssignmentMetadata(activeTasks, standbyTasks);

        assertThat(taskManager.assignedActiveTasks(), equalTo(activeTasks));
        assertThat(taskManager.assignedStandbyTasks(), equalTo(standbyTasks));
    }

    private void mockAssignStandbyPartitions(final long offset) {
        final StandbyTask task = EasyMock.createNiceMock(StandbyTask.class);
        expect(active.allTasksRunning()).andReturn(true);
        expect(standby.running()).andReturn(Collections.singletonList(task));
        expect(task.checkpointedOffsets()).andReturn(Collections.singletonMap(t1p0, offset));
        restoreConsumer.assign(taskId0Partitions);

        expectLastCall();
        EasyMock.replay(task);
    }

    private void mockStandbyTaskExpectations() {
        expect(standbyTaskCreator.createTasks(EasyMock.<Consumer<byte[], byte[]>>anyObject(),
                                                   EasyMock.eq(taskId0Assignment)))
                .andReturn(Collections.singletonList(standbyTask));

    }

    private void mockSingleActiveTask() {
        expect(activeTaskCreator.createTasks(EasyMock.<Consumer<byte[], byte[]>>anyObject(),
                                                  EasyMock.eq(taskId0Assignment)))
                .andReturn(Collections.singletonList(streamTask));

    }

    private void mockTopologyBuilder() {
        expect(activeTaskCreator.builder()).andReturn(topologyBuilder).anyTimes();
        expect(topologyBuilder.sourceTopicPattern()).andReturn(Pattern.compile("abc"));
        expect(topologyBuilder.subscriptionUpdates()).andReturn(subscriptionUpdates);
    }
}
