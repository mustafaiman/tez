/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.Progress;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.serviceplugins.api.ContainerLauncherDescriptor;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.apache.tez.serviceplugins.api.TaskSchedulerDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ConcurrentTasksStressTest {

  private static final String SCHEDULER_NAME = "stressedScheduler";
  private static final String LAUNCHER_NAME = "lazyLauncher";
  private static final String COMMUNICATOR_NAME = "fakeCommunicator";
  private static final int FAILING_TASK_ATTEMPT_PERCENT = 3;
  private static final int NUMBER_OF_EXECUTION_SLOTS = 220;

  private final boolean useBlockingScheduler;

  private TezConfiguration tezConf;

  @Parameterized.Parameters(name = "useBlockingScheduler:{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public ConcurrentTasksStressTest(boolean useBlockingScheduler) {
    this.useBlockingScheduler = useBlockingScheduler;
  }

  @Before
  public void setup() {
    tezConf = new TezConfiguration();
    tezConf.setInt(TezConfiguration.TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS, NUMBER_OF_EXECUTION_SLOTS);
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf.setInt(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 1500);
  }

  @Test(timeout = 400000)
  public void testSuccessWithFailedTaskAttempts() throws TezException, InterruptedException, IOException {
    final String vertexName = "vertex1";
    final String dagName = "dag1";
    final int taskCount = 10000;
    TezClient tezClient = TezClient.newBuilder("stressedTezClient", tezConf)
        .setServicePluginDescriptor(createPluginsDescriptor())
        .setIsSession(true)
        .build();
    tezClient.start();

    DAG dag = createDagWithSingleVertex(dagName, vertexName, taskCount);

    tezClient.waitTillReady();
    DAGClient dagClient = tezClient.submitDAG(dag);
    dagClient.waitForCompletionWithStatusUpdates(null);
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    Progress progress = dagClient.getVertexStatus(vertexName, null).getProgress();
    assertEquals(taskCount, progress.getSucceededTaskCount());
    assertEquals(0, progress.getFailedTaskCount());
    assertEquals(0, progress.getKilledTaskCount());
    assertTrue("Some task attempts should have failed", progress.getFailedTaskAttemptCount() > 0);

    dagClient.close();
    tezClient.stop();
  }

  @Test(timeout = 400000)
  public void testWithDagRestarts() throws TezException, InterruptedException, IOException {
    final String vertexName = "vertex1";
    final String dagName = "dag1";
    final int taskCount = 1000;
    TezClient tezClient = TezClient.newBuilder("stressedTezClient", tezConf)
        .setServicePluginDescriptor(createPluginsDescriptor())
        .setIsSession(true)
        .build();
    tezClient.start();

    DAG dag = createDagWithSingleVertex(dagName, vertexName, taskCount);

    tezClient.waitTillReady();
    for (int i = 0; i < 3; i++) {
      DAGClient dagClient = tezClient.submitDAG(dag);
      Thread.sleep(700);
      dagClient.tryKillDAG();
      dagClient.waitForCompletionWithStatusUpdates(null);
      assertKilled(dagClient, vertexName);
      dagClient.close();

      dagClient = tezClient.submitDAG(dag);
      dagClient.waitForCompletionWithStatusUpdates(null);
      dagClient.close();
      assertSucceeded(dagClient, vertexName, taskCount);
      dagClient.close();
    }

    tezClient.stop();
  }

  private void assertSucceeded(DAGClient dagClient, String vertexName, int taskCount) throws IOException, TezException {
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
    Progress progress = dagClient.getVertexStatus(vertexName, null).getProgress();
    assertEquals(taskCount, progress.getSucceededTaskCount());
    assertEquals(0, progress.getFailedTaskCount());
    assertEquals(0, progress.getKilledTaskCount());
  }

  private void assertKilled(DAGClient dagClient, String vertexName) throws IOException, TezException {
    assertEquals(DAGStatus.State.KILLED, dagClient.getDAGStatus(null).getState());
    Progress progress = dagClient.getVertexStatus(vertexName, null).getProgress();
    assertTrue(0 < progress.getKilledTaskCount());
  }

  private ServicePluginsDescriptor createPluginsDescriptor() throws IOException {
    ServicePluginsDescriptor spd = ServicePluginsDescriptor.create(true,
        createTaskSchedulerDescriptor(),
        createContainerLauncherDescriptor(),
        createTaskCommunicatorDescriptor());
    return spd;
  }

  private TaskSchedulerDescriptor[] createTaskSchedulerDescriptor() throws IOException {
    Configuration schedulerConf = new Configuration(tezConf);
    UserPayload payload = TezUtils.createUserPayloadFromConf(schedulerConf);
    TaskSchedulerDescriptor stressedSchedulerDescriptor = TaskSchedulerDescriptor.create(SCHEDULER_NAME, RandomlyDelayingTaskScheduler.class.getName());
    stressedSchedulerDescriptor.setUserPayload(payload);
    TaskSchedulerDescriptor[] schedulerArray = new TaskSchedulerDescriptor[] { stressedSchedulerDescriptor};
    return schedulerArray;
  }

  private ContainerLauncherDescriptor[] createContainerLauncherDescriptor() throws IOException {
    Configuration conf = new Configuration(tezConf);
    conf.set(FakeTaskCommunicator.COMMUNICATOR_NAME_CONFIG_KEY, COMMUNICATOR_NAME);
    conf.setInt(FakeContainerLauncher.TASK_ATTEMPT_FAILURE_PERCENT_CONFIG_KEY, FAILING_TASK_ATTEMPT_PERCENT);
    conf.setInt(FakeContainerLauncher.TASK_ATTEMPT_DURATION_CONFIG_KEY, 600);
    conf.setInt(FakeContainerLauncher.TASK_ATTEMPT_DURATION_JITTER_CONFIG_KEY, 400);
    conf.setInt(FakeContainerLauncher.TASK_ATTEMPT_STRAGGLER_DURATION_CONFIG_KEY, 1000);
    conf.setDouble(FakeContainerLauncher.TASK_ATTEMPT_STRAGGLER_RATIO_CONFIG_KEY, 0.02);
    UserPayload payload = TezUtils.createUserPayloadFromConf(conf);
    ContainerLauncherDescriptor descriptor = ContainerLauncherDescriptor.create(LAUNCHER_NAME, FakeContainerLauncher.class.getName());
    descriptor.setUserPayload(payload);
    ContainerLauncherDescriptor[] descriptorArray = new ContainerLauncherDescriptor[] {descriptor};
    return descriptorArray;
  }

  private TaskCommunicatorDescriptor[] createTaskCommunicatorDescriptor() throws IOException {
    UserPayload payload = TezUtils.createUserPayloadFromConf(tezConf);
    TaskCommunicatorDescriptor descriptor = TaskCommunicatorDescriptor.create(COMMUNICATOR_NAME, FakeTaskCommunicator.class.getName());
    descriptor.setUserPayload(payload);
    TaskCommunicatorDescriptor[] descriptorArray = new TaskCommunicatorDescriptor[] {descriptor};
    return descriptorArray;
  }

  /**
   * This test uses fake container launcher and task communicator. Container launcher
   * and task scheduler imitates task start, run, success and fail events. So we do
   * not need an actual processor. We just use dummy {@link NoProcessor}.
   * @param dagName
   * @param vertexName
   * @param parallelism
   * @return
   */
  private DAG createDagWithSingleVertex(String dagName, String vertexName, int parallelism) {
    DAG dag = DAG.create(dagName)
        .addVertex(Vertex.create(vertexName, ProcessorDescriptor.create(NoProcessor.class.getName()), parallelism));
    if (useBlockingScheduler) {
      dag.setExecutionContext(Vertex.VertexExecutionContext.create(SCHEDULER_NAME, LAUNCHER_NAME, COMMUNICATOR_NAME));
    } else {
      dag.setExecutionContext(Vertex.VertexExecutionContext.create(null, LAUNCHER_NAME, COMMUNICATOR_NAME));
    }
    return dag;
  }

  /**
   * An absolutely dummy processor. It is just to trick Tez to run usual
   * state machines when we simulate the events coming from the taskattempts.
   */
  public static class NoProcessor extends AbstractLogicalIOProcessor {

    public NoProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {

    }
  }
}
