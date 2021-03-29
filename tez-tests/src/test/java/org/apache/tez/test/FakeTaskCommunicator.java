package org.apache.tez.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.TezLocalTaskCommunicatorImpl;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.events.TaskAttemptCompletedEvent;
import org.apache.tez.runtime.api.events.TaskAttemptFailedEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskCommunicatorContext;
import org.apache.tez.serviceplugins.api.TaskHeartbeatRequest;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This communicator does not send/receive any messages. It can
 * be triggered from {@link FakeContainerLauncher} to simulate heartbeats
 * from any task attempt.
 */
public class FakeTaskCommunicator extends TezLocalTaskCommunicatorImpl {

  public static final String COMMUNICATOR_NAME_CONFIG_KEY = "test.custom.communicator.name";

  private final int maximumFailedAttempts;
  private Map<ContainerId, TaskSpec> containerMap = new ConcurrentHashMap<>();

  public FakeTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext) throws IOException {
    super(taskCommunicatorContext);
    Configuration conf = TezUtils.createConfFromUserPayload(taskCommunicatorContext.getInitialUserPayload());
    maximumFailedAttempts = conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 0);
  }

  @Override
  public void registerRunningContainer(ContainerId containerId, String hostname, int port) {

  }

  @Override
  public void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason, @Nullable String diagnostics) {
    containerMap.remove(containerId);
  }

  @Override
  public void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec, Map<String, LocalResource> additionalResources, Credentials credentials, boolean credentialsChanged, int priority) {
    getContext().taskSubmitted(taskSpec.getTaskAttemptID(), containerId);
    getContext().taskStartedRemotely(taskSpec.getTaskAttemptID());
    containerMap.put(containerId, taskSpec);
  }

  @Override
  public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID, TaskAttemptEndReason endReason, @Nullable String diagnostics) {
  }


  @Override
  public void onVertexStateUpdated(VertexStateUpdate stateUpdate) {

  }

  @Override
  public void dagComplete(int dagIdentifier)  {

  }

  /**
   * Return task communicator's context. This way we can get it from
   * container launcher and fake heartbeats from tasks.
   * @return
   * @throws ServicePluginException
   */
  @Override
  public Object getMetaInfo() {
    return this;
  }

  public boolean heartbeatEnd(ContainerId containerId, boolean intentToSucceed) throws IOException, TezException {
    TaskSpec taskSpec = containerMap.get(containerId);
    boolean success = true;
    if (!intentToSucceed && taskSpec.getTaskAttemptID().getId() <= maximumFailedAttempts) {
      // we do not want to fail the task altogether, only some task attempts
      success = false;
    }
    EventMetaData metaData = new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR,
        "vertexName", null, taskSpec.getTaskAttemptID());
    Event event;
    if (success) {
      event = new TaskAttemptCompletedEvent();
    } else {
      event = new TaskAttemptFailedEvent("", TaskFailureType.NON_FATAL);
    }
    List<TezEvent> events = Collections.singletonList(new TezEvent(event, metaData));
    TaskHeartbeatRequest request = new TaskHeartbeatRequest(containerId.toString(),
        taskSpec.getTaskAttemptID(), events, 0, 0, 0);
    getContext().heartbeat(request);
    return success;
  }
}
