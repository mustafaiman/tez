package org.apache.tez.test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This launcher schedules a success or fail response for a later time instead of actually
 * launching containers and running tasks. It pairs with {@link FakeTaskCommunicator} to
 * create heartbeats when a task succeeds or fails. It can imitate running tens of thousands
 * of tasks. So we can test the load of a large query on Dag/Vertez/Task state machines.
 */
public class FakeContainerLauncher extends ContainerLauncher {

  public static final String TASK_ATTEMPT_FAILURE_PERCENT_CONFIG_KEY = "test.custom.container.failure.percent";
  public static final String TASK_ATTEMPT_DURATION_CONFIG_KEY = "test.custom.launcher.attempt.duration";
  public static final String TASK_ATTEMPT_DURATION_JITTER_CONFIG_KEY = "test.custom.launcher.attempt.duration.jitter";
  public static final String TASK_ATTEMPT_STRAGGLER_DURATION_CONFIG_KEY = "test.custom.launcher.straggler.duration";
  public static final String TASK_ATTEMPT_STRAGGLER_RATIO_CONFIG_KEY = "test.custom.launcher.straggler.ratio";

  /**
   * Number of concurrently running tasks does not depend on this thread count.
   * However, we still use multiple threads for scheduler responses so we
   * better test concurrent responses from task attempts.
   */
  private static final int RESPONSE_THREAD_COUNT = 10;

  /**
   * We need to know the communicator so we can trigger heartbeats when we want.
   */
  private final String communicatorName;
  private final int failurePercent;
  private final int attemptDuration;
  private final int attemptDurationJitter;
  private final int stragglerDuration;
  private final double stragglerRatio;

  private FakeTaskCommunicator taskCommunicator;
  private Random random = new Random();

  private ScheduledExecutorService responseExecutor =
      Executors.newScheduledThreadPool(RESPONSE_THREAD_COUNT, new ThreadFactoryBuilder().setDaemon(true).build());

  public FakeContainerLauncher(ContainerLauncherContext containerLauncherContext) throws IOException {
    super(containerLauncherContext);
    UserPayload payload = containerLauncherContext.getInitialUserPayload();
    Configuration configuration = TezUtils.createConfFromUserPayload(payload);
    this.communicatorName = configuration.get(FakeTaskCommunicator.COMMUNICATOR_NAME_CONFIG_KEY);
    this.failurePercent = configuration.getInt(TASK_ATTEMPT_FAILURE_PERCENT_CONFIG_KEY, 0);
    this.attemptDuration = configuration.getInt(TASK_ATTEMPT_DURATION_CONFIG_KEY, 0);
    this.attemptDurationJitter = configuration.getInt(TASK_ATTEMPT_DURATION_JITTER_CONFIG_KEY, 0);
    this.stragglerDuration = configuration.getInt(TASK_ATTEMPT_STRAGGLER_DURATION_CONFIG_KEY, 0);
    this.stragglerRatio = configuration.getDouble(TASK_ATTEMPT_STRAGGLER_RATIO_CONFIG_KEY, 0.0);
  }

  @Override
  public void start() throws Exception {
    super.start();
    taskCommunicator = (FakeTaskCommunicator) getContext().getTaskCommunicatorMetaInfo(communicatorName);
  }

  @Override
  public void launchContainer(ContainerLaunchRequest launchRequest) {
    getContext().containerLaunched(launchRequest.getContainerId());
    schedule(launchRequest);
  }

  @Override
  public void stopContainer(ContainerStopRequest stopRequest) {
    // nothing to do
  }

  private void schedule(ContainerLaunchRequest launchRequest) {
    boolean success = mustSucceed();
    int runtime = calculateRunTime();
    responseExecutor.schedule(new Responder(getContext(), taskCommunicator, launchRequest.getContainerId(), success),
        runtime, TimeUnit.MILLISECONDS);
  }

  private boolean mustSucceed() {
    return random.nextInt(100) >= failurePercent;
  }

  private int calculateRunTime() {
    int runtime = attemptDuration + random.nextInt(attemptDurationJitter);
    // we get a few stragglers
    if (random.nextDouble() < stragglerRatio) {
      runtime += stragglerDuration;
    }
    return runtime;
  }

  private static class Responder implements Runnable {

    private final ContainerLauncherContext launcherContext;
    private final FakeTaskCommunicator taskCommunicator;
    private final ContainerId containerId;
    private final boolean intentToSucceed;

    /**
     *
     * @param launcherContext
     * @param taskCommunicator
     * @param containerId
     * @param intentToSucceed when {@code true} the task attempt succeeds. Otherwise
     *                        the task attempt fails unless it is the 4th attempt.
     */
    public Responder(ContainerLauncherContext launcherContext, FakeTaskCommunicator taskCommunicator,
                     ContainerId containerId, boolean intentToSucceed) {
      this.launcherContext = launcherContext;
      this.taskCommunicator = taskCommunicator;
      this.containerId = containerId;
      this.intentToSucceed = intentToSucceed;
    }

    @Override
    public void run() {
      try {
        boolean success = taskCommunicator.heartbeatEnd(containerId, intentToSucceed);
        if (success) {
          launcherContext.containerCompleted(containerId, 0, null, TaskAttemptEndReason.CONTAINER_EXITED);
        } else {
          launcherContext.containerCompleted(containerId, 1, null, TaskAttemptEndReason.OTHER);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (TezException e) {
        e.printStackTrace();
      }
    }
  }
}
