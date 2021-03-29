package org.apache.tez.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.app.rm.LocalTaskSchedulerService;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;

import java.io.IOException;
import java.util.Random;

/**
 * This scheduler randomly blocks during allocation calls.
 * It simulates a struggling scheduler.
 */
public class RandomlyDelayingTaskScheduler extends LocalTaskSchedulerService {

  private static final int BLOCKING_CALL_PERCENT = 1;
  private static final int BLOCK_WAIT_AMOUNT = 500;
  private static final int BLOCK_WAIT_JITTER = 400;

  private final Random random = new Random();

  public RandomlyDelayingTaskScheduler(TaskSchedulerContext taskSchedulerContext) throws IOException {
    super(taskSchedulerContext);
    Configuration conf = TezUtils.createConfFromUserPayload(taskSchedulerContext.getInitialUserPayload());
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks, Priority priority,
                           Object containerSignature, Object clientCookie) {
    maybeBlock();
    super.allocateTask(task, capability, hosts, racks, priority, containerSignature, clientCookie);
  }

  @Override
  public void allocateTask(Object task, Resource capability, ContainerId containerId, Priority priority,
                           Object containerSignature, Object clientCookie) {
    maybeBlock();
    super.allocateTask(task, capability, containerId, priority, containerSignature, clientCookie);
  }

  private void maybeBlock() {
    if (random.nextInt(100) < BLOCKING_CALL_PERCENT) {
      try {
        Thread.sleep(BLOCK_WAIT_AMOUNT + random.nextInt(BLOCK_WAIT_JITTER));
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }
}
