package org.apache.tez.dag.app.rm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.api.oldrecords.TaskAttemptState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.dag.app.dag.impl.TaskAttemptImpl;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskScheduler;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskSchedulerManagerStressTest {

  private final Logger LOG = LoggerFactory.getLogger(TaskSchedulerManagerStressTest.class.getName());
  private static final int TEST_TIMEOUT_SECONDS = 180;
  private static final int MAX_CALLBACK_DELAY_MS = 100;
  private static final int DELAYED_SCHEDULING_WAIT_JITTER = 450;
  private static final int DELAYED_SCHEDULING_WAIT_MS = 500;
  private static final float DELAYED_SCHEDULING_PERCENT = 0.01f;
  private static final int NUMBER_OF_TASK_ALLOCATIONS = 10000;

  private final CountDownLatch latch = new CountDownLatch(1);
  private final Semaphore liveTasks = new Semaphore(0);

  AppContext mockAppContext;
  DAGClientServer mockClientService;
  EventHandler mockEventHandler;
  ContainerSignatureMatcher mockSigMatcher;
  AMContainerMap mockAMContainerMap;
  WebUIService mockWebUIService;

  @Before
  public void setup() {
    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    doReturn(new Configuration(false)).when(mockAppContext).getAMConf();
    mockClientService = mock(DAGClientServer.class);
    mockEventHandler = mock(EventHandler.class);
    mockSigMatcher = mock(ContainerSignatureMatcher.class);
    mockAMContainerMap = mock(AMContainerMap.class);
    mockWebUIService = mock(WebUIService.class);
    when(mockAppContext.getAllContainers()).thenReturn(mockAMContainerMap);
    when(mockClientService.getBindAddress()).thenReturn(new InetSocketAddress(10000));
  }

  @Test(timeout = 200000)
  public void testManyConcurrentEventsAndCallbacks() throws InterruptedException {
    StressTestTaskSchedulerManager tsm = new StressTestTaskSchedulerManager(
        null, mockAppContext, mockClientService, mockEventHandler, mockSigMatcher, mockWebUIService);

    Configuration conf = new Configuration(false);
    tsm.init(conf);
    tsm.start();

    AtomicBoolean running = new AtomicBoolean(true);
    TaskStarter taskStarter = new TaskStarter(tsm.eventQueue);
    TaskEnder taskEnder = new TaskEnder(running, tsm.eventQueue);

    taskEnder.start();
    taskStarter.start();

    latch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    running.set(false);

    tsm.verify();
  }

  class TaskStarter extends Thread {

    private final BlockingQueue<AMSchedulerEvent> eventQueue;

    TaskStarter(BlockingQueue<AMSchedulerEvent> eventQueue) {
      this.eventQueue = eventQueue;
    }

    @Override
    public void run() {
      for (int i = 0; i < NUMBER_OF_TASK_ALLOCATIONS; i++) {
        eventQueue.add(createLaunchRequest(i));
      }
    }
  }

  class TaskEnder extends Thread {

    private final AtomicBoolean running;
    private final BlockingQueue<AMSchedulerEvent> eventQueue;

    TaskEnder(AtomicBoolean running, BlockingQueue<AMSchedulerEvent> eventQueue) {
      this.running = running;
      this.eventQueue = eventQueue;
    }

    @Override
    public void run() {
      LOG.info("Starting to end tasks");
      while (running.get()) {
        try {
          if (liveTasks.tryAcquire(50, TimeUnit.MILLISECONDS)) {
            eventQueue.add(createEndEvent());
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private AMSchedulerEventTALaunchRequest createLaunchRequest(int taskId) {
    TaskAttemptImpl mockTaskAttempt = mock(TaskAttemptImpl.class);
    TezTaskAttemptID attemptID = TezTaskAttemptID.fromString("application_1_1_1_1_"+ taskId + "_0");
    when(mockTaskAttempt.getID()).thenReturn(attemptID);
    Resource resource = Resource.newInstance(1024, 1);
    ContainerContext containerContext = new ContainerContext(new HashMap<>(), new Credentials(), new HashMap<>(), "");
    int priority = 10;
    TaskLocationHint locHint = TaskLocationHint.createTaskLocationHint(new HashSet<String>(), null);
    AMSchedulerEventTALaunchRequest lr =
        new AMSchedulerEventTALaunchRequest(attemptID, resource, null, mockTaskAttempt, locHint,
            priority, containerContext, 0, 0, 0);
    return lr;
  }

  private AMSchedulerEventTAEnded createEndEvent() {
    TaskAttemptImpl mockTaskAttempt = mock(TaskAttemptImpl.class);
    AMSchedulerEventTAEnded event = new AMSchedulerEventTAEnded(mockTaskAttempt, null, TaskAttemptState.SUCCEEDED, null, null, 0);
    return event;
  }

  class StressTestTaskSchedulerManager extends MockTaskSchedulerManager {

    Queue<Long> waitAmountQueue = new BlockingArrayQueue<>();

    public StressTestTaskSchedulerManager(TaskScheduler taskScheduler, AppContext appContext, DAGClientServer clientService, EventHandler eventHandler, ContainerSignatureMatcher containerSignatureMatcher, WebUIService webUI) {
      super(taskScheduler, appContext, clientService, eventHandler, containerSignatureMatcher, webUI);
    }

    @Override
    protected void instantiateSchedulers(String host, int port, String trackingUrl, AppContext appContext) {
      TaskSchedulerContext originalContext = new TaskSchedulerContextImpl(this, mockAppContext, 0, "", 0, "", 0, null);
      TaskSchedulerContext taskSchedulerContext = wrapTaskSchedulerContext(originalContext);
      TestTaskScheduler testTaskScheduler = new TestTaskScheduler(taskSchedulerContext);
      taskSchedulers[0] = new TaskSchedulerWrapper(testTaskScheduler);
      taskSchedulerServiceWrappers[0] =
          new ServicePluginLifecycleAbstractService<>(taskSchedulers[0].getTaskScheduler());
    }

    @Override
    public void taskAllocated(int schedulerId, Object task, Object appCookie, Container container) {
      long startTime = System.currentTimeMillis();
      super.taskAllocated(schedulerId, task, appCookie, container);
      long endTime = System.currentTimeMillis();
      liveTasks.release();
      waitAmountQueue.add(endTime - startTime);
    }

    public void verify() {
      while (!waitAmountQueue.isEmpty()) {
        Long amount = waitAmountQueue.poll();
        assertTrue("Callback delayed for " + amount + " ms", amount < MAX_CALLBACK_DELAY_MS);
      }
      assertEquals(NUMBER_OF_TASK_ALLOCATIONS, getTaskScheduler().allocatedTasks.get());
      assertEquals(NUMBER_OF_TASK_ALLOCATIONS, getTaskScheduler().deallocatedTasks.get());
    }

    private TestTaskScheduler getTaskScheduler() {
      return (TestTaskScheduler) taskSchedulers[0].getTaskScheduler();
    }
  }

  class TestTaskScheduler extends TaskScheduler {

    private Random random = new Random();

    AtomicInteger allocatedTasks = new AtomicInteger();
    AtomicInteger deallocatedTasks = new AtomicInteger();

    public TestTaskScheduler(TaskSchedulerContext taskSchedulerContext) {
      super(taskSchedulerContext);
    }

    @Override
    public Resource getAvailableResources() throws ServicePluginException {
      return null;
    }

    @Override
    public Resource getTotalResources() throws ServicePluginException {
      return null;
    }

    @Override
    public int getClusterNodeCount() throws ServicePluginException {
      return 0;
    }

    @Override
    public void blacklistNode(NodeId nodeId) throws ServicePluginException {

    }

    @Override
    public void unblacklistNode(NodeId nodeId) throws ServicePluginException {

    }

    @Override
    public void allocateTask(Object task, Resource capability, String[] hosts, String[] racks, Priority priority, Object containerSignature, Object clientCookie) throws ServicePluginException {
      allocatedTasks.incrementAndGet();
      if (random.nextFloat() < DELAYED_SCHEDULING_PERCENT) {
        try {
          int schedulingDelay = DELAYED_SCHEDULING_WAIT_MS - DELAYED_SCHEDULING_WAIT_JITTER;
          schedulingDelay += random.nextInt(DELAYED_SCHEDULING_WAIT_JITTER * 2);
          LOG.info("Scheduling is blocked for {} ms", schedulingDelay);
          Thread.sleep(schedulingDelay);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Container container = mock(Container.class);
      getContext().taskAllocated(task, clientCookie, container);
    }

    @Override
    public void allocateTask(Object task, Resource capability, ContainerId containerId, Priority priority, Object containerSignature, Object clientCookie) throws ServicePluginException {
      allocatedTasks.incrementAndGet();
      Container container = mock(Container.class);
      getContext().taskAllocated(task, clientCookie, container);
    }

    @Override
    public boolean deallocateTask(Object task, boolean taskSucceeded, TaskAttemptEndReason endReason, @Nullable String diagnostics) throws ServicePluginException {
      int numberOfDeallocations = deallocatedTasks.incrementAndGet();
      if (numberOfDeallocations == NUMBER_OF_TASK_ALLOCATIONS) {
        latch.countDown();
      }
      return false;
    }

    @Override
    public Object deallocateContainer(ContainerId containerId) throws ServicePluginException {
      getContext().containerBeingReleased(containerId);
      return null;
    }

    @Override
    public void setShouldUnregister() throws ServicePluginException {

    }

    @Override
    public boolean hasUnregistered() throws ServicePluginException {
      return false;
    }

    @Override
    public void dagComplete() throws ServicePluginException {

    }
  }
}
