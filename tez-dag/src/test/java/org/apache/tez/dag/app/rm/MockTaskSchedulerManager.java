package org.apache.tez.dag.app.rm;


import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.ContainerSignatureMatcher;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.client.DAGClientServer;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ServicePluginLifecycleAbstractService;
import org.apache.tez.dag.app.web.WebUIService;
import org.apache.tez.hadoop.shim.HadoopShimsLoader;
import org.apache.tez.serviceplugins.api.TaskScheduler;

import java.util.concurrent.atomic.AtomicBoolean;

public class MockTaskSchedulerManager extends TaskSchedulerManager {

  final AtomicBoolean notify = new AtomicBoolean(false);

  private TaskScheduler taskScheduler;

  public MockTaskSchedulerManager(TaskScheduler taskScheduler, AppContext appContext,
                                  DAGClientServer clientService, EventHandler eventHandler,
                                  ContainerSignatureMatcher containerSignatureMatcher,
                                  WebUIService webUI) {
    super(appContext, clientService, eventHandler, containerSignatureMatcher, webUI,
        Lists.newArrayList(new NamedEntityDescriptor("FakeDescriptor", null)), false,
        new HadoopShimsLoader(appContext.getAMConf()).getHadoopShim());
    this.taskScheduler = taskScheduler;
  }

  @Override
  protected void instantiateSchedulers(String host, int port, String trackingUrl,
                                       AppContext appContext) {
    taskSchedulers[0] = new TaskSchedulerWrapper(taskScheduler);
    taskSchedulerServiceWrappers[0] =
        new ServicePluginLifecycleAbstractService<>(taskSchedulers[0].getTaskScheduler());
  }

  @Override
  protected void notifyForTest() {
    synchronized (notify) {
      notify.set(true);
      notify.notifyAll();
    }
  }

}