package org.rhq.enterprise.server.cloud;

import java.util.List;

import javax.ejb.Local;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.core.domain.cloud.StorageClusterState;
import org.rhq.core.domain.cloud.StorageClusterState.OperationStatus;
import org.rhq.core.domain.cloud.StorageClusterState.Status;
import org.rhq.core.domain.operation.ResourceOperationHistory;
import org.rhq.core.domain.resource.Resource;

@Local
public interface StorageClusterStateManagerLocal {

    StorageClusterState getState(Subject subject);

    void handleResourceOperation(ResourceOperationHistory operationHistory);
    void scheduleTasks(ClusterTask... tasks);

    void scheduleTasks(List<ClusterTask> tasks);

    void clearTasks(boolean force);
    void runTasks(boolean force);

    int scheduleResourceOperationInNewTx(ClusterTask task, Resource resource);
    void setStatus(Status status, OperationStatus operationStatus, String message);
}
