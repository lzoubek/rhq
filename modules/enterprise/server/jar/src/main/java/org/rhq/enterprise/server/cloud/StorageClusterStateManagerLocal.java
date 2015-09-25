package org.rhq.enterprise.server.cloud;

import javax.ejb.Local;

import org.rhq.core.domain.cloud.StorageClusterState;

@Local
public interface StorageClusterStateManagerLocal {

    StorageClusterState getState();

    void runTasks();
}
