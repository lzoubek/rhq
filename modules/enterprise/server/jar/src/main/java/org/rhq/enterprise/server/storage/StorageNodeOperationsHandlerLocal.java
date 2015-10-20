package org.rhq.enterprise.server.storage;

import java.util.List;

import javax.ejb.Asynchronous;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.cloud.StorageNode;
import org.rhq.core.domain.operation.OperationHistory;
import org.rhq.core.domain.operation.ResourceOperationHistory;

/**
 * @author John Sanda
 */
public interface StorageNodeOperationsHandlerLocal {

    @Asynchronous
    void handleOperationUpdateIfNecessary(OperationHistory operationHistory);

    void announceStorageNode(Subject subject, StorageNode storageNode);

    void unannounceStorageNode(Subject subject, StorageNode storageNode);

    void bootstrapStorageNode(Subject subject, StorageNode storageNode);

    void performAddNodeMaintenance(Subject subject, StorageNode storageNode);

    void performAddMaintenance(Subject subject, StorageNode storageNode);

    void uninstall(Subject subject, StorageNode storageNode);

    void detachFromResource(StorageNode storageNode);

    void decommissionStorageNode(Subject subject, StorageNode storageNode);

    void performRemoveNodeMaintenance(Subject subject, StorageNode storageNode);

    void performRemoveMaintenance(Subject subject, StorageNode storageNode);

    void runRepair(Subject subject);

    void handleRepair(ResourceOperationHistory operationHistory);

    void logError(String address, String error, Exception e);

    StorageNode setMode(StorageNode storageNode, StorageNode.OperationMode newMode);

    List<StorageNode> getStorageNodesByMode(StorageNode.OperationMode mode);

    void finishUninstall(Subject subject, StorageNode storageNode);
}
