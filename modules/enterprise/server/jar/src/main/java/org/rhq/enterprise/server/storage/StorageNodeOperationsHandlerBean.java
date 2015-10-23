package org.rhq.enterprise.server.storage;

import java.util.List;

import javax.ejb.Asynchronous;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.rhq.cassandra.schema.Table;
import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.core.domain.cloud.StorageClusterSettings;
import org.rhq.core.domain.cloud.StorageNode;
import org.rhq.core.domain.cloud.StorageNode.OperationMode;
import org.rhq.core.domain.operation.OperationHistory;
import org.rhq.core.domain.operation.ResourceOperationHistory;
import org.rhq.core.domain.resource.Resource;
import org.rhq.core.domain.resource.ResourceType;
import org.rhq.core.util.exception.ThrowableUtil;
import org.rhq.enterprise.server.RHQConstants;
import org.rhq.enterprise.server.auth.SubjectManagerLocal;
import org.rhq.enterprise.server.cloud.ClusterTaskFactory;
import org.rhq.enterprise.server.cloud.StorageClusterStateManagerLocal;
import org.rhq.enterprise.server.cloud.StorageNodeManagerLocal;
import org.rhq.enterprise.server.operation.OperationManagerLocal;
import org.rhq.enterprise.server.resource.ResourceManagerLocal;
import org.rhq.enterprise.server.scheduler.jobs.ReplicationFactorCheckJob;
import org.rhq.server.metrics.StorageSession;

/**
 * @author John Sanda
 */
@Stateless
public class StorageNodeOperationsHandlerBean implements StorageNodeOperationsHandlerLocal {

    private final Log log = LogFactory.getLog(StorageNodeOperationsHandlerBean.class);

    private static final String STORAGE_NODE_TYPE_NAME = "RHQ Storage Node";
    private static final String STORAGE_NODE_PLUGIN_NAME = "RHQStorage";
    private static final String KEYSPACE_TYPE_NAME = "Keyspace";


    @PersistenceContext(unitName = RHQConstants.PERSISTENCE_UNIT_NAME)
    private EntityManager entityManager;

    @EJB
    private SubjectManagerLocal subjectManager;

    @EJB
    private StorageNodeManagerLocal storageNodeManager;

    @EJB
    private OperationManagerLocal operationManager;

    @EJB
    private StorageClusterSettingsManagerLocal storageClusterSettingsManager;

    @EJB
    private StorageClientManager storageClientManager;

    @EJB
    private StorageNodeOperationsHandlerLocal storageNodeOperationsHandler;

    @EJB
    private ResourceManagerLocal resourceManager;

    @EJB
    private StorageClusterStateManagerLocal storageClusterStateManager;

    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void announceStorageNode(Subject subject, StorageNode storageNode) {
        if (log.isInfoEnabled()) {
            log.info("Announcing " + storageNode + " to storage node cluster.");
        }
        try {
            storageNodeOperationsHandler.setMode(storageNode, StorageNode.OperationMode.ANNOUNCE);

            List<StorageNode> clusterNodes = getStorageNodesByMode(OperationMode.NORMAL);

            if (clusterNodes.isEmpty()) {
                throw new IndexOutOfBoundsException("There is no StorageNode in " + OperationMode.NORMAL);
            }

            List<ClusterTask> tasks = ClusterTaskFactory.createAnnounce(clusterNodes, storageNode);
            storageClusterStateManager.scheduleTasks(tasks);
            storageClusterStateManager.runTasksInNewTx();

        } catch (IndexOutOfBoundsException e) {
            String msg = "Aborting storage node deployment due to unexpected error while announcing storage node at "
                + storageNode.getAddress();
            log.error(msg, e);
            log.error("If this error occurred with a storage node that was deployed prior to installing the server, "
                + "then this may indicate that the rhq.storage.nodes property in rhq-server.properties was not set "
                + "correctly. All nodes deployed prior to server installation should be listed in the "
                + "rhq.storage.nodes property. Please review the deployment documentation for additional details.");
            storageNodeOperationsHandler.logError(storageNode.getAddress(), msg, e);

        } catch (Exception e) {
            String msg = "Aborting storage node deployment due to unexpected error while announcing storage node at "
                + storageNode.getAddress();
            log.error(msg, e);
            storageNodeOperationsHandler.logError(storageNode.getAddress(), msg, e);
        }
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void unannounceStorageNode(Subject subject, StorageNode storageNode) {
        log.info("Unannouncing " + storageNode);
        List<StorageNode> clusterNodes = getStorageNodesByMode(StorageNode.OperationMode.NORMAL);
        List<ClusterTask> tasks = ClusterTaskFactory.createUnannounce(clusterNodes, storageNode);
        storageClusterStateManager.scheduleTasks(tasks);

    }

    @Override
    public List<StorageNode> getStorageNodesByMode(StorageNode.OperationMode mode) {
        List<StorageNode> clusterNodes = entityManager
            .createNamedQuery(StorageNode.QUERY_FIND_ALL_BY_MODE, StorageNode.class)
            .setParameter("operationMode", mode).getResultList();

        return clusterNodes;
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void uninstall(Subject subject, StorageNode storageNode) {
        log.info("Uninstalling " + storageNode);
        List<ClusterTask> tasks = ClusterTaskFactory.createUninstall(storageNode);
        storageClusterStateManager.scheduleTasks(tasks);

    }

    @Override
    public void finishUninstall(Subject subject, StorageNode storageNode) {
        storageNode = entityManager.find(StorageNode.class, storageNode.getId());
        if (storageNode.getResource() != null) {
            log.info("Removing storage node resource " + storageNode.getResource() + " from inventory");
            Resource resource = storageNode.getResource();
            storageNodeOperationsHandler.detachFromResource(storageNode);
            resourceManager.uninventoryResource(subject, resource.getId());
        }
        log.info("Removing storage node entity " + storageNode + " from database");
        entityManager.remove(storageNode);

        log.info(storageNode + " has been undeployed");
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public void detachFromResource(StorageNode storageNode) {
        storageNode.setResource(null);
        storageNode.setFailedOperation(null);
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void decommissionStorageNode(Subject subject, StorageNode storageNode) {
        log.info("Preparing to decommission " + storageNode);

        storageNode = storageNodeOperationsHandler.setMode(storageNode, StorageNode.OperationMode.DECOMMISSION);

        List<StorageNode> storageNodes = getStorageNodesByMode(StorageNode.OperationMode.NORMAL);
        boolean runRepair = updateSchemaIfNecessary(storageNodes.size() + 1, storageNodes.size());

        List<ClusterTask> tasks = ClusterTaskFactory.createDecomission(storageNodes, storageNode, runRepair);
        storageClusterStateManager.scheduleTasks(tasks);
    }


    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public void logError(String address, String error, Exception e) {
        try {
            StorageNode newStorageNode = findStorageNodeByAddress(address);
            newStorageNode.setErrorMessage(error + " Check the server log for details. Root cause: "
                + ThrowableUtil.getRootCause(e).getMessage());
        } catch (Exception e1) {
            log.error("Failed to log error against storage node", e);
        }
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void performAddNodeMaintenance(Subject subject, StorageNode storageNode) {
        try {
            storageNodeOperationsHandler.performAddMaintenance(subject, storageNode);
        } catch (Exception e) {
            String msg = "Aborting storage node deployment due to unexpected error while performing add node "
                + "maintenance.";
            log.error(msg, e);
            storageNodeOperationsHandler.logError(storageNode.getAddress(), msg, e);
        }
    }

    @Override
    public void performAddMaintenance(Subject subject, StorageNode storageNode) {
        List<StorageNode> clusterNodes = getStorageNodesByMode(StorageNode.OperationMode.NORMAL);

        boolean runRepair = updateSchemaIfNecessary(clusterNodes.size(), clusterNodes.size() + 1);

        StorageClusterSettings settings = storageClusterSettingsManager.getClusterSettings(subject);
        storageNodeManager.scheduleSnapshotManagementOperationsForStorageNode(subject, storageNode, settings);

        List<ClusterTask> tasks = ClusterTaskFactory.createAddMaintenance(clusterNodes, storageNode, runRepair);
        storageClusterStateManager.scheduleTasks(tasks);

    }

    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void performRemoveNodeMaintenance(Subject subject, StorageNode storageNode) {
        try {
            storageNodeOperationsHandler.performRemoveMaintenance(subject, storageNode);
        } catch (Exception e) {
            String msg = "Aborting undeployment due to unexpected error while performing remove node maintenance.";
            log.error(msg, e);
            storageNodeOperationsHandler.logError(storageNode.getAddress(), msg, e);
        }
    }

    @Override
    public void performRemoveMaintenance(Subject subject, StorageNode storageNode) {

    }

    @Override
    @Asynchronous
    public void handleOperationUpdateIfNecessary(OperationHistory operationHistory) {
        if (!(operationHistory instanceof ResourceOperationHistory)) {
            return;
        }

        ResourceOperationHistory resourceOperationHistory = (ResourceOperationHistory) operationHistory;
        if (!isStorageNodeOperation(resourceOperationHistory)) {
            return;
        }
        storageClusterStateManager.handleResourceOperation(resourceOperationHistory);
    }


    @Override
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void bootstrapStorageNode(Subject subject, StorageNode storageNode) {
        List<StorageNode> clusterNodes = storageNodeOperationsHandler
            .getStorageNodesByMode(StorageNode.OperationMode.NORMAL);

        StorageClusterSettings settings = storageClusterSettingsManager.getClusterSettings(subjectManager.getOverlord());
        storageClusterStateManager.scheduleTasks(ClusterTaskFactory.createBootstrap(clusterNodes, storageNode, settings));
    }

    @Override
    public void runRepair(Subject subject) {
        List<StorageNode> clusterNodes = storageNodeManager.getClusterNodes();
        if (clusterNodes.size() <= 1) {
            log.info("Temporary not skipping repair even on this 1 cluster case:)");
            //log.info("Skipping scheduled repair since this is a single-node cluster");
            //return;
        }

        log.info("Starting anti-entropy repair on storage cluster: " + clusterNodes);
        for (StorageNode node : clusterNodes) {
            node.setErrorMessage(null);
            node.setFailedOperation(null);
            node.setMaintenancePending(true);
        }

        List<ClusterTask> tasks = ClusterTaskFactory.createRepair(clusterNodes);
        storageClusterStateManager.scheduleTasks(tasks);
        storageClusterStateManager.runTasksInNewTx();
    }

    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public void handleRepair(ResourceOperationHistory operationHistory) {
    }


    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public StorageNode setMode(StorageNode storageNode, StorageNode.OperationMode newMode) {
        storageNode.setOperationMode(newMode);
        return entityManager.merge(storageNode);
    }


    private StorageNode findStorageNodeByAddress(String address) {
        return entityManager.createNamedQuery(StorageNode.QUERY_FIND_BY_ADDRESS, StorageNode.class)
            .setParameter("address", address).getSingleResult();
    }

    private boolean isStorageNodeOperation(ResourceOperationHistory operationHistory) {
        if (operationHistory == null) {
            return false;
        }

        ResourceType resourceType = operationHistory.getOperationDefinition().getResourceType();
        return resourceType.getPlugin().equals(STORAGE_NODE_PLUGIN_NAME)
            && (resourceType.getName().equals(STORAGE_NODE_TYPE_NAME) || resourceType.getName().equals(
                KEYSPACE_TYPE_NAME));
    }

    private boolean updateSchemaIfNecessary(int previousClusterSize, int newClusterSize) {
        boolean isRepairNeeded;
        int replicationFactor = 1;

        if (previousClusterSize == 0) {
            throw new IllegalStateException("previousClusterSize cannot be 0");
        }
        if (newClusterSize == 0) {
            throw new IllegalStateException("newClusterSize cannot be 0");
        }
        if (Math.abs(newClusterSize - previousClusterSize) != 1) {
            throw new IllegalStateException("The absolute difference between previousClusterSize["
                + previousClusterSize + "] and newClusterSize[" + newClusterSize + "] must be 1");
        }

        if (newClusterSize == 1) {
            isRepairNeeded = false;
            replicationFactor = 1;
        } else if (newClusterSize >= 5) {
            isRepairNeeded = false;
        } else if (previousClusterSize > 4) {
            isRepairNeeded = false;
        } else if (previousClusterSize == 4 && newClusterSize == 3) {
            isRepairNeeded = true;
            replicationFactor = 2;
        } else if (previousClusterSize == 3 && newClusterSize == 2) {
            isRepairNeeded = false;
        } else if (previousClusterSize == 1 && newClusterSize == 2) {
            isRepairNeeded = true;
            replicationFactor = 2;
        } else if (previousClusterSize == 2 && newClusterSize == 3) {
            isRepairNeeded = false;
        } else if (previousClusterSize == 3 && newClusterSize == 4) {
            isRepairNeeded = true;
            replicationFactor = 3;
        } else {
            throw new IllegalStateException("previousClusterSize[" + previousClusterSize + "] and newClusterSize["
                + newClusterSize + "] is not supported");
        }

        if (isRepairNeeded) {
            updateReplicationFactor(replicationFactor, newClusterSize);
            if (previousClusterSize == 1) {
                updateGCGraceSeconds(691200); // 8 days
            }
        } else if (newClusterSize == 1) {
            updateReplicationFactor(1, 1);
            updateGCGraceSeconds(0);
        }

        return isRepairNeeded;
    }

    private void updateReplicationFactor(int replicationFactor, int clusterSize) {
        StorageSession session = storageClientManager.getSession();
        ReplicationFactorCheckJob.updateReplicationFactor(session, "rhq", replicationFactor);
        ReplicationFactorCheckJob.updateReplicationFactor(session, "system_auth", clusterSize);
    }

    private void updateGCGraceSeconds(int seconds) {
        StorageSession session = storageClientManager.getSession();
        for (Table table : Table.values()) {
            session.execute("ALTER TABLE " + table.getTableName() + " WITH gc_grace_seconds = " + seconds);
        }
        session.execute("ALTER TABLE rhq.schema_version WITH gc_grace_seconds = " + seconds);
    }

}
