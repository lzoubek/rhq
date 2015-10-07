package org.rhq.enterprise.server.cloud;

import java.util.Arrays;
import java.util.List;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.authz.Permission;
import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.core.domain.cloud.StorageClusterState;
import org.rhq.core.domain.cloud.StorageClusterState.OperationStatus;
import org.rhq.core.domain.cloud.StorageClusterState.Status;
import org.rhq.core.domain.cloud.StorageNode;
import org.rhq.core.domain.cloud.StorageNode.OperationMode;
import org.rhq.core.domain.common.JobTrigger;
import org.rhq.core.domain.common.composite.SystemSetting;
import org.rhq.core.domain.common.composite.SystemSettings;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.operation.OperationRequestStatus;
import org.rhq.core.domain.operation.ResourceOperationHistory;
import org.rhq.core.domain.operation.bean.ResourceOperationSchedule;
import org.rhq.core.domain.resource.Resource;
import org.rhq.enterprise.server.RHQConstants;
import org.rhq.enterprise.server.auth.SubjectManagerLocal;
import org.rhq.enterprise.server.authz.RequiredPermission;
import org.rhq.enterprise.server.configuration.ConfigurationManagerLocal;
import org.rhq.enterprise.server.operation.OperationManagerLocal;
import org.rhq.enterprise.server.resource.ResourceNotFoundException;
import org.rhq.enterprise.server.storage.StorageClusterSettingsManagerLocal;
import org.rhq.enterprise.server.storage.StorageNodeOperationsHandlerLocal;
import org.rhq.enterprise.server.system.SystemManagerLocal;
import org.rhq.enterprise.server.util.LookupUtil;

@Stateless
public class StorageClusterStateManagerBean implements StorageClusterStateManagerLocal {
    private final Log log = LogFactory.getLog(StorageClusterStateManagerBean.class);
    private static final String NON_RESOURCE_OPERATION_PREFIX = "#";
    public static final String SERVER_TASK_REPAIR_END = "endRepair";

    @EJB
    private ConfigurationManagerLocal configurationManager;

    @EJB
    private SystemManagerLocal systemManager;

    @EJB
    private OperationManagerLocal operationManager;

    @EJB
    private StorageNodeManagerLocal storageNodeManager;

    @EJB
    private StorageNodeOperationsHandlerLocal storageNodeOperationsHandler;

    @EJB
    private StorageClusterSettingsManagerLocal storageClusterSettingsManager;

    @EJB
    private SubjectManagerLocal subjectManager;

    @EJB
    private StorageClusterStateManagerLocal self;

    @PersistenceContext(unitName = RHQConstants.PERSISTENCE_UNIT_NAME)
    private EntityManager entityManager;

    private StorageClusterState initializeNew() {
        Configuration config = new Configuration();
        StorageClusterState state = new StorageClusterState(config);
        state.setStatus(Status.UP);
        state.setOperationStatus(OperationStatus.IDLE);
        entityManager.persist(config);
        debug("Persisting Setting STORAGE_CLUSTER_STATE_CONFIGURATION_ID=" + config.getId());
        systemManager.setAnySystemSetting(SystemSetting.STORAGE_CLUSTER_STATE_CONFIGURATION_ID,
            String.valueOf(config.getId()));
        return state;
    }

    private void pollTask(StorageClusterState state) {
        entityManager.remove(state.pollTask().getBackingMap());
    }

    private StorageClusterState loadState() {
        SystemSettings settings = systemManager.getUnmaskedSystemSettings(true);
        String configurationIdStr = settings.get(SystemSetting.STORAGE_CLUSTER_STATE_CONFIGURATION_ID);
        Integer configurationId = null;
        if (configurationIdStr != null) {
            configurationId = Integer.parseInt(configurationIdStr);
        }
        if (configurationId == null || configurationId == 0) {
            // state config not yet initialized (very first start or upgrade)
            return initializeNew();
        }
        Configuration config = entityManager.find(Configuration.class, configurationId);
        if (config == null) {
            log.warn("Failed to find StorageClusterConfiguration with id=" + configurationId
                + " it may have been removed. Re-initializing ClusterState");
            return initializeNew();
        }
        return new StorageClusterState(config);
    }

    private void saveState(StorageClusterState state) {
        state.setLastUpdate();
        // persist new tasks and it it's new values
        for (Property p : state.getConfig().getList("queue").getList()) {
            if (p.getId() <= 0) {
                entityManager.persist(p);

            } else {
                PropertyMap map = (PropertyMap) p;
                for (Property pp : map.getMap().values()) {
                    if (pp.getId() <= 0) {
                        entityManager.persist(pp);
                    }
                }
            }

        }
        entityManager.merge(state.getConfig());
    }

    @Override
    @RequiredPermission(Permission.MANAGE_SETTINGS)
    public StorageClusterState getState(Subject subject) {
        StorageClusterState state = loadState();
        return state;
    }

    @Override
    public void runTasks(boolean force) {
        debug("Running Storage Cluster tasks");
        StorageClusterState state = loadState();
        ClusterTask task = state.peekTask();
        if (task == null) {
            // nothing to do
            debug("Task queue is empty - nothing to do");
            return;
        }
        if (!force) {
            if (OperationRequestStatus.INPROGRESS.equals(task.getStatus())) {
                debug("Task " + task + " in progress.");
                return;
            }
            if (OperationRequestStatus.FAILURE.equals(task.getStatus())) {
                // TODO maybe we can implement re-try logic?
                log.error("Task " + task + " has failed, Admin intervention needed!");
                return;
            }
        }

        if (task.getStatus() == null || force) {
            // we're either scheduling new task
            // or forced to re-schedule Failed or Running task
            debug("Executing  " + task);
            Subject overlord = LookupUtil.getSubjectManager().getOverlord();
            StorageNode storageNode = null;
            if (task.getStorageNodeId() != null) {
                storageNode = entityManager.find(StorageNode.class, task.getStorageNodeId());
            }
            if (task.getOperationName().startsWith(NON_RESOURCE_OPERATION_PREFIX)) {
                debug("Running server code");
                runNonResourceOperation(state, task, storageNode);
                return;
            }
            if (storageNode == null) {
                // storageNode *MUST* exist for resource operations
                task.withStatus(OperationRequestStatus.FAILURE)
                    .withDescription("Could not find StorageNode by id="+task.getStorageNodeId());
                    return;
            }
            debug("Scheduling resource operation");
            Resource resource = null;
            if (task.getResourceId() == null) {
                // resource being used is storage node
                resource = storageNode.getResource();
            } else {
                try {
                    resource = LookupUtil.getResourceManager().getResource(overlord, task.getResourceId());
                } catch (ResourceNotFoundException rnf) {
                    log.error("Unable to schedule task " + task + " marking as " + OperationRequestStatus.FAILURE, rnf);
                    task.withStatus(OperationRequestStatus.FAILURE).withDescription(
                        "Unable to schedule task " + rnf.getMessage());
                    saveState(state);
                    return;
                }
            }
            debug("Checking live availability of " + storageNode);
            if (!storageNodeManager.isStorageNodeAvailable(storageNode)) {
                log.error(storageNode + " does not appear to be UP");
                task.withStatus(OperationRequestStatus.FAILURE)
                    .withDescription("Not scheduling operation, because StorageNode resource does not appear to be UP");
                saveState(state);
                return;
            }
            debug("Scheduling operation");
            int historyId = self.scheduleResourceOperationInNewTx(task, resource);
            debug("Operation scheduled");
            task.withOperationHistoryId(historyId)
                .withStatus(OperationRequestStatus.INPROGRESS);
            saveState(state);
        }

    }

    private void runNonResourceOperation(StorageClusterState state, ClusterTask task, StorageNode storageNode) {
        if (ClusterTaskFactory.OP_SETMODE.equals(task.getOperationName())) {
            OperationMode mode = OperationMode.valueOf(task.getParams().getSimpleValue("mode"));
            storageNodeOperationsHandler.setMode(storageNode, mode);
            pollTask(state);
            return;
        }
        if (ClusterTaskFactory.OP_SET_CLUSTER_OPERATION_STATUS.equals(task.getOperationName())) {
            OperationStatus mode = OperationStatus.valueOf(task.getParams().getSimpleValue("mode"));
            self.setStatus(null, mode, null);
            pollTask(state);
            return;
        }
        if (ClusterTaskFactory.OP_NODE_ANNOUNCED.equals(task.getOperationName())) {
            log.info("Successfully announced new storage node to storage cluster");
            storageNodeOperationsHandler.bootstrapStorageNode(subjectManager.getOverlord(), storageNode);
            pollTask(state);
            return;
        }
        if (ClusterTaskFactory.OP_NODE_BOOTSTRAPPED.equals(task.getOperationName())) {
            log.info("The prepare for bootstrap operation completed successfully for " + storageNode);
            storageNodeOperationsHandler.performAddMaintenance(subjectManager.getOverlord(), storageNode);
            pollTask(state);
            return;
        }
        log.error("Unknown server operation in "+task);
        task.withStatus(OperationRequestStatus.FAILURE)
            .withDescription(task.getDescription()+" failed : Unknown server operation name");
    }


    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Override
    public int scheduleResourceOperationInNewTx(ClusterTask task, Resource resource) {
        Subject overlord = LookupUtil.getSubjectManager().getOverlord();
        ResourceOperationSchedule schedule = new ResourceOperationSchedule();
        schedule.setResource(resource);
        schedule.setJobTrigger(JobTrigger.createNowTrigger());
        schedule.setSubject(overlord);
        schedule.setOperationName(task.getOperationName());
        schedule.setParameters(task.getParams());
        return operationManager.scheduleResourceOperation(overlord, schedule);
    }

    private void debug(String message) {
        log.info(message);
        if (log.isDebugEnabled()) {
            log.debug(message);
        }
    }

    @Override
    public void scheduleTasks(ClusterTask... tasks) {
        if (tasks != null) {
            scheduleTasks(Arrays.asList(tasks));
        }
    }

    @Override
    public void scheduleTasks(List<ClusterTask> tasks) {
        if (tasks != null) {
            StorageClusterState state = loadState();
            for (ClusterTask task : tasks) {
                state.addTask(task);
            }
            saveState(state);
        }
    }

    @Override
    public void handleResourceOperation(ResourceOperationHistory operationHistory) {
        if (OperationRequestStatus.INPROGRESS.equals(operationHistory.getStatus())) {
            return;
        }
        StorageClusterState state = loadState();
        ClusterTask task = state.peekTask();
        if (task.getOperationHistoryId() != null && operationHistory.getId() == task.getOperationHistoryId()) {
            task.withStatus(operationHistory.getStatus());
            if (OperationRequestStatus.SUCCESS.equals(task.getStatus())) {
                debug("Task " + task + " successfully completed");
                // remove from queue
                pollTask(state);
                if (state.getTaskQueue().isEmpty()) {
                    state.setOperationStatus(OperationStatus.IDLE);
                }
            } else {
                debug("Task " + task + " failed " + operationHistory);
                task.withStatus(operationHistory.getStatus())
                    .addDescription("failed: " + operationHistory.getErrorMessage());
            }
        } else {
            log.warn("Unable to handle Resource Operation with " + operationHistory
                + " no such task on top of the queue");
        }
    }


    @Override
    public void setStatus(Status status, OperationStatus operationStatus, String message) {
        StorageClusterState state = loadState();
        if (status != null) {
            state.setStatus(status);
        }
        if (operationStatus != null) {
            state.setOperationStatus(operationStatus);
        }
    }

    @Override
    public void clearTasks(boolean force) {
        StorageClusterState state = loadState();
        ClusterTask task = null;
        while ((task = state.pollTask()) != null) {
            entityManager.remove(task.getBackingMap());
        }

    }
}
