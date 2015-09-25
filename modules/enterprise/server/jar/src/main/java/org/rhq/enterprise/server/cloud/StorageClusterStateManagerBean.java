package org.rhq.enterprise.server.cloud;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.core.domain.cloud.StorageClusterState;
import org.rhq.core.domain.cloud.StorageClusterState.OperationStatus;
import org.rhq.core.domain.cloud.StorageClusterState.Status;
import org.rhq.core.domain.cloud.StorageNode;
import org.rhq.core.domain.common.JobTrigger;
import org.rhq.core.domain.common.composite.SystemSetting;
import org.rhq.core.domain.common.composite.SystemSettings;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.operation.OperationDefinition;
import org.rhq.core.domain.operation.OperationHistory;
import org.rhq.core.domain.operation.OperationRequestStatus;
import org.rhq.core.domain.operation.bean.ResourceOperationSchedule;
import org.rhq.enterprise.server.RHQConstants;
import org.rhq.enterprise.server.configuration.ConfigurationManagerLocal;
import org.rhq.enterprise.server.operation.OperationManagerLocal;
import org.rhq.enterprise.server.storage.StorageNodeOperationsHandlerBean;
import org.rhq.enterprise.server.system.SystemManagerLocal;
import org.rhq.enterprise.server.util.LookupUtil;

@Stateless
public class StorageClusterStateManagerBean implements StorageClusterStateManagerLocal {
    private final Log log = LogFactory.getLog(StorageClusterStateManagerBean.class);

    @EJB
    private ConfigurationManagerLocal configurationManager;

    @EJB
    private SystemManagerLocal systemManager;

    @EJB
    private OperationManagerLocal operationManager;

    @EJB
    private StorageNodeManagerLocal storageNodeManager;

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

    private StorageClusterState loadState() {
        SystemSettings settings = systemManager.getUnmaskedSystemSettings(true);
        String configurationIdStr = settings.get(SystemSetting.STORAGE_CLUSTER_STATE_CONFIGURATION_ID);
        log.info("Configuration id is " + configurationIdStr);
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
        entityManager.merge(state.getConfig());
    }

    @Override
    public StorageClusterState getState() {
        StorageClusterState state = loadState();
        ClusterTask task = new ClusterTask()
            .withStatus(OperationRequestStatus.INPROGRESS)
            .withDescription("outstanding task " + System.currentTimeMillis());
        state.addTask(task);
        state.setOperationStatus(OperationStatus.MAINTANANCE);
        state.pollTask();
        saveState(state);
        return state;
    }

    @Override
    public void runTasks() {
        debug("Running Storage Cluster tasks");
        StorageClusterState state = loadState();
        ClusterTask task = state.peekTask();
        if (task == null) {
            // nothing to do
            debug("Done - task queue was empty.");
            return;
        }
        if (OperationRequestStatus.INPROGRESS.equals(task.getStatus())) {
            debug("Done - task " + task + " in progress.");
            return;
        }
        if (OperationRequestStatus.FAILURE.equals(task.getStatus())) {
            // TODO implement re-try logic
            log.error("Done - task " + task + " has failed, Admin intervention needed!");
            return;
        }
        if (task.getStatus() == null) {
            debug("Scheduling resource operation based on  " + task);
            StorageNode storageNode = entityManager.find(StorageNode.class, task.getStorageNodeId());
            if (!storageNodeManager.isStorageNodeAvailable(storageNode)) {

            }
        }

    }

    /**
     * Optimally this should be called outside of a transaction, because when scheduling an operation we
     * currently call back into {@link StorageNodeOperationsHandlerBean#handleOperationUpdateIfNecessary(OperationHistory)}.
     * This runs the risk of locking if called inside an existing transaction.
     */
    private void scheduleOperation(ClusterTask task, StorageNode storageNode) {
        Subject overlord = LookupUtil.getSubjectManager().getOverlord();
        ResourceOperationSchedule schedule = new ResourceOperationSchedule();
        schedule.setResource(storageNode.getResource());
        schedule.setJobTrigger(JobTrigger.createNowTrigger());
        schedule.setSubject(overlord);
        schedule.setOperationName(task.getOperationName());
        new Configuration().setProperties(task.getParams().);
        schedule.setParameters(task.getParams().);

        operationManager.scheduleResourceOperation(overlord, schedule);

    }

    private void debug(String message) {
        log.info(message);
    }
}
