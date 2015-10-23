package org.rhq.enterprise.server.cloud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.Days;

import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.core.domain.cloud.StorageClusterSettings;
import org.rhq.core.domain.cloud.StorageClusterState.OperationStatus;
import org.rhq.core.domain.cloud.StorageNode;
import org.rhq.core.domain.cloud.StorageNode.OperationMode;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Configuration.Builder;
import org.rhq.core.domain.configuration.Configuration.Builder.ListInMap;
import org.rhq.core.domain.criteria.ResourceCriteria;
import org.rhq.core.domain.operation.OperationDefinition;
import org.rhq.core.domain.resource.Resource;
import org.rhq.core.domain.util.PageList;
import org.rhq.enterprise.server.resource.ResourceNotFoundException;
import org.rhq.enterprise.server.util.LookupUtil;

public class ClusterTaskFactory {

    private static final Log log = LogFactory.getLog(ClusterTaskFactory.class);

    public static String OP_SETMODE = "#setStorageNodeMode";
    public static String OP_SET_CLUSTER_OPERATION_STATUS = "#setClusterOperationStatus";
    public static String OP_NODE_ANNOUNCED = "#handleNodeAnnounced";
    public static String OP_NODE_BOOTSTRAPPED = "#handleNodeBootstrapped";
    public static String OP_NODE_MAINTENANCE_REMOVED = "#handleDecomission";
    public static String OP_NODE_UNANNOUNCED = "#handleUnannounced";
    public static String OP_NODE_REMOVED = "#handleRemoved";
    public static String OP_NODE_DISCOVERY = "#discovery";
    
    private static final int LONG_RUNNING_OPERATION_TIMEOUT = Days.SEVEN.toStandardSeconds().getSeconds();
    
    /**
     * create tasks for announcing phase. We are announcing node 'announced' to 'existingNodes'. These tasks first set the Cluster State to {@link OperationStatus#ADD_NODE}, then 
     * for each node in 'existingNodes' run "announce" resource Operation, then the last task, which handles
     * transition to next phase @see {@link #createBootstrap(List, StorageNode, StorageClusterSettings)}
     * @param existingNodes nodes in cluster
     * @param announced node being announced to cluster
     * @return
     */
    public static List<ClusterTask> createAnnounce(List<StorageNode> existingNodes, StorageNode announced) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        tasks.add(createOperationStatusTask(OperationStatus.ADD_NODE));
        tasks.add(createSetModeTask(announced, OperationMode.ANNOUNCE));
        for (StorageNode node : existingNodes) {
            tasks.add(createAnnounceTask(node, announced));
        }        
        tasks.add(new ClusterTask()
            .withOperationName(OP_NODE_ANNOUNCED)
            .withDescription("StorageNode was announced, move to BOOTSTRAP phase")
            .withStorageNodeId(announced.getId())
            );
        
        return tasks;
    }

    /**
     * create tasks for boostrap phase. We're bootstrapping 'storageNode' (this means sending it a seed list of all existing nodes in cluster
     * and telling it CQL and gossip ports). Last task handles transition to ADD_MAINTENANCE phase.
     * 
     * @param existingNodes existing nodes in cluster
     * @param storageNode node being bootstrapped
     * @param settings global cluster settings
     * @return
     */
    public static List<ClusterTask> createBootstrap(List<StorageNode> existingNodes, StorageNode storageNode, StorageClusterSettings settings) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        // take existing nodes and new node and share new seedlist
        existingNodes.add(storageNode);
        
        ListInMap<Builder> list = Configuration.builder()
            .addSimple("cqlPort", settings.getCqlPort())
            .addSimple("gossipPort", settings.getGossipPort())
            .openList("addresses", "address");
        for (StorageNode node : existingNodes) {
            list.addSimple(node.getAddress());
        }
        tasks.add(new ClusterTask()
            .withStorageNodeId(storageNode.getId())
            .withOperationName("prepareForBootstrap")
            .withParams(list.closeList().build()));
        
        tasks.add(createSetModeTask(storageNode, OperationMode.ADD_MAINTENANCE));
        
        tasks.add(new ClusterTask()
            .withDescription("Run discovery on "+storageNode.getAddress())
            .withOperationName(OP_NODE_DISCOVERY)
            .withStorageNodeId(storageNode.getId())
            .withResourceId(storageNode.getResource().getId())
        );

        tasks.add(new ClusterTask()
            .withOperationName(OP_NODE_BOOTSTRAPPED)
            .withDescription("StorageNode was bootstrapped, update schema if needed and move to ADD_MAINTENANCE phase")
            .withStorageNodeId(storageNode.getId()));
        
        return tasks;
    }

    /**
     * creates task to perform ADD_MAINTANANCE phase of storage node deployment
     * @param storageNodes existing nodes
     * @param newNode node being deployed
     * @param needRepair whether repair needs to run on all nodes
     * @return list of tasks
     */
    public static List<ClusterTask> createAddMaintenance(List<StorageNode> storageNodes, StorageNode newNode,
        boolean needRepair) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        tasks.add(createOperationStatusTask(OperationStatus.MAINTENANCE));
        // schedule for all nodes, but start with newNode
        List<StorageNode> allNodes = new ArrayList<StorageNode>(storageNodes.size() + 1);
        allNodes.add(newNode);
        allNodes.addAll(storageNodes);
        // for each node schedule
        // 1. repair (if needed)
        // 2. cleanup (always)
        // 3. udpateSeedList
        // repair and cleanup will be scheduled as separate steps for each keyspace we manage (system_auth, rhq)
        // finally we'll schedule setting mode of the last node as NORMAL (workflow finished)
        ListInMap<Builder> list = Configuration.builder()
            .openList("seedsList", "address");
        for (StorageNode node : allNodes) {
            list.addSimple(node.getAddress());
        }
        for (StorageNode node : allNodes) {
            if (needRepair) {
                tasks.addAll(createRepairTasksByKeyspace(node));
            }
            tasks.addAll(createCleanupTasksByKeyspace(node));
            tasks.add(new ClusterTask()
                .withDescription("Update seeds list for "+node.getAddress())
                .withOperationName("updateSeedsList")
                .withStorageNodeId(node.getId())
                .withParams(list.closeList().build().deepCopy())
            );
        }
        tasks.add(createSetModeTask(newNode, OperationMode.NORMAL));
        tasks.add(createOperationStatusTask(OperationStatus.IDLE));
        return tasks;
    }

    /**
     * schedules tasks for DECOMISSION and REMOVE_MAINTENANCE phases
     * @param clusterNodes
     * @param leaving
     * @param needRepair
     * @return
     */
    public static List<ClusterTask> createDecomission(List<StorageNode> clusterNodes,  StorageNode leaving, boolean needRepair) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        // - run decomission on node which is leaving
        // - switch to MAINTENANCE
        // - for each node left in cluster
        //  - run repair (if needed)
        //  - run cleanup
        //  - run updateSeedlist
        tasks.add(createOperationStatusTask(OperationStatus.REMOVE_NODE));
        tasks.add(createSetModeTask(leaving, OperationMode.DECOMMISSION));
        tasks.add(
            new ClusterTask()
                .withDescription("Run [decommission] operation on "+leaving.getAddress())
                .withStorageNodeId(leaving.getId())
                .withOperationName("decommission")
                .withParams(Configuration.builder().build())
            );
        tasks.add(createOperationStatusTask(OperationStatus.MAINTENANCE));
        tasks.add(createSetModeTask(leaving, OperationMode.REMOVE_MAINTENANCE));
        // new seedList (leaving node should not be present)
        ListInMap<Builder> list = Configuration.builder()
            .openList("seedsList", "address");
        for (StorageNode node : clusterNodes) {
            list.addSimple(node.getAddress());
        }
        for (StorageNode node : clusterNodes) {
            if (needRepair) {
                tasks.addAll(createRepairTasksByKeyspace(node));
            }
            tasks.addAll(createCleanupTasksByKeyspace(node));
            tasks.add(new ClusterTask()
                .withDescription("Update seeds list for "+node.getAddress())
                .withOperationName("updateSeedsList")
                .withStorageNodeId(node.getId())
                .withParams(list.closeList().build().deepCopy())
            );
        }
        tasks.add(new ClusterTask()
            .withOperationName(OP_NODE_MAINTENANCE_REMOVED)
            .withDescription("StorageNode decomission and REMOVE_MAINTENANCE phases are node, moving to UNANNOUNCE")
            .withStorageNodeId(leaving.getId()));
        return tasks;
    }
    
    public static List<ClusterTask> createUnannounce(List<StorageNode> clusterNodes, StorageNode leaving) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        tasks.add(createSetModeTask(leaving, OperationMode.UNANNOUNCE));
        
        ListInMap<Builder> list = Configuration.builder()
            .openList("addresses", "address");
        
        list.addSimple(leaving.getAddress());

        for (StorageNode node : clusterNodes) {
            tasks.add(new ClusterTask()
                .withStorageNodeId(node.getId())
                .withDescription("Run unannounce leaving node " + leaving.getAddress() + " on " + node.getAddress())
                .withOperationName("unannounce")
                .withParams(list.closeList().build())
                );
        }
        tasks.add(new ClusterTask()
            .withOperationName(OP_NODE_UNANNOUNCED)
            .withDescription("StorageNode UNANNOUNCE phase completed, moving to UNINSTALL phase")
            .withStorageNodeId(leaving.getId()));
        return tasks;
    }

    public static List<ClusterTask> createUninstall(StorageNode storageNode) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        tasks.add(new ClusterTask()
            .withDescription("Run [uninstall] operation on " + storageNode.getAddress())
            .withStorageNodeId(storageNode.getId())
            .withOperationName("uninstall")
            .withParams(Configuration.builder().build()));
        tasks.add(new ClusterTask()
            .withDescription("StorageNode UNINSTALLED, finish uninstallation")
            .withStorageNodeId(storageNode.getId())
            .withOperationName(OP_NODE_REMOVED));

        tasks.add(createOperationStatusTask(OperationStatus.IDLE));
        return tasks;
    }

    public static List<ClusterTask> createRepair(List<StorageNode> storageNodes) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        tasks.add(createOperationStatusTask(OperationStatus.MAINTENANCE));
        for (StorageNode node : storageNodes) {
            tasks.add(createSetModeTask(node, OperationMode.MAINTENANCE));
            tasks.addAll(createRepairTasksByKeyspace(node));
            tasks.add(createSetModeTask(node, OperationMode.NORMAL));
        }
        tasks.add(createOperationStatusTask(OperationStatus.IDLE));
        return tasks;
    }

    private static ClusterTask createOperationStatusTask(OperationStatus mode) {
        return new ClusterTask().withOperationName(OP_SET_CLUSTER_OPERATION_STATUS)
            .withDescription("Set Cluster Operation Status to " + mode)
            .withParams(Configuration.builder().addSimple("mode", mode).build());
    }

    private static ClusterTask createSetModeTask(StorageNode node, OperationMode mode) {
        return new ClusterTask().withOperationName(OP_SETMODE)
            .withDescription("Set " + node.getAddress() + " to " + mode)
            .withParams(Configuration.builder().addSimple("mode", mode).build()).withStorageNodeId(node.getId());
    }


    private static ClusterTask createAnnounceTask(StorageNode node, StorageNode announced) {
        Configuration parameters = Configuration.builder()
            .openList("addresses", "address")
            .addSimple(announced.getAddress())
            .closeList()
            .build();
        return new ClusterTask()
            .withDescription(
                "Announce new StorageNode " + announced.getAddress() + " to existing node " + node.getAddress())
            .withOperationName("announce")
            .withParams(parameters)
            .withStorageNodeId(node.getId());
    }
    
    private static ClusterTask createLongRunningStrorageNodeTask(StorageNode node, String operationName) {
        Configuration parameters = Configuration.builder()
            .addSimple(OperationDefinition.TIMEOUT_PARAM_NAME, LONG_RUNNING_OPERATION_TIMEOUT)
            .build();

        return new ClusterTask()
            .withDescription("Run "+operationName+" on " + node.getAddress())
            .withParams(parameters)
            .withOperationName(operationName)
            .withStorageNodeId(node.getId());
    }
    
    private static List<ClusterTask> createRepairTasksByKeyspace(StorageNode storageNode) {
        Configuration parameters = Configuration.builder()
            .addSimple(OperationDefinition.TIMEOUT_PARAM_NAME, LONG_RUNNING_OPERATION_TIMEOUT)
            .build();
        
        int rhqKeyspaceId;
        int systemAuthKeyspaceId;
        
        try {
            rhqKeyspaceId = findKeyspaceResourceId(storageNode, "rhq");
            systemAuthKeyspaceId = findKeyspaceResourceId(storageNode, "system_auth");
        } catch (ResourceNotFoundException rnfe) {
            // keyspaces might be uninventoried or not yet discovered, fallback to classic repair
            log.warn("Unable to schedule Cluster repair on " + storageNode
                + " on keyspace resources, defaulting to StorageNode operation, reason: " + rnfe.getMessage());
            return Arrays.asList(createLongRunningStrorageNodeTask(storageNode, "repair"));
        }
        return Arrays.asList(
            new ClusterTask()
                .withStorageNodeId(storageNode.getId())
                .withResourceId(systemAuthKeyspaceId)
                .withDescription("Run repair on "+storageNode.getAddress()+" on [system_auth] keyspace")
                .withOperationName("repair")
                .withParams(parameters.deepCopy()),
            new ClusterTask()
                .withStorageNodeId(storageNode.getId())
                .withResourceId(rhqKeyspaceId)
                .withDescription("Run repair on "+storageNode.getAddress()+" on [rhq] keyspace")
                .withOperationName("repair")
                .withParams(parameters.deepCopy())
            );
    }
    
    private static List<ClusterTask> createCleanupTasksByKeyspace(StorageNode storageNode) {
        int rhqKeyspaceId;
        int systemAuthKeyspaceId;
        
        try {
            rhqKeyspaceId = findKeyspaceResourceId(storageNode, "rhq");
            systemAuthKeyspaceId = findKeyspaceResourceId(storageNode, "system_auth");
        } catch (ResourceNotFoundException rnfe) {
            // keyspaces might be uninventoried or not yet discovered, fallback to classic cleanup
            log.warn("Unable to schedule Cluster cleanup on " + storageNode
                + " on keyspace resources, defaulting to StorageNode operation reason: " + rnfe.getMessage());
            return Arrays.asList(createLongRunningStrorageNodeTask(storageNode, "cleanup"));
        }
        Configuration parameters = Configuration.builder()
            .addSimple(OperationDefinition.TIMEOUT_PARAM_NAME, LONG_RUNNING_OPERATION_TIMEOUT)
            .build();
        return Arrays.asList(
            new ClusterTask()
                .withStorageNodeId(storageNode.getId())
                .withResourceId(systemAuthKeyspaceId)
                .withDescription("Run cleanup on "+storageNode.getAddress()+" on [system_auth] keyspace")
                .withOperationName("cleanup")
                .withParams(parameters.deepCopy()),
            new ClusterTask()
                .withStorageNodeId(storageNode.getId())
                .withResourceId(rhqKeyspaceId)
                .withDescription("Run cleanup on "+storageNode.getAddress()+" on [rhq] keyspace")
                .withOperationName("cleanup")
                .withParams(parameters.deepCopy())
            );
    }

    private static int findKeyspaceResourceId(StorageNode storageNode, String keyspaceName)
        throws ResourceNotFoundException {
        ResourceCriteria criteria = new ResourceCriteria();
        criteria.addFilterParentResourceId(storageNode.getResource().getId());
        criteria.addFilterPluginName("RHQStorage");
        criteria.addFilterResourceTypeName("Keyspace");
        criteria.addFilterName(keyspaceName);
        criteria.setStrict(true);
        PageList<Resource> resources = LookupUtil.getResourceManager().findResourcesByCriteria(
            LookupUtil.getSubjectManager().getOverlord(), criteria);
        if (resources.size() != 1) {
            throw new ResourceNotFoundException("Cannot find Keyspace [" + keyspaceName + "] child resource of "
                + storageNode.getResource());
        }
        return resources.get(0).getId();
    }

}
