package org.rhq.enterprise.server.cloud;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.Days;

import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.core.domain.cloud.StorageClusterState.OperationStatus;
import org.rhq.core.domain.cloud.StorageNode;
import org.rhq.core.domain.cloud.StorageNode.OperationMode;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.operation.OperationDefinition;

public class ClusterTaskFactory {

    public static String OP_SETMODE = "#setStorageNodeMode";
    public static String OP_SET_CLUSTER_OPERATION_STATUS = "#setClusterOperationStatus";

    public static List<ClusterTask> createRepair(List<StorageNode> storageNodes) {
        List<ClusterTask> tasks = new ArrayList<ClusterTask>();
        tasks.add(createOperationStatusTask(OperationStatus.MAINTENANCE));
        for (StorageNode node : storageNodes) {
            tasks.add(createSetModeTask(node, OperationMode.MAINTENANCE));
            tasks.add(createRepairTask(node));
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

    private static ClusterTask createRepairTask(StorageNode node) {
        Configuration parameters = Configuration.builder()
            .addSimple(OperationDefinition.TIMEOUT_PARAM_NAME, Days.SEVEN.toStandardSeconds().getSeconds()).build();

        return new ClusterTask().withDescription("Run repair on " + node.getAddress()).withParams(parameters)
            .withOperationName("repair").withStorageNodeId(node.getId());
    }
}
