package org.rhq.core.domain.cloud;

import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.operation.OperationRequestStatus;

public class ClusterTask {

    private static final String PROP_DESCRIPTION = "description";
    private static final String PROP_OPERATION_HISTORY = "operationHistory";
    private static final String PROP_RESOURCE = "resource";
    private static final String PROP_STORAGE_NODE = "storageNode";
    private static final String PROP_STATUS = "operationStatus";
    private static final String PROP_OPERATION_NAME = "operationName";
    private static final String PROP_PARAMS = "params";

    private final PropertyMap backingMap;

    public ClusterTask() {
        this.backingMap = new PropertyMap("task");
    }

    public ClusterTask(PropertyMap backingMap) {
        this.backingMap = backingMap;
    }

    public PropertyMap getBackingMap() {
        return backingMap;
    }

    public Integer getStorageNodeId() {
        PropertySimple prop = this.backingMap.getSimple(PROP_STORAGE_NODE);
        return prop == null ? null : prop.getIntegerValue();
    }

    public ClusterTask withStorageNodeId(Integer storageNodeId) {
        backingMap.put(new PropertySimple(PROP_STORAGE_NODE, storageNodeId));
        return this;
    }

    public Integer getOperationHistoryId() {
        PropertySimple prop = this.backingMap.getSimple(PROP_OPERATION_HISTORY);
        return prop == null ? null : prop.getIntegerValue();
    }

    public ClusterTask withOperationHistoryId(Integer operationHistoryId) {
        backingMap.put(new PropertySimple(PROP_OPERATION_HISTORY, operationHistoryId));
        return this;
    }

    public Integer getResourceId() {
        PropertySimple prop = this.backingMap.getSimple(PROP_RESOURCE);
        return prop == null ? null : prop.getIntegerValue();
    }

    public ClusterTask withResourceId(Integer resourceId) {
        backingMap.put(new PropertySimple(PROP_RESOURCE, resourceId));
        return this;
    }

    public OperationRequestStatus getStatus() {
        PropertySimple prop = this.backingMap.getSimple(PROP_STATUS);
        return prop == null ? null : OperationRequestStatus.valueOf(prop.getStringValue());
    }

    public ClusterTask withStatus(OperationRequestStatus status) {
        backingMap.put(new PropertySimple(PROP_STATUS, status.name()));
        return this;
    }

    public String getOperationName() {
        PropertySimple prop = this.backingMap.getSimple(PROP_OPERATION_NAME);
        return prop == null ? null : prop.getStringValue();
    }

    public ClusterTask withOperationName(String operationName) {
        backingMap.put(new PropertySimple(PROP_OPERATION_NAME, operationName));
        return this;
    }

    public PropertyMap getParams() {
        return (PropertyMap) backingMap.get(PROP_PARAMS);
    }

    public ClusterTask setParams(PropertyMap params) {
        params.setName(PROP_PARAMS);
        backingMap.put(params);
        return this;
    }

    public String getDescription() {
        return backingMap.getSimpleValue(PROP_DESCRIPTION, null);
    }

    public ClusterTask withDescription(String description) {
        backingMap.put(new PropertySimple(PROP_DESCRIPTION, description));
        return this;
    }

}
