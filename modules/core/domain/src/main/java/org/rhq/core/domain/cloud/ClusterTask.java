package org.rhq.core.domain.cloud;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.operation.OperationRequestStatus;

public class ClusterTask {

    private static final String PROP_CREATED = "created";
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
        createdNow();
    }

    ClusterTask(PropertyMap backingMap) {
        this.backingMap = backingMap;
    }

    public PropertyMap getBackingMap() {
        return backingMap;
    }

    public long getCreated() {
        PropertySimple prop = this.backingMap.getSimple(PROP_CREATED);
        return prop == null ? 0 : prop.getLongValue();
    }

    public ClusterTask createdNow() {
        backingMap.put(new PropertySimple(PROP_CREATED, System.currentTimeMillis()));
        return this;
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


    public Configuration getParams() {
        Configuration c = new Configuration();
        for (Property p : ((PropertyMap) backingMap.get(PROP_PARAMS)).getMap().values()) {
            c.put(p.deepCopy(false));
        }
        return c;
    }

    public ClusterTask withParams(Configuration params) {
        params = params.deepCopy(false);
        PropertyMap map = new PropertyMap(PROP_PARAMS);
        for (Property p : params.getMap().values()) {
            // disconnect from original 
            p.setConfiguration(null);
            p.setParentMap(map);
            map.put(p);
        }
        this.backingMap.put(map);
        return this;
    }

    public String getDescription() {
        return backingMap.getSimpleValue(PROP_DESCRIPTION, null);
    }

    public ClusterTask withDescription(String description) {
        backingMap.put(new PropertySimple(PROP_DESCRIPTION, description));
        return this;
    }
    
    @Override
    public String toString() {
        return new StringBuilder("ClusterTask[")
            .append("created="+getCreated())
            .append(", description="+getDescription())
            .append(", operationHistory="+getOperationHistoryId())
            .append(", status="+getStatus())
            .append(", storageNode="+getStorageNodeId())
            .append(", resource="+getResourceId())
            .append(", operationName="+getOperationName())
            .append("]")
            .toString();
    }

}
