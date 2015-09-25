package org.rhq.core.domain.cloud;

import java.util.ArrayList;
import java.util.List;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;

public class StorageClusterState {

    public enum Status {
        UP, DEGRADED, DOWN
    }

    public enum OperationStatus {
        IDLE, ADD_NODE, MAINTENANCE, REMOVE_NODE
    }

    private static final String PROP_STATUS = "status";
    private static final String PROP_OP_STATUS = "operationStatus";
    private static final String PROP_QUEUE = "queue";
    private static final String PROP_LAST_UPDATED = "lastUpdate";

    private final Configuration config;

    public StorageClusterState(Configuration config) {
        this.config = config;
    }

    public Configuration getConfig() {
        return config;
    }

    public void setStatus(Status status) {
        config.put(new PropertySimple(PROP_STATUS, status.name()));
    }

    public void setOperationStatus(OperationStatus status) {
        config.put(new PropertySimple(PROP_OP_STATUS, status.name()));
    }

    public void setLastUpdate() {
        config.put(new PropertySimple(PROP_LAST_UPDATED, System.currentTimeMillis()));
    }

    public long getPropLastUpdate() {
        PropertySimple prop = config.getSimple(PROP_LAST_UPDATED);
        return prop == null ? 0 : prop.getLongValue();
    }

    public Status getStatus() {
        PropertySimple prop = config.getSimple(PROP_STATUS);
        return prop == null ? null : Status.valueOf(prop.getStringValue());
    }

    public OperationStatus getOperationStatus() {
        PropertySimple prop = config.getSimple(PROP_OP_STATUS);
        return prop == null ? null : OperationStatus.valueOf(prop.getStringValue());
    }

    public void addTask(ClusterTask task) {
        task.createdNow();
        getQueue().add(task.getBackingMap());
    }

    public ClusterTask peekTask() {
        PropertyList queue = getQueue();
        if (queue.getList().isEmpty()) {
            return null;
        }
        return new ClusterTask((PropertyMap) queue.getList().get(0));
    }

    public ClusterTask pollTask() {
        PropertyList queue = getQueue();
        if (queue.getList().isEmpty()) {
            return null;
        }
        return new ClusterTask((PropertyMap) queue.getList().remove(0));
    }

    private PropertyList getQueue() {
        PropertyList queue = config.getList(PROP_QUEUE);
        if (queue == null) {
            queue = new PropertyList(PROP_QUEUE);
            config.put(queue);
        }
        return queue;
    }

    public List<ClusterTask> getTaskQueue() {
        PropertyList queue = getQueue();
        List<ClusterTask> tasks = new ArrayList<ClusterTask>(queue.getList().size());
        for (Property p : queue.getList()) {
            if (p instanceof PropertyMap) {
                tasks.add(new ClusterTask((PropertyMap) p));
            }
        }
        return tasks;
    }

    @Override
    public String toString() {
        return new StringBuilder("StorageClusterState[")
            .append(config.toString(true))
            .append("]")
            .toString();
    }
}
