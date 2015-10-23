/*
 *
 *  * RHQ Management Platform
 *  * Copyright (C) 2005-2014 Red Hat, Inc.
 *  * All rights reserved.
 *  *
 *  * This program is free software; you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License, version 2, as
 *  * published by the Free Software Foundation, and/or the GNU Lesser
 *  * General Public License, version 2.1, also as published by the Free
 *  * Software Foundation.
 *  *
 *  * This program is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  * GNU General Public License and the GNU Lesser General Public License
 *  * for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * and the GNU Lesser General Public License along with this program;
 *  * if not, write to the Free Software Foundation, Inc.,
 *  * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 *
 */

package org.rhq.plugins.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperic.sigar.SigarException;
import org.mc4j.ems.connection.EmsConnection;
import org.mc4j.ems.connection.EmsInvocationException;
import org.mc4j.ems.connection.bean.EmsBean;
import org.mc4j.ems.connection.bean.attribute.EmsAttribute;
import org.mc4j.ems.connection.bean.operation.EmsOperation;
import org.yaml.snakeyaml.error.YAMLException;

import org.rhq.cassandra.util.ConfigEditor;
import org.rhq.cassandra.util.ConfigEditorException;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.ConfigurationUpdateStatus;
import org.rhq.core.domain.configuration.Property;
import org.rhq.core.domain.configuration.PropertyList;
import org.rhq.core.domain.configuration.PropertyMap;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.pluginapi.configuration.ConfigurationFacet;
import org.rhq.core.pluginapi.configuration.ConfigurationUpdateReport;
import org.rhq.core.pluginapi.inventory.ProcessScanResult;
import org.rhq.core.pluginapi.inventory.ResourceContext;
import org.rhq.core.pluginapi.operation.OperationFacet;
import org.rhq.core.pluginapi.operation.OperationResult;
import org.rhq.core.system.ProcessInfo;
import org.rhq.core.util.StringUtil;
import org.rhq.core.util.exception.ThrowableUtil;
import org.rhq.core.util.file.FileUtil;
import org.rhq.core.util.stream.StreamUtil;
import org.rhq.plugins.cassandra.CassandraNodeComponent;
import org.rhq.plugins.cassandra.util.KeyspaceService;

/**
 * @author John Sanda
 */
public class StorageNodeComponent extends CassandraNodeComponent implements OperationFacet, ConfigurationFacet {

    private Log log = LogFactory.getLog(StorageNodeComponent.class);

    private static final String SYSTEM_AUTH_KEYSPACE = "system_auth";

    private static final String RHQ_KEYSPACE = "rhq";

    private static final String SYSTEM_KEYSPACE = "system";

    @Override
    public Configuration loadResourceConfiguration() throws Exception {
        return new StorageNodeConfigDelegate(getBasedir(), this).loadResourceConfiguration();
    }

    @Override
    public void updateResourceConfiguration(ConfigurationUpdateReport configurationUpdateReport) {
        StorageNodeConfigDelegate configDelegate = new StorageNodeConfigDelegate(getBasedir(), this);
        configDelegate.updateResourceConfiguration(configurationUpdateReport);
    }

    private OperationResult shutdownIfNecessary() {
        log.info("Shutting down " + getResourceContext().getResourceKey());

        ProcessInfo process = getResourceContext().getNativeProcess();
        if (process == null) {
            File pidFile = new File(getBinDir(), "cassandra.pid");
            if (pidFile.exists()) {
                return shutdownStorageNode();
            } else {
                return new OperationResult("Storage node is not running");
            }
        } else {
            return shutdownStorageNode();
        }
    }

    private File getBasedir() {
        Configuration pluginConfig = getResourceContext().getPluginConfiguration();
        return new File(pluginConfig.getSimpleValue("baseDir"));
    }

    private File getBinDir() {
        return new File(getBasedir(), "bin");
    }

    private File getConfDir() {
        return new File(getBasedir(), "conf");
    }

    private File getInternodeAuthConfFile() {
        return new File(getConfDir(), "rhq-storage-auth.conf");
    }

    @Override
    public OperationResult invokeOperation(String name, Configuration parameters) throws Exception {
        if (name.equals("addNodeMaintenance")) {
            return nodeAdded(parameters);
        } else if (name.equals("removeNodeMaintenance")) {
            return nodeRemoved(parameters);
        } else if (name.equals("prepareForUpgrade")) {
            return prepareForUpgrade(parameters);
        } else if (name.equals("repair")) {
            return repair();
        } else if (name.equals("updateConfiguration")) {
            return updateConfiguration(parameters);
        } else if (name.equals("announce")) {
            return announce(parameters);
        } else if (name.equals("unannounce")) {
            return unannounce(parameters);
        } else if (name.equals("prepareForBootstrap")) {
            return prepareForBootstrap(parameters);
        } else if (name.equals("shutdown")) {
            return shutdownStorageNode();
        } else if (name.equals("decommission")) {
            return decommission();
        } else if (name.equals("uninstall")) {
            return uninstall();
        } else if (name.equals("moveDataFiles")) {
            return moveDataFiles(parameters);
        } else {
            return super.invokeOperation(name, parameters);
        }
    }

    private OperationResult shutdownStorageNode() {
        OperationResult result = new OperationResult();
        File binDir = new File(getBasedir(), "bin");
        File pidFile = new File(binDir, "cassandra.pid");

        try {
            if (pidFile.exists()) {
                long pid = readPidFile(pidFile);
                log.info("Shutting down storage node with pid " + pid);
                ProcessInfo process = findProcessInfo(pid);
                if (process != null) {
                    try {
                        process.kill("KILL");
                        waitForNodeToGoDown();
                        pidFile.delete();
                        result.setSimpleResult("Successfully storage node with pid " + pid);
                    } catch (SigarException e) {
                        log.error("Failed to delete storage node with pid " + process.getPid(), e);
                        result.setErrorMessage("Failed to delete storage node with pid " + pid + ": " +
                            ThrowableUtil.getAllMessages(e));
                    }
                } else {
                    log.warn("Could not find process info for pid " + pid);
                    result = shutdownUsingNativeProcessInfo();
                }

            } else {
                log.warn("Did not find pid file " + pidFile + ". It should not be modified, deleted, or moved.");
                result = shutdownUsingNativeProcessInfo();
            }
        } catch (FileNotFoundException e) {
            log.error("Could not read pid file " + pidFile, e);
            result.setErrorMessage("Could not read pid file " + pidFile + ": " + ThrowableUtil.getAllMessages(e));
        } catch (InterruptedException e) {
            log.warn("The shutdown operation was cancelled or interrupted. This interruption occurred while trying " +
                "to verify that the storage node process has exited.");
            result.setErrorMessage("The operation was cancelled or interrupted while trying to verify that the " +
                "storage node process has exited.");
            Thread.currentThread().interrupt();
        }
        return result;
    }

    private long readPidFile(File pidFile) throws FileNotFoundException {
        return Long.parseLong(StreamUtil.slurp(new FileReader(pidFile)));
    }

    @SuppressWarnings("unchecked")
    private ProcessInfo findProcessInfo(long pid) {
        List<ProcessScanResult> scanResults = getResourceContext().getNativeProcessesForType();

        for (ProcessScanResult scanResult : scanResults) {
            if (scanResult.getProcessInfo().getPid() == pid) {
                return scanResult.getProcessInfo();
            }
        }
        return null;
    }

    private OperationResult shutdownUsingNativeProcessInfo() throws InterruptedException {
        log.warn("Could not obtain process info from pid file");
        log.info("Obtaining process info from the system to perform the shutdown");

        OperationResult result = shutdownNode();
        waitForNodeToGoDown();

        return result;
    }

    private OperationResult updateConfiguration(Configuration params) {
        //update storage node jvm settings only
        Configuration config = new Configuration();
        Configuration.builder().addSimple("jmxPort", params.getSimpleValue("jmxPort"))
            .addSimple("jmxPort", params.getSimpleValue("jmxPort"))
            .addSimple("minHeapSize", params.getSimpleValue("heapSize"))
            .addSimple("maxHeapSize", params.getSimpleValue("heapSize"))
            .addSimple("heapNewSize", params.getSimpleValue("heapNewSize"))
            .addSimple("threadStackSize", params.getSimpleValue("threadStackSize"))
            .addSimple("maxHeapSize", params.getSimpleValue("heapSize")).build();

        config.put(new PropertySimple("jmxPort", params.getSimpleValue("jmxPort")));
        config.put(new PropertySimple("minHeapSize", params.getSimpleValue("heapSize")));
        config.put(new PropertySimple("maxHeapSize", params.getSimpleValue("heapSize")));
        config.put(new PropertySimple("heapNewSize", params.getSimpleValue("heapNewSize")));
        config.put(new PropertySimple("threadStackSize", params.getSimpleValue("threadStackSize")));

        String restartIfRequiredString = params.getSimpleValue("restartIfRequired");
        boolean restartIfRequired = restartIfRequiredString != null && Boolean.parseBoolean(restartIfRequiredString);

        ConfigurationUpdateReport configurationUpdateReport = new ConfigurationUpdateReport(config);
        StorageNodeConfigDelegate configDelegate = new StorageNodeConfigDelegate(getBasedir(), this);
        configDelegate.updateResourceConfigurationAndRestartIfNecessary(configurationUpdateReport, restartIfRequired);

        OperationResult result = new OperationResult("Configuration updated.");
        if (!configurationUpdateReport.getStatus().equals(ConfigurationUpdateStatus.SUCCESS)) {
            result.setErrorMessage(configurationUpdateReport.getErrorMessage());
        }

        return result;
    }

    private OperationResult decommission() {
        log.info("Decommissioning " + getResourceContext().getResourceKey());

        OperationResult result = new OperationResult();
        try {
            EmsConnection emsConnection = getEmsConnection();
            EmsBean storageService = emsConnection.getBean("org.apache.cassandra.db:type=StorageService");

            EmsAttribute operationModeAttr = storageService.getAttribute("OperationMode");
            String operationMode = (String) operationModeAttr.refresh();
            if (operationMode.equals("DECOMMISSIONED")) {
                log.info("The storage node " + getHost() + " is already decommissioned.");
            } else {
                Class<?>[] emptyParams = new Class<?>[0];
                EmsOperation operation = storageService.getOperation("decommission", emptyParams);
                operation.invoke((Object[]) emptyParams);

                operationMode = (String) operationModeAttr.refresh();
                if (!operationMode.equals("DECOMMISSIONED")) {
                    result.setErrorMessage("Failed to decommission storage node " + getHost() + ". The " +
                        "StorageService is reporting " + operationMode + " for its operation mode but it should be " +
                        "reporting DECOMMISSIONED. The StorageService operation mode is not to be confused with the " +
                        "Storage Node operation mode.");
                }
            }
        } catch (EmsInvocationException e) {
            result.setErrorMessage("Decommission operation failed: " + ThrowableUtil.getAllMessages(e));
        }
        return result;
    }

    private OperationResult uninstall() {
        log.info("Uninstalling storage node at " + getResourceContext().getResourceKey());

        OperationResult result = new OperationResult();
        OperationResult shutdownResult = shutdownIfNecessary();
        if (shutdownResult.getErrorMessage() != null) {
            result.setErrorMessage("Failed to shut down storage node: " + shutdownResult.getErrorMessage());
        } else {
            File basedir = getBasedir();
            if (basedir.exists()) {
                log.info("Purging data directories");
                ConfigEditor yamlEditor = getYamlConfigEditor();
                yamlEditor.load();
                purgeDataDirs(yamlEditor);

                log.info("Purging installation directory " + basedir);
                purgeDir(basedir);

                log.info("Finished deleting storage node " + getResourceContext().getResourceKey());
            } else {
                log.info(basedir + " does not exist. Storage node files have already been purged.");
            }
        }
        return result;
    }

    private OperationResult announce(Configuration params) {
        OperationResult result = new OperationResult();
        Set<String> addressesToAdd = null;
        try {
            addressesToAdd = getAddreses(params);
            log.info("Announcing "  + addressesToAdd);

            createSnapshots(addressesToAdd, "pre_" + StringUtil.collectionToString(addressesToAdd) + "_bootstrap_");

            Set<String> knownAddresses = getAuthAddresses();
            knownAddresses.addAll(addressesToAdd);
            setAuthAddresses(knownAddresses);
            reloadInternodeAuthConfig();

            result.getComplexResults().put(new PropertySimple("details",
                "Successfully announced " + addressesToAdd));
        } catch (InternodeAuthConfUpdateException e) {
            result.setErrorMessage("Failed to update authorized nodes due to the following error(s): " +
                ThrowableUtil.getAllMessages(e));
        }
        return result;
    }

    private OperationResult unannounce(Configuration params) {
        OperationResult result = new OperationResult();
        Set<String> addressesToRemove = null;
        try {
            addressesToRemove = getAddreses(params);
            log.info("Unannouncing " + addressesToRemove);

            createSnapshots(addressesToRemove, "pre_" + StringUtil.collectionToString(addressesToRemove) +
                "_decommission_");

            Set<String> knownAddresses = getAuthAddresses();
            knownAddresses.removeAll(addressesToRemove);
            setAuthAddresses(knownAddresses);
            reloadInternodeAuthConfig();

            result.getComplexResults().put(new PropertySimple("details",
                "Successfully unannounced " + addressesToRemove));
        } catch (InternodeAuthConfUpdateException e) {
            result.setErrorMessage("Failed to update authorized nodes due to the following error(s): " +
                ThrowableUtil.getAllMessages(e));
        }
        return result;
    }

    private Set<String> getAddreses(Configuration params) {
        PropertyList propertyList = params.getList("addresses");
        Set<String> ipAddresses = new HashSet<String>();

        for (Property property : propertyList.getList()) {
            PropertySimple propertySimple = (PropertySimple) property;
            ipAddresses.add(propertySimple.getStringValue());
        }

        return ipAddresses;
    }

    private void createSnapshots(Set<String> addressesToAdd, String snapshotPrefix) {
        KeyspaceService keyspaceService = new KeyspaceService(getEmsConnection());
        keyspaceService.takeSnapshot(new String[] { SYSTEM_KEYSPACE, SYSTEM_AUTH_KEYSPACE, RHQ_KEYSPACE },
            snapshotPrefix + System.currentTimeMillis());
    }

    private void reloadInternodeAuthConfig() {
        EmsBean authBean = getEmsConnection().getBean("org.rhq.cassandra.auth:type=RhqInternodeAuthenticator");
        EmsOperation emsOperation = authBean.getOperation("reloadConfiguration");
        emsOperation.invoke();
    }

    @SuppressWarnings("deprecation")
    private OperationResult updateKnownNodes(Configuration params) {
        OperationResult result = new OperationResult();

        PropertyList propertyList = params.getList("addresses");
        Set<String> ipAddresses = new HashSet<String>();

        for (Property property : propertyList.getList()) {
            PropertySimple propertySimple = (PropertySimple) property;
            ipAddresses.add(propertySimple.getStringValue());
        }

        try {
            updateInternodeAuthConfFile(ipAddresses);

            EmsBean authBean = getEmsConnection().getBean("org.rhq.cassandra.auth:type=RhqInternodeAuthenticator");
            EmsOperation emsOperation = authBean.getOperation("reloadConfiguration");
            emsOperation.invoke();

            Configuration complexResults = result.getComplexResults();
            complexResults.put(new PropertySimple("details", "Successfully updated the set of known nodes."));

            return result;
        } catch (InternodeAuthConfUpdateException e) {
            File authFile = getInternodeAuthConfFile();
            log.error("Failed to update set of trusted nodes in " + authFile + " due to the following error(s): " +
                ThrowableUtil.getAllMessages(e)) ;
            result.setErrorMessage("Failed to update set of trusted nodes in " + authFile + " due to the following " +
                "error(s): " + ThrowableUtil.getAllMessages(e));
            return result;
        }
    }

    @SuppressWarnings("rawtypes")
    private OperationResult prepareForBootstrap(Configuration params) {
        log.info("Preparing " + this + " for bootstrap...");

        ResourceContext context = getResourceContext();
        OperationResult result = new OperationResult();

        log.info("Stopping storage node");
        OperationResult shutdownResult = shutdownIfNecessary();
        if (shutdownResult.getErrorMessage() != null) {
            log.error("Failed to stop storage node " + getResourceContext().getResourceKey() + ". The storage node " +
                "must be shut down in order for the changes made by this operation to take effect.");
            result.setErrorMessage("Failed to stop the storage node. The storage node must be shut down in order " +
                "for the changes made by this operation to take effect. The attempt to stop shut down the storage " +
                "node failed with this error: " + shutdownResult.getErrorMessage());
            return result;
        }

        Configuration pluginConfig = context.getPluginConfiguration();
        ConfigEditor configEditor = getYamlConfigEditor();

        try {
            configEditor.load();

            purgeDataDirs(configEditor);

            log.info("Updating cluster settings");

            String address = pluginConfig.getSimpleValue("host");
            int cqlPort = Integer.parseInt(params.getSimpleValue("cqlPort"));
            int gossipPort = Integer.parseInt(params.getSimpleValue("gossipPort"));
            List<String> addresses = getAddresses(params.getList("addresses"));

            // Make sure this node's address is not in the list; otherwise, it
            // won't bootstrap properly.
            List<String> seeds = new ArrayList<String>(addresses);
            seeds.remove(address);

            log.info("Updating seeds property to " + seeds);

            configEditor.setSeeds(seeds.toArray(new String[seeds.size()]));
            configEditor.setNativeTransportPort(cqlPort);
            configEditor.setStoragePort(gossipPort);

            configEditor.save();
            log.info("Cluster configuration settings have been applied to " + configEditor.getConfigFile());

            updateInternodeAuthConfFile(new HashSet<String>(addresses));

            log.info(this + " is ready to be bootstrap. Restarting storage node...");
            OperationResult startResult = startNode();
            if (startResult.getErrorMessage() != null) {
                log.error("Failed to restart storage node:\n" + startResult.getErrorMessage());
                result.setErrorMessage("Failed to restart storage node:\n" + startResult.getErrorMessage());
            } else {
                result.setSimpleResult("The storage node was successfully updated is now bootstrapping into the cluster.");
            }

            return result;
        } catch (ConfigEditorException e) {
            log.error("There was an error while trying to update " + configEditor.getConfigFile(), e);
            if (e.getCause() instanceof YAMLException) {
                log.info("Attempting to restore " + configEditor.getConfigFile());
                try {
                    configEditor.restore();
                    result.setErrorMessage("Failed to update configuration file [" + configEditor.getConfigFile() + "]: " +
                        ThrowableUtil.getAllMessages(e.getCause()));
                } catch (ConfigEditorException e1) {
                    log.error("Failed to restore " + configEditor.getConfigFile() + ". A copy of the file prior to any modifications " +
                        "can be found at " + configEditor.getBackupFile());
                    result.setErrorMessage("There was an error updating [" + configEditor.getConfigFile() + "] and undoing the changes " +
                        "Failed. A copy of the file can be found at " + configEditor.getBackupFile() + ". See the " +
                        "agent logs for more details");
                }
            }

            EmsConnection emsConnection = getEmsConnection();
            EmsBean storageService = emsConnection.getBean("org.apache.cassandra.db:type=StorageService");
            EmsAttribute attribute = storageService.getAttribute("OperationMode");
            String operationMode = (String) attribute.refresh();

            if (!operationMode.equals("NORMAL")) {
                result.setErrorMessage("Bootstrapping " + getHost() + " failed. The StorageService is reporting " +
                    operationMode + " for its operation mode but it should be reporting NORMAL. The StorageService " +
                    "operation mode is not to be confused with the Storage Node operation mode.");
            }
            return result;
        } catch (InternodeAuthConfUpdateException e) {
            File authFile = getInternodeAuthConfFile();
            result.setErrorMessage("Failed to update " + authFile + " due to the following error(s): " +
                ThrowableUtil.getAllMessages(e));
            return result;
        }
    }

    /**
     * @param origDir Current data directory in the config
     * @param destDir Potential new data directory
     * @return true if files were moved
     */
    private boolean copyDataDirectoryIfChanged(String origDir, String destDir) throws IOException {
        if(destDir != null && destDir.length() > 0 && !origDir.equals(destDir)) {
            log.debug("Moving data from " + origDir + " to " + destDir);
            File currentDir = new File(origDir);
            File newDir = new File(destDir);

            FileUtil.copyDirectory(currentDir, newDir);
            return true;
        }
        return false;
    }

    private OperationResult moveDataFiles(Configuration params) {
        ResourceContext context = getResourceContext();
        OperationResult result = new OperationResult();

        log.info("Preparing to move " + this + "'s datafiles to new locations");

        String newCommitLogDirectory = params.getSimpleValue("CommitLogLocation");
        String newSavedCachesDirectory = params.getSimpleValue("SavedCachesLocation");
        PropertyList allDataFileLocations = params.getList("AllDataFileLocations");
        String newDataFileDirectory = null;
        if(allDataFileLocations != null) {
            List<String> dataDirectories = new LinkedList<String>();
            for (Property property : allDataFileLocations.getList()) {
                PropertySimple dataFileLocation = (PropertySimple) property;
                dataDirectories.add(dataFileLocation.getStringValue());
            }
            if(dataDirectories.size() > 1) {
                result.setErrorMessage("This process does not support more than one active directory for StorageNode data locations. ");
                return result;
            }
            newDataFileDirectory = dataDirectories.get(0);
        }

        if(newCommitLogDirectory == null && newSavedCachesDirectory == null && newDataFileDirectory == null) {
            return new OperationResult("No new directories were specified");
        }


        log.info("Stopping storage node");
        OperationResult shutdownResult = super.shutdownNode(); // CassandraNodeComponent.shutDownNode() does draining before shutting down
        try {
            waitForNodeToGoDown();
        } catch (InterruptedException e) {
            log.error("Received " + e.getLocalizedMessage() + " while waiting for storage node " + getResourceContext().getResourceKey() + " to shutdown", e);
            result.setErrorMessage("Failed to stop the storage node. The storage node must be shut down in order " +
                    "for the changes made by this operation to take effect. The attempt to stop shut down the storage " +
                    "node failed with this error: " + shutdownResult.getErrorMessage());
            return result;
        }
        if (shutdownResult.getErrorMessage() != null) {
            log.error("Failed to stop storage node " + getResourceContext().getResourceKey() + ". The storage node " +
                    "must be shut down in order for the changes made by this operation to take effect.");
            result.setErrorMessage("Failed to stop the storage node. The storage node must be shut down in order " +
                    "for the changes made by this operation to take effect. The attempt to stop shut down the storage " +
                    "node failed with this error: " + shutdownResult.getErrorMessage());
            return result;
        }

        log.info("Storage node shutdown, preparing to move datafiles");

        List<String> originalDataDirectories = new LinkedList<String>();
        List<String> createdDataDirectories = new LinkedList<String>();

        ConfigEditor configEditor = getYamlConfigEditor();
        try {
            configEditor.load();

            // Moving the data directory
            List<String> dataFileDirectories = configEditor.getDataFileDirectories();
            if(dataFileDirectories.size() > 1) {
                // We do not support this scenario
                log.error("More than one datadirectory configured for the StorageNode. This operation mode is not supported by this tool");
                StringBuilder pathListBuilder = new StringBuilder();
                for (String dataFileDir : dataFileDirectories) {
                    pathListBuilder.append(dataFileDir).append(", ");
                }
                result.setErrorMessage("Could not proceed with moving datafiles from " + pathListBuilder.toString() + "this tool does not support"
                        + " multiple datafile paths.");
                return result;
            } else if(dataFileDirectories.size() == 1) {
                String currentDataFileLocation = dataFileDirectories.get(0);
                boolean dataFilesMoved = copyDataDirectoryIfChanged(currentDataFileLocation, newDataFileDirectory);
                if(dataFilesMoved) {
                    originalDataDirectories.add(currentDataFileLocation);
                    createdDataDirectories.add(newDataFileDirectory);
                    List<String> newDataFileDirectories = new LinkedList<String>();
                    newDataFileDirectories.add(newDataFileDirectory);
                    configEditor.setDataFileDirectories(newDataFileDirectories);
                }
            }

            // In theory we wouldn't need to copy these, as draining should empty these
            String currentCommitLogDirectory = configEditor.getCommitLogDirectory();

            boolean commitLogCopied = copyDataDirectoryIfChanged(currentCommitLogDirectory, newCommitLogDirectory);
            if(commitLogCopied) {
                originalDataDirectories.add(currentCommitLogDirectory);
                createdDataDirectories.add(newCommitLogDirectory);
                configEditor.setCommitLogDirectory(newCommitLogDirectory);
            }

            // Not so dangerous if we lose these, but lets try to keep them
            String currentSavedCachesDirectory = configEditor.getSavedCachesDirectory();

            boolean savedCachesCopied = copyDataDirectoryIfChanged(currentSavedCachesDirectory, newSavedCachesDirectory);
            if(savedCachesCopied) {
                originalDataDirectories.add(currentSavedCachesDirectory);
                createdDataDirectories.add(newSavedCachesDirectory);
                configEditor.setSavedCachesDirectory(newSavedCachesDirectory);
            }

            log.info(this + " datafiles have been moved. Restarting storage node...");
            OperationResult startResult = startNode();
            if (startResult.getErrorMessage() != null) {
                log.error("Failed to restart storage node:\n" + startResult.getErrorMessage());
                result.setErrorMessage("Failed to restart storage node:\n" + startResult.getErrorMessage());
                // rollback here
                configEditor.restore();
                purgeDirectories(createdDataDirectories);
            } else {
                result.setSimpleResult("The storage node was succesfully updated.");
                // Commit changes, remove old directories
                configEditor.save(); // This can still throw an exception, in which case we need to rollback
                purgeDirectories(originalDataDirectories);
            }

            return result;
        } catch (ConfigEditorException e) {
            log.error("There was an error while trying to update " + configEditor.getConfigFile(), e);
            if (e.getCause() instanceof YAMLException) {
                log.info("Attempting to restore " + configEditor.getConfigFile());
                try {
                    configEditor.restore();
                    purgeDirectories(createdDataDirectories);
                    result.setErrorMessage("Failed to update configuration file [" + configEditor.getConfigFile() + "]: " +
                            ThrowableUtil.getAllMessages(e.getCause()));
                } catch (ConfigEditorException e1) {
                    log.error("Failed to restore " + configEditor.getConfigFile() + ". A copy of the file prior to any modifications " +
                            "can be found at " + configEditor.getBackupFile());
                    result.setErrorMessage("There was an error updating [" + configEditor.getConfigFile() + "] and undoing the changes " +
                            "Failed. A copy of the file can be found at " + configEditor.getBackupFile() + ". See the " +
                            "agent logs for more details");
                }
            }

            EmsConnection emsConnection = getEmsConnection();
            EmsBean storageService = emsConnection.getBean("org.apache.cassandra.db:type=StorageService");
            EmsAttribute attribute = storageService.getAttribute("OperationMode");
            String operationMode = (String) attribute.refresh();

            if (!operationMode.equals("NORMAL")) {
                result.setErrorMessage("Bootstrapping " + getHost() + " failed. The StorageService is reporting " +
                        operationMode + " for its operation mode but it should be reporting NORMAL. The StorageService " +
                        "operation mode is not to be confused with the Storage Node operation mode.");
            }
            return result;
        } catch (IOException e) {
            log.error("Moving datafiles failed", e);
            purgeDirectories(createdDataDirectories);
            configEditor.restore();
            result.setErrorMessage("Failed to move all the files to new destinations, " + e.getLocalizedMessage() + ". StorageService was left offline" +
                    ", investigate before restarting the node");
//            OperationResult startResult = startNode(); // return the StorageNode online, but what if IOException was out of diskspace?
            return result;
        }
    }

    private void purgeDirectories(List<String> directories) {
        for(String dir : directories) {
            File directory = new File(dir);
            purgeDir(directory);
        }
    }

    private void purgeDataDirs(ConfigEditor configEditor) {
        purgeDir(new File(configEditor.getCommitLogDirectory()));
        purgeDir(new File(configEditor.getCommitLogDirectory()));
        for (String path : configEditor.getDataFileDirectories()) {
            purgeDir(new File(path));
        }
        purgeDir(new File(configEditor.getSavedCachesDirectory()));
    }

    private void purgeDir(File dir) {
        if (dir.isAbsolute()) {
            log.info("Purging " + dir);
            FileUtil.purge(dir, true);
        } else {
            File relativeDir = new File(getBinDir(), dir.getPath());
            log.info("Purging " + relativeDir);
            FileUtil.purge(relativeDir, true);
        }
    }

    private Set<String> getAuthAddresses() throws InternodeAuthConfUpdateException {
        File authFile = null;
        try {
            authFile = getInternodeAuthConfFile();
            String contents = StreamUtil.slurp(new FileReader(authFile));
            Set<String> addresses = new TreeSet<String>();
            for (String address : contents.split("\\n")) {
                addresses.add(address);
            }
            return addresses;
        } catch (FileNotFoundException e) {
            throw new InternodeAuthConfUpdateException("Could not load internode authentication file " + authFile, e);
        }
    }

    private void setAuthAddresses(Set<String> addresses) throws InternodeAuthConfUpdateException {
        File authFile = null;

        log.info("Updating " + authFile);
        if (log.isDebugEnabled()) {
            log.debug("Updating authorized storage node addresses to " + addresses);
        }

        try {
            authFile = getInternodeAuthConfFile();
            StreamUtil.copy(new StringReader(StringUtil.collectionToString(addresses, "\n")),
                new FileWriter(authFile), true);
        } catch (Exception e) {
            throw new InternodeAuthConfUpdateException("An error occurred while trying to update " + authFile, e);
        }
    }

    private void updateInternodeAuthConfFile(Set<String> ipAddresses) throws InternodeAuthConfUpdateException {
        File authFile = getInternodeAuthConfFile();

        log.info("Updating " + authFile);

        try {
            StreamUtil.copy(new StringReader(StringUtil.collectionToString(ipAddresses, "\n")),
                new FileWriter(authFile), true);
        } catch (Exception e) {
            log.error("An error occurred while trying to update " + authFile, e);
            throw new InternodeAuthConfUpdateException("An error occurred while trying to update " + authFile, e);
        }
    }

    String getOperationMode() {
        EmsConnection emsConnection = getEmsConnection();
        EmsBean storageService = emsConnection.getBean("org.apache.cassandra.db:type=StorageService");
        EmsAttribute attribute = storageService.getAttribute("OperationMode");

        return (String) attribute.refresh();
    }

    private OperationResult nodeAdded(Configuration params) {
        return performTopologyChangeMaintenance(params);
    }

    private OperationResult nodeRemoved(Configuration params) {
        return performTopologyChangeMaintenance(params);
    }

    private OperationResult performTopologyChangeMaintenance(Configuration params) {
        boolean runRepair = params.getSimple("runRepair").getBooleanValue();
        boolean updateSeedsList = params.getSimple("updateSeedsList").getBooleanValue();

        EmsConnection emsConnection = getEmsConnection();
        KeyspaceService keyspaceService = new KeyspaceService(emsConnection);
        boolean hasErrors = false;
        OperationResult result = new OperationResult();
        Configuration resultConfig = result.getComplexResults();
        PropertyList resultsList = new PropertyList("results");

        OpResult opResult = null;
        if (runRepair) {
            opResult = repairKeyspace(keyspaceService, SYSTEM_AUTH_KEYSPACE);
            if (!opResult.succeeded) {
                hasErrors = true;
            }
            resultsList.add(toPropertyMap(opResult));
        }

        opResult = cleanupKeyspace(keyspaceService, SYSTEM_AUTH_KEYSPACE);
        if (!opResult.succeeded) {
            hasErrors = true;
        }
        resultsList.add(toPropertyMap(opResult));

        if (runRepair) {
            opResult = repairKeyspace(keyspaceService, RHQ_KEYSPACE);
            if (!opResult.succeeded) {
                hasErrors = true;
            }
            resultsList.add(toPropertyMap(opResult));
        }

        opResult = cleanupKeyspace(keyspaceService, RHQ_KEYSPACE);
        if (!opResult.succeeded) {
            hasErrors = true;
        }
        resultsList.add(toPropertyMap(opResult));

        if (updateSeedsList) {
            List<String> addresses = getAddresses(params.getList("seedsList"));
            try {
                opResult = new OpResult();
                opResult.operation = "Update seeds list";
                updateSeedsList(addresses);
                opResult.succeeded = true;
            } catch (Exception e) {
                log.error("An error occurred while updating the seeds lists for " + getResourceContext().getResourceKey(),
                    e);
                opResult.succeeded = false;

                Throwable rootCause = ThrowableUtil.getRootCause(e);
                opResult.details = "An error occurred while updating the seeds list: " +
                    ThrowableUtil.getStackAsString(rootCause);
            }
            resultsList.add(toPropertyMap(opResult));
        }

        resultConfig.put(resultsList);

        if (hasErrors) {
            result.setErrorMessage("One or more tasks failed to complete successfully.");
        }
        return result;
    }

    private OperationResult repair() {
        KeyspaceService keyspaceService = new KeyspaceService(getEmsConnection());
        OperationResult result = new OperationResult();
        Configuration resultConfig = result.getComplexResults();
        PropertyList resultsList = new PropertyList("results");

        OpResult opResult = repairKeyspace(keyspaceService, RHQ_KEYSPACE);
        resultsList.add(toPropertyMap(opResult));

        opResult = repairKeyspace(keyspaceService, SYSTEM_AUTH_KEYSPACE);
        resultsList.add(toPropertyMap(opResult));

        resultConfig.put(resultsList);

        return result;
    }

    private OpResult repairKeyspace(KeyspaceService keyspaceService, String keyspace) {
        OpResult result = new OpResult();
        result.operation = "repair " + keyspace + " keyspace";
        try {
            if (log.isDebugEnabled()) {
                log.debug("Running primary range repair on " + keyspace + " keyspace");
            }
            long start = System.currentTimeMillis();
            keyspaceService.repairPrimaryRange(keyspace);
            long end = System.currentTimeMillis();
            if (log.isDebugEnabled()) {
                log.debug("Finsihed primary range repair on " + keyspace + " keyspace in " + (end - start) + " ms");
            }
            result.succeeded = true;
            result.details = "Completed repair operation in " + (end - start) + " ms.";
        } catch (Exception e) {
            log.error("An error occurred while running repair on " + keyspace, e);
            Throwable rootCause = ThrowableUtil.getRootCause(e);

            result.succeeded = false;
            result.details = "An error occurred while running repair: " + ThrowableUtil.getStackAsString(rootCause);
        }
        return result;
    }

    private OpResult cleanupKeyspace(KeyspaceService keyspaceService, String keyspace) {
        OpResult result = new OpResult();
        result.operation = "cleanup " + keyspace + " keyspace";

        long start;
        long end;
        if (log.isDebugEnabled()) {
            log.debug("Running cleanup on " + keyspace + " keyspace");
        }
        start = System.currentTimeMillis();
        try {
            keyspaceService.cleanup(keyspace);
            end = System.currentTimeMillis();
            if (log.isDebugEnabled()) {
                log.debug("Finished cleanup on " + keyspace + " keyspace in " + (end - start) + " ms");
            }
            result.succeeded = true;
        } catch (Exception e) {
            log.error("An error occurred while running cleanup on " + keyspace + " keyspace", e);
            Throwable rootCause = ThrowableUtil.getRootCause(e);

            result.succeeded = false;
            result.details = "An error occurred while running cleanup: " + ThrowableUtil.getStackAsString(rootCause);
        }
        return result;
    }

    private OperationResult prepareForUpgrade(Configuration parameters) throws Exception {
        EmsConnection emsConnection = getEmsConnection();
        EmsBean storageService = emsConnection.getBean("org.apache.cassandra.db:type=StorageService");
        Class<?>[] emptyParams = new Class<?>[0];

        if (log.isDebugEnabled()) {
            log.debug("Disabling native transport...");
        }
        EmsOperation operation = storageService.getOperation("stopNativeTransport", emptyParams);
        operation.invoke((Object[]) emptyParams);

        if (log.isDebugEnabled()) {
            log.debug("Disabling gossip...");
        }
        operation = storageService.getOperation("stopGossiping", emptyParams);
        operation.invoke((Object[]) emptyParams);

        if (log.isDebugEnabled()) {
            log.debug("Taking the snapshot...");
        }
        operation = storageService.getOperation("takeSnapshot", String.class, String[].class);
        String snapshotName = parameters.getSimpleValue("snapshotName");
        if (snapshotName == null || snapshotName.trim().isEmpty()) {
            snapshotName = System.currentTimeMillis() + "";
        }
        operation.invoke(snapshotName, new String[] {});

        // max 2 sec
        waitForTaskToComplete(500, 10, 150);

        if (log.isDebugEnabled()) {
            log.debug("Initiating drain...");
        }
        operation = storageService.getOperation("drain", emptyParams);
        operation.invoke((Object[]) emptyParams);

        return new OperationResult();
    }

    private void waitForTaskToComplete(int initialWaiting, int maxTries, int sleepMillis) throws InterruptedException {
        // initial waiting
        Thread.sleep(initialWaiting);
        EmsConnection emsConnection = getEmsConnection();
        EmsBean flushWriterBean = emsConnection.getBean("org.apache.cassandra.internal:type=FlushWriter");
        EmsAttribute attribute = flushWriterBean.getAttribute("PendingTasks");

        Long valueObject = (Long) attribute.refresh();
        // wait until org.apache.cassandra.internal:type=FlushWriter / PendingTasks == 0
        while (valueObject > 0 && maxTries-- > 0) {
            Thread.sleep(sleepMillis);
            valueObject = (Long) attribute.refresh();
        }
        flushWriterBean.unload();
    }

    private PropertyMap toPropertyMap(OpResult opResult) {
        PropertyMap map = new PropertyMap("resultsMap");
        map.put(new PropertySimple("task", opResult.operation));
        map.put(new PropertySimple("succeeded", opResult.succeeded));
        map.put(new PropertySimple("details", opResult.details));

        return map;
    }

    private static class OpResult {
        String operation;
        boolean succeeded;
        String details;
    }

    @Override
    public String toString() {
        return StorageNodeComponent.class.getSimpleName() + "[resourceKey: " + getResourceContext().getResourceKey() +
            "]";
    }

    private ConfigEditor getYamlConfigEditor() {
        Configuration pluginConfig = getResourceContext().getPluginConfiguration();
        String yamlProp = pluginConfig.getSimpleValue("yamlConfiguration");
        File yamlFile = new File(yamlProp);
        return new ConfigEditor(yamlFile);
    }
}
