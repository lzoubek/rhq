/*
 * RHQ Management Platform
 * Copyright (C) 2005-2008 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.rhq.enterprise.server.support;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.jboss.mx.util.MBeanServerLocator;
import org.jboss.system.server.ServerConfig;

import org.rhq.core.clientapi.agent.support.SnapshotReportAgentService;
import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.authz.Permission;
import org.rhq.core.util.ObjectNameFactory;
import org.rhq.core.util.stream.StreamUtil;
import org.rhq.enterprise.server.agentclient.AgentClient;
import org.rhq.enterprise.server.authz.RequiredPermission;
import org.rhq.enterprise.server.core.AgentManagerLocal;

@Stateless
public class SnapshotReportManagerBean implements SnapshotReportManagerLocal {

    @EJB
    private AgentManagerLocal agentManager;

    // a snapshot report is potentially very sensitive - it can contain configuration settings, data files and logs - all
    // of which can contain confidential information. Therefore, because the amount of data the user has access to
    // is far sweeping, we require the user to be an inventory manager.
    @RequiredPermission(Permission.MANAGE_INVENTORY)
    public URL getSnapshotReport(Subject subject, int resourceId, String name, String description) throws Exception {

        AgentClient agentClient = this.agentManager.getAgentClient(resourceId);
        SnapshotReportAgentService snapshotService = agentClient.getSnapshotReportAgentService();
        InputStream snapshot = snapshotService.getSnapshotReport(resourceId, name, description);

        // TODO: not sure what we should really do with it, for now, put it in the downloads location.
        // you can retrieve this by going to http://localhost:7080/downloads
        File dir = getDownloadsDir();
        File downloadFile = File.createTempFile(name + "-" + resourceId + "-", ".zip", dir);
        StreamUtil.copy(snapshot, new FileOutputStream(downloadFile));

        return downloadFile.toURI().toURL();
    }

    private File getDownloadsDir() throws Exception {
        MBeanServer mbs = MBeanServerLocator.locateJBoss();
        ObjectName name = ObjectNameFactory.create("jboss.system:type=ServerConfig");
        Object mbean = MBeanServerInvocationHandler.newProxyInstance(mbs, name, ServerConfig.class, false);
        File serverHomeDir = ((ServerConfig) mbean).getServerHomeDir();
        File downloadDir = new File(serverHomeDir, "deploy/rhq.ear/rhq-downloads");
        if (!downloadDir.exists()) {
            throw new FileNotFoundException("Missing downloads directory at [" + downloadDir + "]");
        }
        return downloadDir;
    }
}
