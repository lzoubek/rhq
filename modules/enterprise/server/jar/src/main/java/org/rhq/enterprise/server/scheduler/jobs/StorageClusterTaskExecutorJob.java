/*
 * RHQ Management Platform
 * Copyright (C) 2005-2015 Red Hat, Inc.
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
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA
 */
package org.rhq.enterprise.server.scheduler.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import org.rhq.core.domain.cloud.ClusterTask;
import org.rhq.enterprise.server.util.LookupUtil;

/**
 * This job executes {@link ClusterTask} tasks
 * @author lzoubek@redhat.com
 *
 */
public class StorageClusterTaskExecutorJob extends AbstractStatefulJob {
    private static final Log log = LogFactory.getLog(StorageClusterTaskExecutorJob.class);

    @Override
    public void executeJobCode(JobExecutionContext context) throws JobExecutionException {
        log.info("Job starting");
        LookupUtil.getStorageClusterStateManager().runTasksInNewTx();
    }
}
