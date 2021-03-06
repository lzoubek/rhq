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

import java.io.InputStream;
import java.net.URL;

import javax.ejb.Local;

import org.rhq.core.domain.auth.Subject;

/**
 * Provides some methods that are useful for supporting managed resources. This includes being
 * able to take a snapshot report of a managed resource, such as its log files, data files, and anything
 * the managed resource wants to expose.
 * 
 * @author John Mazzitelli
 */
@Local
public interface SupportManagerLocal {
    InputStream getSnapshotReportStream(Subject subject, int resourceId, String name, String description)
        throws Exception;

    URL getSnapshotReport(Subject subject, int resourceId, String name, String description) throws Exception;
}

