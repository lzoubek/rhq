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
package org.rhq.core.db;

/**
 * Postgres 8.1 database which extends the Postgres 8.x database.
 */
public class Postgresql81DatabaseType extends Postgresql8DatabaseType {
    /**
     * @see DatabaseType#getName()
     */
    @Override
    public String getName() {
        return PostgresqlDatabaseType.VENDOR_NAME + "8.1";
    }

    /**
     * @see DatabaseType#getVersion()
     */
    @Override
    public String getVersion() {
        return "8.1";
    }
}