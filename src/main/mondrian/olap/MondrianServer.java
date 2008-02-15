/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import java.util.List;

/**
 * Interface by which to control an instance of Mondrian.
 *
 * <p>Typically, there is only one instance of Mondrian per JVM. However, you
 * access a MondrianServer via the {@link #forConnection} method for future
 * expansion.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 25, 2006
 */
public abstract class MondrianServer {
    private static MondrianServer instance = new MondrianServerImpl();

    /**
     * Returns the MondrianServer which hosts a given connection.
     * @param connection Connection
     */
    public static MondrianServer forConnection(Connection connection) {
        // Mondrian server is currently a singleton, so the connection is
        // irrelevant.
        Util.discard(connection);
        return instance;
    }

    /**
     * Returns the version of this MondrianServer.
     */
    public abstract MondrianVersion getVersion();

    /**
     * Returns a list of MDX keywords.
     * @return list of MDX keywords
     */
    public abstract List<String> getKeywords();

    /**
     * Description of the version of the server.
     */
    public interface MondrianVersion {
        /**
         * Returns the version string, for example "2.3.0".
         *
         * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
         */
        String getVersionString();

        /**
         * Returns the major part of the version number.
         *
         * <p>For example, if the full version string is "2.3.0", the major
         * version is 2.
         *
         * @return major part of the version number
         * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
         */
        int getMajorVersion();

        /**
         * Returns the minor part of the version number.
         *
         * <p>For example, if the full version string is "2.3.0", the minor
         * version is 3.
         *
         * @return minor part of the version number
         *
         * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
         */
        int getMinorVersion();

        /**
         * Retrieves the name of this database product.
         *
         * @return database product name
         * @see java.sql.DatabaseMetaData#getDatabaseProductName()
         */
        String getProductName();
    }
}

// End MondrianServer.java
