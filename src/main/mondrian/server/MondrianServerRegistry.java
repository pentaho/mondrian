/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.olap.Util;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.IdentityCatalogLocator;
import mondrian.util.LockBox;

import java.io.*;
import java.net.URL;
import java.util.*;

/**
 * Registry of all servers within this JVM, and also serves as a factory for
 * servers.
 *
 * <p>This class is not a public API. User applications should use the
 * methods in {@link mondrian.olap.MondrianServer}.
 *
 * @author jhyde
 */
public class MondrianServerRegistry {
    public static final MondrianServerRegistry INSTANCE =
        new MondrianServerRegistry();

    public MondrianServerRegistry() {
        super();
    }

    /**
     * Registry of all servers.
     */
    final LockBox lockBox = new LockBox();

    /**
     * The one and only one server that does not have a repository.
     */
    final MondrianServer staticServer =
        createWithRepository(null, null);

    private MondrianServer.MondrianVersion version = null;

    /**
     * Looks up a server with a given id. If the id is null, returns the
     * static server.
     *
     * @param instanceId Unique identifier of server instance
     * @return Server
     * @throws RuntimeException if no server instance exists
     */
    public MondrianServer serverForId(String instanceId) {
        if (instanceId != null) {
            final LockBox.Entry entry = lockBox.get(instanceId);
            if (entry == null) {
                throw Util.newError(
                    "No server instance has id '" + instanceId + "'");
            }
            return (MondrianServer) entry.getValue();
        } else {
            return staticServer;
        }
    }

    public synchronized MondrianServer.MondrianVersion getOrLoadVersion() {
        if (version == null) {
            final String[] vendorTitleVersion = loadVersionFile();
            String vendor = vendorTitleVersion[0];
            final String title = vendorTitleVersion[1];
            final String versionString = vendorTitleVersion[2];
            if (false) {
                System.out.println(
                    "vendor=" + vendor
                    + ", title=" + title
                    + ", versionString=" + versionString);
            }
            int dot1 = versionString.indexOf('.');
            final int majorVersion =
                dot1 < 0
                ? 1
                : Integer.valueOf(versionString.substring(0, dot1));
            int dot2 = versionString.indexOf('.', dot1 + 1);
            final int minorVersion =
                dot2 < 0
                ? 0
                : Integer.valueOf(versionString.substring(dot1 + 1, dot2));
            version = new MondrianServer.MondrianVersion() {
                public String getVersionString() {
                    return versionString;
                }

                public int getMajorVersion() {
                    return majorVersion;
                }

                public int getMinorVersion() {
                    return minorVersion;
                }

                public String getProductName() {
                    return title;
                }
            };
        }
        return version;
    }

    private static String[] loadVersionFile() {
        // First, try to read the version info from the package. If the classes
        // came from a jar, this info will be set from manifest.mf.
        Package pakkage = MondrianServerImpl.class.getPackage();
        String implementationVersion = pakkage.getImplementationVersion();

        // Second, try to read VERSION.txt.
        String version = "Unknown Version";
        String title = "Unknown Database";
        String vendor = "Unknown Vendor";
        URL resource =
            MondrianServerImpl.class.getClassLoader()
                .getResource("DefaultRules.xml");
        if (resource != null) {
            try {
                String path = resource.getPath();
                String path2 =
                    Util.replace(
                        path, "DefaultRules.xml", "VERSION.txt");
                URL resource2 =
                    new URL(
                        resource.getProtocol(),
                        resource.getHost(),
                        path2);

                // Parse VERSION.txt. E.g.
                //   Title: mondrian
                //   Version: 3.4.9
                // becomes {("Title", "mondrian"), ("Version", "3.4.9")}
                final Map<String, String> map = new HashMap<String, String>();
                final LineNumberReader r =
                    new LineNumberReader(
                        new InputStreamReader(resource2.openStream()));
                try {
                    String line;
                    while ((line = r.readLine()) != null) {
                        int i = line.indexOf(": ");
                        if (i >= 0) {
                            String key = line.substring(0, i);
                            String value = line.substring(i + ": ".length());
                            map.put(key, value);
                        }
                    }
                } finally {
                    r.close();
                }

                title = map.get("Title");
                version = map.get("Version");
                try {
                    Integer.parseInt(version);
                } catch (NumberFormatException e) {
                    // Version is not a number (e.g. "TRUNK-SNAPSHOT").
                    // Fall back on VersionMajor, VersionMinor, if present.
                    String versionMajor = map.get("VersionMajor");
                    String versionMinor = map.get("VersionMinor");
                    if (versionMajor != null) {
                        version = versionMajor;
                    }
                    if (versionMinor != null) {
                        version += "." + versionMinor;
                    }
                }
                vendor = map.get("Vendor");
            } catch (IOException e) {
                // ignore exception - it's OK if file is not found
                Util.discard(e);
            }
        }

        // Version from jar manifest overrides that from VERSION.txt.
        if (implementationVersion != null) {
            version = implementationVersion;
        }
        return new String[] {vendor, title, version};
    }

    public MondrianServer createWithRepository(
        RepositoryContentFinder contentFinder,
        CatalogLocator catalogLocator)
    {
        if (catalogLocator == null) {
            catalogLocator = new IdentityCatalogLocator();
        }
        final Repository repository;
        if (contentFinder == null) {
            // NOTE: registry.staticServer is initialized by calling this
            // method; this is the only time that it is null.
            if (staticServer != null) {
                return staticServer;
            }
            repository = new ImplicitRepository();
        } else {
            repository = new FileRepository(contentFinder, catalogLocator);
        }
        return new MondrianServerImpl(this, repository, catalogLocator);
    }
}

// End MondrianServerRegistry.java
