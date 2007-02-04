/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.rolap.RolapSchema;

import java.net.URL;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Implementation of {@link MondrianServer}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 25, 2006
 */
class MondrianServerImpl extends MondrianServer {
    private static MondrianVersion version = null;

    public void flushSchemaCache() {
        RolapSchema.clearCache();
    }

    public void flushDataCache() {
        // not implemented
    }

    public MondrianVersion getVersion() {
        return getVersionStatic();
    }

    private static synchronized MondrianVersion getVersionStatic() {
        if (version == null) {
            final String versionString = loadVersionFile();
            version = new MondrianVersion() {
                public String getVersionString() {
                    return versionString;
                }
            };
        }
        return version;
    }

    private static String loadVersionFile() {
        // First, try to read the version info from the package. If the classes
        // came from a jar, this info will be set from manifest.mf.
        Package pakkage = MondrianServerImpl.class.getPackage();
        String implementationVersion = pakkage.getImplementationVersion();
        if (implementationVersion != null) {
            return implementationVersion;
        }

        // Second, try to read VERSION.txt.
        URL resource =
            MondrianServerImpl.class.getClassLoader()
                .getResource("DefaultRules.xml");
        if (resource != null) {
            try {
                String path = resource.getPath();
                String path2 =
                    Util.replace(
                        path, "classes/DefaultRules.xml", "VERSION.txt");
                URL resource2 =
                    new URL(
                        resource.getProtocol(),
                        resource.getHost(),
                        path2);
                String versionString = Util.readURL(resource2);
                Pattern pattern =
                    Pattern.compile(
                        "(?s)Title: (.*)\nVersion: (.*)\nVendor: (.*)\n.*");
                Matcher matcher = pattern.matcher(versionString);
                if (matcher.matches()) {
                    int groupCount = matcher.groupCount();
                    String title = matcher.group(1);
                    String version = matcher.group(2);
                    String vendor = matcher.group(3);
                    return version;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return "Unknown version";
    }
}

// End MondrianServerImpl.java
