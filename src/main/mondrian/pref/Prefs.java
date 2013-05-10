/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.pref;

import mondrian.olap.Util;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Utilities concerning preferences.
 *
 * <h2>Note to developers</h2>
 *
 * If you add a property, you must:<ul>
 *
 * <li>Add a property definition to MondrianProperties.xml.</li>
 *
 * <li>Re-generate MondrianProperties.java using PropertyUtil.</li>
 *
 * <li>Modify the default <code>mondrian.properties</code> file checked into
 * source control, with a description of the property and its default
 * value.</li>
 *
 * <li>Modify the
 * <a target="_top" href="{@docRoot}/../configuration.html#Property_list">
 * Configuration Specification</a>.</li>
 * </ul>
 *
 * <p>Similarly if you update or delete a property.
 *
 */
public class Prefs {
    private static final Logger LOGGER = Logger.getLogger(Prefs.class);

    protected static final String mondrianDotProperties = "mondrian.properties";

    static ServerPref initServer() {
        final ServerPref pref = new ServerPref();
        populate(pref, (PropertySource) null);
        return pref;
    }

    public static ServerPref server() {
        return ServerPref.instance();
    }

    /** Creates connection preferences from a server. */
    public static SchemaPref schema(ServerPref serverPref) {
        return new SchemaPref(serverPref);
    }

    /** Creates connection preferences from a server. */
    public static ConnectionPref connection(ServerPref server) {
        return new ConnectionPref(server);
    }

    /** Creates statement preferences from a connection. */
    public static StatementPref statement(
        ConnectionPref pref, SchemaPref schema)
    {
        return new StatementPref(pref, schema);
    }

    public static String read(Map[] maps, StringProperty property) {
        final String path = property.getPath();
        for (Map map : maps) {
            final String value = (String) map.get(path);
            if (value != null) {
                return value;
            }
        }
        return property.getDefaultValue();
    }

    public static int read(Map[] maps, IntegerProperty property) {
        final String path = property.getPath();
        for (Map map : maps) {
            final Integer value = (Integer) map.get(path);
            if (value != null) {
                return value;
            }
        }
        return property.getDefaultValue();
    }

    public static double read(Map[] maps, DoubleProperty property) {
        final String path = property.getPath();
        for (Map map : maps) {
            final Double value = (Double) map.get(path);
            if (value != null) {
                return value;
            }
        }
        return property.getDefaultValue();
    }

    public static boolean read(Map[] maps, BooleanProperty property) {
        final String path = property.getPath();
        for (Map map : maps) {
            final Boolean value = (Boolean) map.get(path);
            if (value != null) {
                return value;
            }
        }
        return property.getDefaultValue();
    }

    /** Gets the value of a property from its path. Need not be a built-in
     * property. */
    public static Object get(StatementPref pref, BaseProperty property) {
        switch (property.scope) {
        case System:
            return get_(pref.server, property.name);
        case Connection:
            return get_(pref.connection, property.name);
        case Schema:
            return get_(pref.schema, property.name);
        case Statement:
            return get_(pref, property.name);
        default:
            throw new AssertionError(property.scope);
        }
    }

    static Object get_(Object o, String fieldName) {
        if (fieldName == null) {
            return null;
        }
        try {
            return o.getClass().getDeclaredField(fieldName).get(o);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static void set(
        StatementPref pref, BaseProperty property, Object value)
    {
        if (property.name == null) {
            pref.server.defaultMap.put(property, value);
        } else {
            switch (property.scope) {
            case System:
                set_(pref.server, property.name, value);
                break;
            case Connection:
                set_(pref.connection, property.name, value);
                break;
            case Schema:
                set_(pref.schema, property.name, value);
                break;
            case Statement:
                set_(pref, property.name, value);
                break;
            default:
                throw new AssertionError(property.scope);
            }
        }
    }

    private static void set_(Object o, String fieldName, Object value) {
        try {
            o.getClass()
                .getDeclaredField(fieldName)
                .set(o, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static void remove(StatementPref pref, String path) {
        throw new UnsupportedOperationException(); // TODO:
    }

    /** Loads a preferences object (usually an instance of ServerPref,
     * SchemaPref, ConnectionPref, StatementPref) by reflection. */
    static void load(
        Object o,
        mondrian.pref.Scope scope,
        Map<BaseProperty, Object> defaultMap,
        Map<String, String> map)
    {
        // First, look for values of built-in properties.
        for (BaseProperty property : PrefDef.MAP.values()) {
            final String path = property.getPath();
            final String stringValue = map.get(path);
            final Object value;
            if (stringValue == null) {
                value = property.defaultValue;
            } else if (property instanceof StringProperty) {
                value = stringValue;
            } else if (property instanceof BooleanProperty) {
                value = Boolean.valueOf(stringValue);
            } else if (property instanceof IntegerProperty) {
                value = Integer.valueOf(stringValue);
            } else {
                value = Double.valueOf(stringValue);
            }
            if (property.scope == scope) {
                try {
                    o.getClass().getDeclaredField(property.name).set(o, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                }
            } else {
                defaultMap.put(property, value);
            }
        }
        // Next, put non-built-in properties into defaultMap.
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!PrefDef.MAP.containsKey(entry.getKey())) {
                defaultMap.put(
                    new StringProperty(
                        null, Scope.System, entry.getKey(), null),
                    entry.getValue());
            }
        }
    }

    public static void populate(ServerPref pref, File file) {
        populate(pref, new FilePropertySource(file));
    }

    /**
     * Loads this property set from: the file "$PWD/mondrian.properties" (if it
     * exists); the "mondrian.properties" in the CLASSPATH; and from the system
     * properties.
     */
    public static void populate(
        ServerPref pref,
        PropertySource propertySource)
    {
        // Read properties file "mondrian.properties", if it exists. If we have
        // read the file before, only read it if it is newer.
        if (propertySource != null) {
            loadIfStale(pref, propertySource);
        }

        URL url = null;
        File file = new File(mondrianDotProperties);
        if (file.exists() && file.isFile()) {
            // Read properties file "mondrian.properties" from PWD, if it
            // exists.
            try {
                url = file.toURI().toURL();
            } catch (MalformedURLException e) {
                LOGGER.warn(
                    "Mondrian: file '"
                    + file.getAbsolutePath()
                    + "' could not be loaded", e);
            }
        } else {
            // Then try load it from classloader
            url =
                Prefs.class.getClassLoader().getResource(
                    mondrianDotProperties);
        }

        if (url != null) {
            load(pref, new UrlPropertySource(url));
        } else {
            LOGGER.warn(
                "mondrian.properties can't be found under '"
                + new File(".").getAbsolutePath() + "' or classloader");
        }

        // copy in all system properties which start with "mondrian."
        int count = 0;
        for (Enumeration<?> keys = System.getProperties().keys();
             keys.hasMoreElements();)
        {
            String key = (String) keys.nextElement();
            String value = System.getProperty(key);
            if (key.startsWith("mondrian.")) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("populate: key=" + key + ", value=" + value);
                }
                pref.defaultMap.put(
                    new StringProperty(
                        null, mondrian.pref.Scope.System, key, null),
                    value);
                count++;
            }
        }
        if (pref.populateCount++ == 0) {
            LOGGER.info(
                "Mondrian: loaded " + count + " system properties");
        }
    }

    /**
     * Reads properties from a source.
     * If the source does not exist, or has not changed since we last read it,
     * does nothing.
     *
     * @param source Source of properties
     */
    private static void loadIfStale(ServerPref pref, PropertySource source) {
        if (source.isStale()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Mondrian: loading " + source.getDescription());
            }
            load(pref, source);
        }
    }

    /**
     * Tries to load properties from a URL. Does not fail, just prints success
     * or failure to log.
     *
     * @param source Source to read properties from
     */
    private static void load(ServerPref pref, final PropertySource source) {
        try {
            final Properties properties = new Properties();
            properties.load(source.openStream());
            pref.populate(Util.toMap(properties));
            if (pref.populateCount == 0) {
                LOGGER.info(
                    "Mondrian: properties loaded from '"
                    + source.getDescription()
                    + "'");
            }
        } catch (IOException e) {
            LOGGER.error(
                "Mondrian: error while loading properties "
                + "from '" + source.getDescription() + "' (" + e + ")");
        }
    }

    /**
     * A place that properties can be read from, and remembers the
     * timestamp that we last read them.
     */
    interface PropertySource {
        /**
         * Opens an input stream from the source.
         *
         * <p>Also checks the 'last modified' time, which will determine whether
         * {@link #isStale()} returns true.
         *
         * @return input stream
         */
        InputStream openStream();

        /**
         * Returns true if the source exists and has been modified since last
         * time we called {@link #openStream()}.
         *
         * @return whether source has changed since it was last read
         */
        boolean isStale();

        /**
         * Returns the description of this source, such as a filename or URL.
         *
         * @return description of this PropertySource
         */
        String getDescription();
    }

    /**
     * Implementation of {@link PropertySource} that reads from a {@link File}.
     */
    static class FilePropertySource implements PropertySource {
        private final File file;
        private long lastModified;

        FilePropertySource(File file) {
            this.file = file;
            this.lastModified = 0;
        }

        public InputStream openStream() {
            try {
                this.lastModified = file.lastModified();
                return new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw Util.newInternal(
                    e,
                    "Error while opening properties file '" + file + "'");
            }
        }

        public boolean isStale() {
            return file.exists()
                && file.lastModified() > this.lastModified;
        }

        public String getDescription() {
            return "file=" + file.getAbsolutePath()
                + " (exists=" + file.exists() + ")";
        }
    }

    /**
     * Implementation of {@link PropertySource} which reads from a
     * {@link java.net.URL}.
     */
    static class UrlPropertySource implements PropertySource {
        private final URL url;
        private long lastModified;

        UrlPropertySource(URL url) {
            this.url = url;
        }

        private URLConnection getConnection() {
            try {
                return url.openConnection();
            } catch (IOException e) {
                throw Util.newInternal(
                    e,
                    "Error while opening properties file '" + url + "'");
            }
        }

        public InputStream openStream() {
            try {
                final URLConnection connection = getConnection();
                this.lastModified = connection.getLastModified();
                return connection.getInputStream();
            } catch (IOException e) {
                throw Util.newInternal(
                    e,
                    "Error while opening properties file '" + url + "'");
            }
        }

        public boolean isStale() {
            final long lastModified = getConnection().getLastModified();
            return lastModified > this.lastModified;
        }

        public String getDescription() {
            return url.toExternalForm();
        }
    }
}

// End Prefs.java
