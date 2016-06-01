/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

import org.apache.log4j.Logger;

import org.eigenbase.util.property.TriggerableProperties;

import java.io.*;
import java.net.*;
import java.util.Enumeration;

/**
 * <code>MondrianProperties</code> contains the properties which determine the
 * behavior of a mondrian instance.
 *
 * <p>There is a method for property valid in a
 * <code>mondrian.properties</code> file. Although it is possible to retrieve
 * properties using the inherited {@link java.util.Properties#getProperty(String)}
 * method, we recommend that you use methods in this class.
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
 * @author jhyde
 * @since 22 December, 2002
 */
public abstract class MondrianPropertiesBase extends TriggerableProperties {

    private final PropertySource propertySource;
    private int populateCount;

    private static final Logger LOGGER =
        Logger.getLogger(MondrianProperties.class);

    protected static final String mondrianDotProperties = "mondrian.properties";

    protected MondrianPropertiesBase(PropertySource propertySource) {
        this.propertySource = propertySource;
    }

    public boolean triggersAreEnabled() {
        return ((MondrianProperties) this).EnableTriggers.get();
    }

    /**
     * Represents a place that properties can be read from, and remembers the
     * timestamp that we last read them.
     */
    public interface PropertySource {
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
     * Implementation of {@link PropertySource} which reads from a
     * {@link java.io.File}.
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

    /**
     * Loads this property set from: the file "$PWD/mondrian.properties" (if it
     * exists); the "mondrian.properties" in the CLASSPATH; and from the system
     * properties.
     */
    public void populate() {
        // Read properties file "mondrian.properties", if it exists. If we have
        // read the file before, only read it if it is newer.
        loadIfStale(propertySource);

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
                MondrianPropertiesBase.class.getClassLoader().getResource(
                    mondrianDotProperties);
        }

        if (url != null) {
            load(new UrlPropertySource(url));
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
                // NOTE: the super allows us to bybase calling triggers
                // Is this the correct behavior?
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("populate: key=" + key + ", value=" + value);
                }
                super.setProperty(key, value);
                count++;
            }
        }
        LOGGER.info(
            "Mondrian: loaded " + count + " system properties");
    }

    /**
     * Reads properties from a source.
     * If the source does not exist, or has not changed since we last read it,
     * does nothing.
     *
     * @param source Source of properties
     */
    private void loadIfStale(PropertySource source) {
        if (source.isStale()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Mondrian: loading " + source.getDescription());
            }
            load(source);
        }
    }

    /**
     * Tries to load properties from a URL. Does not fail, just prints success
     * or failure to log.
     *
     * @param source Source to read properties from
     */
    private void load(final PropertySource source) {
        try {
            load(source.openStream());
            if (populateCount == 0) {
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
}

// End MondrianPropertiesBase.java
