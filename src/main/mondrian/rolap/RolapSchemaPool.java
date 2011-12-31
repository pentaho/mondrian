/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.spi.DynamicSchemaProcessor;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * A collection of schemas, identified by their connection properties
 * (catalog name, JDBC URL, and so forth).
 *
 * <p>To lookup a schema, call <code>Pool.instance().{@link #get}</code>.
 */
class RolapSchemaPool {
    private final MessageDigest md;

    private static RolapSchemaPool pool = new RolapSchemaPool();

    private final Map<String, SoftReference<RolapSchema>> mapUrlToSchema =
        new HashMap<String, SoftReference<RolapSchema>>();

    RolapSchemaPool() {
        // Initialize the MD5 digester.
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    static RolapSchemaPool instance() {
        return pool;
    }

    /**
     * Creates an MD5 hash of String.
     *
     * @param value String to create one way hash upon.
     * @return MD5 hash.
     */
    private synchronized String encodeMD5(final String value) {
        md.reset();
        final byte[] bytes = md.digest(value.getBytes());
        return (bytes != null) ? new String(bytes) : null;
    }

    synchronized RolapSchema get(
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr,
        final Util.PropertyList connectInfo)
    {
        return get(
            catalogUrl,
            connectionKey,
            jdbcUser,
            dataSourceStr,
            null,
            connectInfo);
    }

    synchronized RolapSchema get(
        final String catalogUrl,
        final DataSource dataSource,
        final Util.PropertyList connectInfo)
    {
        return get(
            catalogUrl,
            null,
            null,
            null,
            dataSource,
            connectInfo);
    }

    private RolapSchema get(
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr,
        final DataSource dataSource,
        final Util.PropertyList connectInfo)
    {
        String key =
            (dataSource == null)
            ? makeKey(catalogUrl, connectionKey, jdbcUser, dataSourceStr)
            : makeKey(catalogUrl, dataSource);

        RolapSchema schema = null;

        String dynProcName = connectInfo.get(
            RolapConnectionProperties.DynamicSchemaProcessor.name());

        String catalogStr = connectInfo.get(
            RolapConnectionProperties.CatalogContent.name());
        if (catalogUrl == null && catalogStr == null) {
            throw MondrianResource.instance()
                .ConnectStringMandatoryProperties.ex(
                    RolapConnectionProperties.Catalog.name(),
                    RolapConnectionProperties.CatalogContent.name());
        }

        // If CatalogContent is specified in the connect string, ignore
        // everything else. In particular, ignore the dynamic schema
        // processor.
        if (catalogStr != null) {
            dynProcName = null;
            // REVIEW: Are we including enough in the key to make it
            // unique?
            key = catalogStr;
        }

        final boolean useContentChecksum =
            Boolean.parseBoolean(
                connectInfo.get(
                    RolapConnectionProperties.UseContentChecksum.name()));

        // Use the schema pool unless "UseSchemaPool" is explicitly false.
        final boolean useSchemaPool =
            Boolean.parseBoolean(
                connectInfo.get(
                    RolapConnectionProperties.UseSchemaPool.name(),
                    "true"));

        // If there is a dynamic processor registered, use it. This
        // implies there is not MD5 based caching, but, as with the previous
        // implementation, if the catalog string is in the connectInfo
        // object as catalog content then it is used.
        if (! Util.isEmpty(dynProcName)) {
            assert catalogStr == null;

            try {
                @SuppressWarnings("unchecked")
                final Class<DynamicSchemaProcessor> clazz =
                    (Class<DynamicSchemaProcessor>)
                        Class.forName(dynProcName);
                final Constructor<DynamicSchemaProcessor> ctor =
                    clazz.getConstructor();
                final DynamicSchemaProcessor dynProc = ctor.newInstance();
                catalogStr = dynProc.processSchema(catalogUrl, connectInfo);

                // Use the content of the catalog to find the schema.
                // Previously we'd use the key, but we didn't include
                // DynamicSchemaProcessor, and that would give false hits.
                key = catalogStr;
            } catch (Exception e) {
                throw Util.newError(
                    e,
                    "loading DynamicSchemaProcessor " + dynProcName);
            }

            if (RolapSchema.LOGGER.isDebugEnabled()) {
                RolapSchema.LOGGER.debug(
                    "Pool.get: create schema \"" + catalogUrl
                    + "\" using dynamic processor");
            }
        }

        if (!useSchemaPool) {
            schema = RolapSchemaLoader.createSchema(
                key,
                null,
                catalogUrl,
                catalogStr,
                connectInfo,
                dataSource);

        } else if (useContentChecksum) {
            // Different catalogUrls can actually yield the same
            // catalogStr! So, we use the MD5 as the key as well as
            // the key made above - its has two entries in the
            // mapUrlToSchema Map. We must then also during the
            // remove operation make sure we remove both.

            String md5Bytes = null;
            try {
                if (catalogStr == null) {
                    // Use VFS to get the content
                    InputStream in = null;
                    try {
                        in = Util.readVirtualFile(catalogUrl);
                        StringBuilder buf = new StringBuilder(1000);
                        int n;
                        while ((n = in.read()) != -1) {
                            buf.append((char) n);
                        }
                        catalogStr = buf.toString();
                    } finally {
                        if (in != null) {
                            in.close();
                        }
                    }
                }

                md5Bytes = encodeMD5(catalogStr);
            } catch (Exception ex) {
                // Note, can not throw an Exception from this method
                // but just to show that all is not well in Mudville
                // we print stack trace (for now - better to change
                // method signature and throw).
                ex.printStackTrace();
            }

            if (md5Bytes != null) {
                SoftReference<RolapSchema> ref =
                    mapUrlToSchema.get(md5Bytes);
                if (ref != null) {
                    schema = ref.get();
                    if (schema == null) {
                        // clear out the reference since schema is null
                        mapUrlToSchema.remove(key);
                        mapUrlToSchema.remove(md5Bytes);
                    }
                }
            }

            if (schema == null
                || md5Bytes == null
                || schema.md5Bytes == null
                || ! schema.md5Bytes.equals(md5Bytes))
            {
                schema = RolapSchemaLoader.createSchema(
                    key,
                    md5Bytes,
                    catalogUrl,
                    catalogStr,
                    connectInfo,
                    dataSource);

                SoftReference<RolapSchema> ref =
                    new SoftReference<RolapSchema>(schema);
                if (md5Bytes != null) {
                    mapUrlToSchema.put(md5Bytes, ref);
                }
                mapUrlToSchema.put(key, ref);

                if (RolapSchema.LOGGER.isDebugEnabled()) {
                    RolapSchema.LOGGER.debug(
                        "Pool.get: create schema \"" + catalogUrl
                        + "\" with MD5");
                }

            } else if (RolapSchema.LOGGER.isDebugEnabled()) {
                RolapSchema.LOGGER.debug(
                    "Pool.get: schema \"" + catalogUrl
                    + "\" exists already with MD5");
            }

        } else {
            SoftReference<RolapSchema> ref = mapUrlToSchema.get(key);
            if (ref != null) {
                schema = ref.get();
                if (schema == null) {
                    // clear out the reference since schema is null
                    mapUrlToSchema.remove(key);
                }
            }

            if (schema == null) {
                schema = RolapSchemaLoader.createSchema(
                    key,
                    null,
                    catalogUrl,
                    catalogStr,
                    connectInfo,
                    dataSource);

                mapUrlToSchema.put(
                    key,
                    new SoftReference<RolapSchema>(schema));

                if (RolapSchema.LOGGER.isDebugEnabled()) {
                    RolapSchema.LOGGER.debug(
                        "Pool.get: create schema \"" + catalogUrl + "\"");
                }

            } else if (RolapSchema.LOGGER.isDebugEnabled()) {
                RolapSchema.LOGGER.debug(
                    "Pool.get: schema \"" + catalogUrl
                    + "\" exists already ");
            }
        }
        return schema;
    }

    synchronized void remove(
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr)
    {
        final String key = makeKey(
            catalogUrl,
            connectionKey,
            jdbcUser,
            dataSourceStr);
        if (RolapSchema.LOGGER.isDebugEnabled()) {
            RolapSchema.LOGGER.debug(
                "Pool.remove: schema \"" + catalogUrl
                + "\" and datasource string \"" + dataSourceStr + "\"");
        }
        remove(key);
    }

    synchronized void remove(
        final String catalogUrl,
        final DataSource dataSource)
    {
        final String key = makeKey(catalogUrl, dataSource);
        if (RolapSchema.LOGGER.isDebugEnabled()) {
            RolapSchema.LOGGER.debug(
                "Pool.remove: schema \"" + catalogUrl
                + "\" and datasource object");
        }
        remove(key);
    }

    synchronized void remove(RolapSchema schema) {
        if (schema != null) {
            if (RolapSchema.LOGGER.isDebugEnabled()) {
                RolapSchema.LOGGER.debug(
                    "Pool.remove: schema \"" + schema.name
                    + "\" and datasource object");
            }
            remove(schema.key);
        }
    }

    private void remove(String key) {
        SoftReference<RolapSchema> ref = mapUrlToSchema.get(key);
        if (ref != null) {
            RolapSchema schema = ref.get();
            if (schema != null) {
                if (schema.md5Bytes != null) {
                    mapUrlToSchema.remove(schema.md5Bytes);
                }
                schema.finalCleanUp();
            }
        }
        mapUrlToSchema.remove(key);
    }

    synchronized void clear() {
        if (RolapSchema.LOGGER.isDebugEnabled()) {
            RolapSchema.LOGGER.debug("Pool.clear: clearing all RolapSchemas");
        }

        for (SoftReference<RolapSchema> ref : mapUrlToSchema.values()) {
            if (ref != null) {
                RolapSchema schema = ref.get();
                if (schema != null) {
                    schema.finalCleanUp();
                }
            }
        }
        mapUrlToSchema.clear();
        JdbcSchema.clearAllDBs();
    }

    /**
     * Returns an iterator over a copy of the RolapSchema's container.
     *
     * @return Iterator over RolapSchemas
     */
    synchronized List<RolapSchema> getRolapSchemas() {
        List<RolapSchema> list = new ArrayList<RolapSchema>();
        for (Iterator<SoftReference<RolapSchema>> it =
            mapUrlToSchema.values().iterator(); it.hasNext();)
        {
            SoftReference<RolapSchema> ref = it.next();
            RolapSchema schema = ref.get();
            // Schema is null if already garbage collected
            if (schema != null) {
                list.add(schema);
            } else {
                // We will remove the stale reference
                try {
                    it.remove();
                } catch (Exception ex) {
                    // Should not happen, so
                    // warn but otherwise ignore
                    RolapSchema.LOGGER.warn(ex);
                }
            }
        }
        return list;
    }

    synchronized boolean contains(RolapSchema rolapSchema) {
        return mapUrlToSchema.containsKey(rolapSchema.key);
    }


    /**
     * Creates a key with which to identify a schema in the cache.
     */
    private static String makeKey(
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr)
    {
        final StringBuilder buf = new StringBuilder(100);

        appendIfNotNull(buf, catalogUrl);
        appendIfNotNull(buf, connectionKey);
        appendIfNotNull(buf, jdbcUser);
        appendIfNotNull(buf, dataSourceStr);

        return buf.toString();
    }

    /**
     * Creates a key with which to identify a schema in the cache.
     */
    private static String makeKey(
        final String catalogUrl,
        final DataSource dataSource)
    {
        final StringBuilder buf = new StringBuilder(100);

        appendIfNotNull(buf, catalogUrl);
        buf.append('.');
        buf.append("external#");
        buf.append(System.identityHashCode(dataSource));

        return buf.toString();
    }

    private static void appendIfNotNull(StringBuilder buf, String s) {
        if (s != null) {
            if (buf.length() > 0) {
                buf.append('.');
            }
            buf.append(s);
        }
    }
}

// End RolapSchemaPool.java
