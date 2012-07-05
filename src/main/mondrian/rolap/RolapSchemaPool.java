/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde and others
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.resource.MondrianResource;
import mondrian.rolap.aggmatcher.JdbcSchema;
import mondrian.spi.DynamicSchemaProcessor;
import mondrian.util.ByteString;

import java.lang.ref.SoftReference;
import java.lang.reflect.Constructor;
import java.util.*;
import javax.sql.DataSource;

/**
 * A collection of schemas, identified by their connection properties
 * (catalog name, JDBC URL, and so forth).
 *
 * <p>To lookup a schema, call <code>Pool.instance().{@link #get}</code>.
 */
class RolapSchemaPool {
    private static RolapSchemaPool pool = new RolapSchemaPool();

    private final Map<String, SoftReference<RolapSchema>> mapUrlToSchema =
        new HashMap<String, SoftReference<RolapSchema>>();

    private final Map<ByteString, SoftReference<RolapSchema>> mapMd5ToSchema =
        new HashMap<ByteString, SoftReference<RolapSchema>>();

    RolapSchemaPool() {
    }

    static RolapSchemaPool instance() {
        return pool;
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
        final String dialectClassName =
            connectInfo.get(RolapConnectionProperties.Dialect.name());
        String key =
            (dataSource == null)
            ? makeKey(
                catalogUrl, dialectClassName, connectionKey, jdbcUser,
                dataSourceStr)
            : makeKey(catalogUrl, dialectClassName, dataSource);

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

            ByteString md5Bytes = null;
            try {
                if (catalogStr == null) {
                    // Use VFS to get the content
                    catalogStr = Util.readVirtualFileAsString(catalogUrl);
                }

                md5Bytes = new ByteString(Util.digestMd5(catalogStr));
            } catch (Exception ex) {
                // Note, can not throw an Exception from this method
                // but just to show that all is not well in Mudville
                // we print stack trace (for now - better to change
                // method signature and throw).
                ex.printStackTrace();
            }

            if (md5Bytes != null) {
                SoftReference<RolapSchema> ref = mapMd5ToSchema.get(md5Bytes);
                if (ref != null) {
                    schema = ref.get();
                    if (schema == null) {
                        // clear out the reference since schema is null
                        mapUrlToSchema.remove(key);
                        mapMd5ToSchema.remove(md5Bytes);
                    }
                }
            }

            if (schema == null
                || md5Bytes == null
                || !schema.useContentChecksum
                || !schema.md5Bytes.equals(md5Bytes))
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
                    mapMd5ToSchema.put(md5Bytes, ref);
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
        final String dialectClassName,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr)
    {
        final String key = makeKey(
            catalogUrl,
            dialectClassName,
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
        final String dialectClassName,
        final DataSource dataSource)
    {
        final String key = makeKey(catalogUrl, dialectClassName, dataSource);
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
                mapMd5ToSchema.remove(schema.md5Bytes);
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
        mapMd5ToSchema.clear();
        JdbcSchema.clearAllDBs();
    }

    /**
     * Returns a list of schemas in this pool.
     *
     * @return List of schemas in this pool
     */
    synchronized List<RolapSchema> getRolapSchemas() {
        List<RolapSchema> list = new ArrayList<RolapSchema>();
        for (RolapSchema schema : Util.GcIterator.over(mapUrlToSchema.values()))
        {
            list.add(schema);
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
        final String dialectClassName,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr)
    {
        final StringBuilder buf = new StringBuilder(100);

        appendIfNotNull(buf, catalogUrl);
        appendIfNotNull(buf, dialectClassName);
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
        final String dialectClassName,
        final DataSource dataSource)
    {
        final StringBuilder buf = new StringBuilder(100);

        appendIfNotNull(buf, catalogUrl);
        appendIfNotNull(buf, dialectClassName);
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
