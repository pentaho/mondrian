/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 July, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;
import org.apache.log4j.Logger;
import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * A <code>RolapSchema</code> is a collection of {@link RolapCube}s and
 * shared {@link RolapDimension}s. It is shared betweeen {@link
 * RolapConnection}s. It caches {@link MemberReader}s, etc.
 *
 * @see RolapConnection
 * @author jhyde
 * @since 26 July, 2001
 * @version $Id$
 **/
public class RolapSchema implements Schema {
    private static final Logger LOGGER = Logger.getLogger(RolapSchema.class);

    private static final int[] schemaAllowed = new int[] {Access.NONE, Access.ALL, Access.ALL_DIMENSIONS};
    private static final int[] cubeAllowed = new int[] {Access.NONE, Access.ALL};
    private static final int[] dimensionAllowed = new int[] {Access.NONE, Access.ALL};
    private static final int[] hierarchyAllowed = new int[] {Access.NONE, Access.ALL, Access.CUSTOM};
    private static final int[] memberAllowed = new int[] {Access.NONE, Access.ALL};

    private String name;
    /**
     * Internal use only.
     */
    private final RolapConnection internalConnection;
    /**
     * Holds cubes in this schema.
     */
    private final Map mapNameToCube;
    /**
     * Maps {@link String shared hierarchy name} to {@link MemberReader}.
     * Shared between all statements which use this connection.
     */
    private final Map mapSharedHierarchyToReader;

    /**
     * Maps {@link String names of shared hierarchies} to {@link
     * RolapHierarchy the canonical instance of those hierarchies}.
     */
    private final Map mapSharedHierarchyNameToHierarchy;
    /**
     * The default role for connections to this schema.
     */
    private Role defaultRole;

    private final byte[] md5Bytes;
    /**
     * Maps {@link String names of roles} to {@link Role roles with those names}.
     */
    private final Map mapNameToRole;

    private RolapSchema(final byte[] md5Bytes,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {
        this.md5Bytes = md5Bytes;
        // the order of the next two lines is important
        this.defaultRole = createDefaultRole();
        this.internalConnection =
            new RolapConnection(connectInfo, this, dataSource);

        this.mapSharedHierarchyNameToHierarchy = new HashMap();
        this.mapSharedHierarchyToReader = new HashMap();
        this.mapNameToCube = new HashMap();
        this.mapNameToRole = new HashMap();
    }

    /**
     * Loads a schema using a dynamic loader.
     *
     * @param dynProcName
     * @param catalogName
     * @param connectInfo
     */
    private RolapSchema(
            final String catalogName,
            final Util.PropertyList connectInfo,
            final String dynProcName,
            final DataSource dataSource) {
        this((byte[]) null, connectInfo, dataSource);

        String catalogStr = null;

        try {
            final URL url = new URL(catalogName);

            final Class clazz = Class.forName(dynProcName);
            final Constructor ctor = clazz.getConstructor(new Class[0]);
            final DynamicSchemaProcessor dynProc =
                    (DynamicSchemaProcessor) ctor.newInstance(new Object[0]);

            catalogStr = dynProc.processSchema(url);

        } catch (Exception e) {
            throw Util.newError(e, "loading DynamicSchemaProcessor "
                    + dynProcName);
        }

        load(catalogName, catalogStr);
    }

    /**
     * Create RolapSchema given the catalog name and string (content) and
     * the connectInfo object.
     *
     * @param catalogName
     * @param catalogStr
     * @param connectInfo
     */
    private RolapSchema(final String catalogName,
                        final String catalogStr,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {
        this(null, catalogName, catalogStr, connectInfo, dataSource);
    }
    /**
     * Create RolapSchema given the MD5 hash, catalog name and string (content)
     * and the connectInfo object.
     *
     * @param md5Bytes may be null
     * @param catalogName
     * @param catalogStr may be null
     * @param connectInfo
     */
    private RolapSchema(final byte[] md5Bytes,
                        final String catalogName,
                        final String catalogStr,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {
        this(md5Bytes, connectInfo, dataSource);

        load(catalogName, catalogStr);
    }

    private RolapSchema(final String catalogName,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {

        this((byte[]) null, connectInfo, dataSource);

        load(catalogName, null);
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * Method called by all constructors to load the catalog into DOM and build
     * application mdx and sql objects.
     *
     * @param catalogName
     * @param catalogStr
     */
    protected void load(String catalogName, String catalogStr) {
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            DOMWrapper def = null;
            if (catalogStr == null) {
                URL url = new URL(catalogName);
                def = xmlParser.parse(url);
            } else {
                def = xmlParser.parse(catalogStr);
            }
/*
            final DOMWrapper def = (catalogStr == null)
                    ? xmlParser.parse(new URL(catalogName))
                    : xmlParser.parse(catalogStr);
*/

            final MondrianDef.Schema xmlSchema = new MondrianDef.Schema(def);

            if (getLogger().isDebugEnabled()) {
                StringWriter sw = new StringWriter(4096);
                sw.write("RolapSchema.load: dump xmlschema\n");

                PrintWriter pw = new PrintWriter(sw);
                xmlSchema.display(pw, 4);
                pw.flush();

                getLogger().debug(sw.toString());
            }

            load(xmlSchema);

        } catch (MalformedURLException e) {
            throw Util.newError(e, "while parsing catalog " + catalogName);
        } catch (XOMException e) {
            throw Util.newError(e, "while parsing catalog " + catalogName);
        }
    }

    Role getDefaultRole() {
        return defaultRole;
    }

    public String getName() {
        Util.assertPostcondition(name != null, "return != null");
        Util.assertPostcondition(name.length() > 0, "return.length() > 0");
        return name;
    }

    private void load(MondrianDef.Schema xmlSchema) {
        this.name = xmlSchema.name;
        if (name == null || name.equals("")) {
            throw Util.newError("<Schema> name must be set");
        }
        // Validate public dimensions.
        for (int i = 0; i < xmlSchema.dimensions.length; i++) {
            MondrianDef.Dimension xmlDimension = xmlSchema.dimensions[i];
            if (xmlDimension.foreignKey != null) {
                throw MondrianResource.instance()
                        .newPublicDimensionMustNotHaveForeignKey(
                                xmlDimension.name);
            }
        }
        for (int i = 0; i < xmlSchema.cubes.length; i++) {
            MondrianDef.Cube xmlCube = xmlSchema.cubes[i];
            RolapCube cube = createCube(xmlSchema, xmlCube);
            Util.discard(cube);
        }
        for (int i = 0; i < xmlSchema.virtualCubes.length; i++) {
            MondrianDef.VirtualCube xmlVirtualCube = xmlSchema.virtualCubes[i];
            RolapCube cube = new RolapCube(this, xmlSchema, xmlVirtualCube);
            Util.discard(cube);
        }
        for (int i = 0; i < xmlSchema.roles.length; i++) {
            MondrianDef.Role xmlRole = xmlSchema.roles[i];
            Role role = createRole(xmlRole);
            mapNameToRole.put(xmlRole.name, role);
        }
        if (xmlSchema.defaultRole != null) {
            Role role = lookupRole(xmlSchema.defaultRole);
            if (role == null) {
                throw Util.newError("Role '" + xmlSchema.defaultRole + "' not found");
            }
            defaultRole = role;
        }
    }

    /**
     * Creates an registers a cube.
     *
     * @param xmlSchema XML schema definition in which to look up dimensions,
     *   necessary because dimensions may be defined after cubes.
     * @param xmlCube XML cube definition
     * @return A cube
     */
    private RolapCube createCube(MondrianDef.Schema xmlSchema,
                                 MondrianDef.Cube xmlCube) {
        Util.assertPrecondition(xmlSchema != null, "xmlSchema != null");
        RolapCube cube = new RolapCube(this, xmlSchema, xmlCube);
        return cube;
    }

    private Role createRole(MondrianDef.Role xmlRole) {
        Role role = new Role();
        for (int i = 0; i < xmlRole.schemaGrants.length; i++) {
            MondrianDef.SchemaGrant schemaGrant = xmlRole.schemaGrants[i];
            role.grant(this, getAccess(schemaGrant.access, schemaAllowed));
            for (int j = 0; j < schemaGrant.cubeGrants.length; j++) {
                MondrianDef.CubeGrant cubeGrant = schemaGrant.cubeGrants[j];
                Cube cube = lookupCube(cubeGrant.cube);
                if (cube == null) {
                    throw Util.newError("Unknown cube '" + cube + "'");
                }
                role.grant(cube, getAccess(cubeGrant.access, cubeAllowed));
                final SchemaReader schemaReader = cube.getSchemaReader(null);
                for (int k = 0; k < cubeGrant.dimensionGrants.length; k++) {
                    MondrianDef.DimensionGrant dimensionGrant = cubeGrant.dimensionGrants[k];
                    Dimension dimension = (Dimension)
                        schemaReader.lookupCompound(
                            cube, Util.explode(dimensionGrant.dimension), true,
                            Category.Dimension);
                    role.grant(dimension, getAccess(dimensionGrant.access, dimensionAllowed));
                }
                for (int k = 0; k < cubeGrant.hierarchyGrants.length; k++) {
                    MondrianDef.HierarchyGrant hierarchyGrant = cubeGrant.hierarchyGrants[k];
                    Hierarchy hierarchy = (Hierarchy)
                        schemaReader.lookupCompound(
                            cube, Util.explode(hierarchyGrant.hierarchy), true,
                            Category.Hierarchy);
                    final int hierarchyAccess = getAccess(hierarchyGrant.access, hierarchyAllowed);
                    Level topLevel = null;
                    if (hierarchyGrant.topLevel != null) {
                        if (hierarchyAccess != Access.CUSTOM) {
                            throw Util.newError("You may only specify 'topLevel' if access='custom'");
                        }
                        topLevel = (Level) schemaReader.lookupCompound(
                            cube, Util.explode(hierarchyGrant.topLevel), true,
                            Category.Level);
                    }
                    Level bottomLevel = null;
                    if (hierarchyGrant.bottomLevel != null) {
                        if (hierarchyAccess != Access.CUSTOM) {
                            throw Util.newError("You may only specify 'bottomLevel' if access='custom'");
                        }
                        bottomLevel = (Level) schemaReader.lookupCompound(
                            cube, Util.explode(hierarchyGrant.bottomLevel),
                            true, Category.Level);
                    }
                    role.grant(hierarchy, hierarchyAccess, topLevel, bottomLevel);
                    for (int m = 0; m < hierarchyGrant.memberGrants.length; m++) {
                        if (hierarchyAccess != Access.CUSTOM) {
                            throw Util.newError("You may only specify <MemberGrant> if <Hierarchy> has access='custom'");
                        }
                        MondrianDef.MemberGrant memberGrant = hierarchyGrant.memberGrants[m];
                        Member member = schemaReader.getMemberByUniqueName(Util.explode(memberGrant.member),true);
                        if (member.getHierarchy() != hierarchy) {
                            throw Util.newError("Member '" + member + "' is not in hierarchy '" + hierarchy + "'");
                        }
                        role.grant(member, getAccess(memberGrant.access, memberAllowed));
                    }
                }
            }
        }
        role.makeImmutable();
        return role;
    }

    private int getAccess(String accessString, int[] allowed) {
        final int access = Access.instance().getOrdinal(accessString);
        for (int i = 0; i < allowed.length; i++) {
            if (access == allowed[i]) {
                return access; // value is ok
            }
        }
        throw Util.newError("Bad value access='" + accessString + "'");
    }

    public Dimension createDimension(Cube cube, String xml) {
        MondrianDef.CubeDimension xmlDimension = null;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("Dimension")) {
                xmlDimension = new MondrianDef.Dimension(def);
            } else if (tagName.equals("DimensionUsage")) {
                xmlDimension = new MondrianDef.DimensionUsage(def);
            } else {
                throw new XOMException("Got <" + tagName +
                        "> when expecting <Dimension> or <DimensionUsage>");
            }
        } catch (XOMException e) {
            throw Util.newError(e, "Error while adding dimension to cube '" +
                    cube + "' from XML [" + xml + "]");
        }
        return ((RolapCube) cube).createDimension(xmlDimension);
    }

    public Cube createCube(String xml) {
        MondrianDef.Cube xmlDimension;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("Cube")) {
                xmlDimension = new MondrianDef.Cube(def);
            } else {
                throw new XOMException("Got <" + tagName +
                    "> when expecting <Cube>");
            }
        } catch (XOMException e) {
            throw Util.newError(e, "Error while creating cube from XML [" +
                xml + "]");
        }
        // Create empty XML schema, to keep the method happy. This is okay,
        // because there are no forward-references to resolve.
        final MondrianDef.Schema xmlSchema = new MondrianDef.Schema();
        return createCube(xmlSchema, xmlDimension);
    }

    /**
     * A collection of schemas, identified by their connection properties
     * (catalog name, JDBC URL, and so forth).
     *
     * <p>To lookup a schema, call <code>Pool.instance().{@link #get}</code>.
     */
    static class Pool {
        private static Pool pool = new Pool();

        private Map mapUrlToSchema = new HashMap();

        private Pool() {
        }

        static Pool instance() {
            return pool;
        }

        /**
         * Read a Reader until EOF and return as String.
         * Note: this ought to be in a Utility class.
         *
         * @param rdr  Reader to read.
         * @param bufferSize size of buffer to allocate for reading.
         * @return content of Reader as String or null if Reader was empty.
         * @throws IOException
         */
        public static String readFully(final Reader rdr, final int bufferSize)
                     throws IOException {

            if (bufferSize <= 0) {
                throw new IllegalArgumentException(
                            "Buffer size must be greater than 0");
            }

            final char[] buffer = new char[bufferSize];
            final StringBuffer buf = new StringBuffer(bufferSize);

            int len = rdr.read(buffer);
            while (len != -1) {
                buf.append(buffer, 0, len);
                len = rdr.read(buffer);
            }

            final String s = buf.toString();
            return (s.length() == 0) ? null : s;
        }

        /**
         * Create an MD5 hash of String.
         * Note: this ought to be in a Utility class.
         *
         * @param value String to create one way hash upon.
         * @return MD5 hash.
         * @throws NoSuchAlgorithmException
         */
        public static byte[] encodeMD5(final String value)
                throws NoSuchAlgorithmException {

            final MessageDigest md = MessageDigest.getInstance("MD5");
            final byte[] bytes = md.digest(value.getBytes());
            return bytes;
        }


        public static final int BUF_SIZE = 8096;

        /**
         * Read URL and return String containing content.
         * Note: this ought to be in a Utility class.
         *
         * @param urlStr actually a catalog URL
         * @return String containing content of catalog.
         * @throws MalformedURLException
         * @throws IOException
         */
        public static String readURL(final String urlStr)
                throws MalformedURLException, IOException {

            final URL url = new URL(urlStr);
            final Reader r =
                new BufferedReader(new InputStreamReader(url.openStream()));
            final String xmlCatalog = readFully(r, BUF_SIZE);
            return xmlCatalog;
        }

        /**
         * Compare two byte arrays for equality checking both length and
         * byte values.
         * Note: this ought to be in a Utility class.
         *
         * @param b1 first byte array.
         * @param b2 second byte array.
         * @return true if lengths and all values are equal and false otherwise.
         */
        private static boolean equals(final byte[] b1, final byte[] b2) {
            if (b1.length != b2.length) {
                return false;
            } else {
                for (int i = 0; i < b1.length; i++) {
                    if (b1[i] != b2[i]) {
                        return false;
                    }
                }
            }
            return true;
        }
        /**
         * Note: this is a place holder variable. The value of USE_MD5 should be
         * determined by a new mondrian property in the connectInfo string.
         * Currently a "normal" property is used simply so that I can test it.
         */
        private static final String MD5_PROP
                        = "mondrian.catalog.content.cache.enabled";
        private static final boolean USE_MD5    = Boolean.getBoolean(MD5_PROP);

        synchronized RolapSchema get(final String catalogName,
                                     final String connectionKey,
                                     final String jdbcUser,
                                     final String dataSourceStr,
                                     final Util.PropertyList connectInfo) {
            return get(catalogName,
                       connectionKey,
                       jdbcUser,
                       dataSourceStr,
                       null,
                       connectInfo);
        }
        synchronized RolapSchema get(final String catalogName,
                                     final DataSource dataSource,
                                     final Util.PropertyList connectInfo) {
            return get(catalogName,
                       null,
                       null,
                       null,
                       dataSource,
                       connectInfo);
        }
        private RolapSchema get(final String catalogName,
                                final String connectionKey,
                                final String jdbcUser,
                                final String dataSourceStr,
                                final DataSource dataSource,
                                final Util.PropertyList connectInfo) {

            RolapSchema schema = null;

            final String dynProc =
                connectInfo.get(RolapConnectionProperties.DynamicSchemaProcessor);
            // If there is a dynamic processor registered, use it. This
            // implies there is not MD5 based caching, but, as with the previous
            // implementation, if the catalog string is in the connectInfo
            // object as catalog content then it is used.
            if ( ! Util.isEmpty(dynProc)) {
                String catalogStr =
                    connectInfo.get(RolapConnectionProperties.CatalogContent);

                schema = (catalogStr == null)
                    // If a schema will be dynamically processed, caching is not
                    // possible.
                    ? new RolapSchema(catalogName,
                                       connectInfo,
                                       dynProc,
                                       dataSource)
                    // Got the catalog string, no need to get it again in the
                    // constructor
                    : new RolapSchema(catalogName,
                                       catalogStr,
                                       connectInfo,
                                       dataSource);

            } else {
                final String key = (dataSource == null)
                                ? makeKey(catalogName,
                                           connectionKey,
                                           jdbcUser,
                                           dataSourceStr)
                                : makeKey(catalogName,
                                           dataSource);

                schema = (RolapSchema) mapUrlToSchema.get(key);

                if (USE_MD5) {

                    String catalogStr = null;
                    byte[] md5Bytes = null;
                    try {
                        catalogStr = readURL(catalogName);
                        md5Bytes = encodeMD5(catalogStr);

                    } catch (Exception ex) {
                        // Note, can not throw an Exception from this method
                        // but just to show that all is not well in Mudville
                        // we print stack trace (for now - better to change
                        // method signature and throw).
                        ex.printStackTrace();
                    }

                    if ((schema == null) ||
                        md5Bytes == null ||
                        schema.md5Bytes == null ||
                        ! equals(schema.md5Bytes, md5Bytes)) {

                        schema = new RolapSchema(md5Bytes,
                                                 catalogName,
                                                 catalogStr,
                                                 connectInfo,
                                                 dataSource);

                        mapUrlToSchema.put(key, schema);
                    }

                } else if (schema == null) {
                    schema = new RolapSchema(catalogName,
                                             connectInfo,
                                             dataSource);

                    mapUrlToSchema.put(key, schema);
                }

            }
            return schema;
        }

        synchronized void remove(final String catalogName,
                                 final String connectionKey,
                                 final String jdbcUser,
                                 final String dataSourceStr) {
            final String key = makeKey(catalogName,
                                       connectionKey,
                                       jdbcUser,
                                       dataSourceStr);
            mapUrlToSchema.remove(key);
        }
        synchronized void remove(final String catalogName,
                                 final DataSource dataSource) {
            final String key = makeKey(catalogName,
                                       dataSource);
            mapUrlToSchema.remove(key);
        }

        synchronized void clear() {
            mapUrlToSchema.clear();
        }


        /**
         * Creates a key with which to identify a schema in the cache.
         */
        private static String makeKey(final String catalogName,
                                      final String connectionKey,
                                      final String jdbcUser,
                                      final String dataSourceStr) {
            final StringBuffer buf = new StringBuffer(100);

            appendIfNotNull(buf, catalogName);
            appendIfNotNull(buf, connectionKey);
            appendIfNotNull(buf, jdbcUser);
            appendIfNotNull(buf, dataSourceStr);

            final String key = buf.toString();
            return key;
        }
        /**
         * Creates a key with which to identify a schema in the cache.
         */
        private static String makeKey(final String catalogName,
                                      final DataSource dataSource) {
            final StringBuffer buf = new StringBuffer(100);

            appendIfNotNull(buf, catalogName);
            buf.append('.');
            buf.append("external#");
            buf.append(System.identityHashCode(dataSource));

            final String key = buf.toString();
            return key;
        }

        private static void appendIfNotNull(StringBuffer buf, String s) {
            if (s != null) {
                if (buf.length() > 0) {
                    buf.append('.');
                }
                buf.append(s);
            }
        }
    }

    public static void flushSchema(final String catalogName,
                                   final String connectionKey,
                                   final String jdbcUser,
                                   String dataSourceStr) {
        Pool.instance().remove(catalogName,
                               connectionKey,
                               jdbcUser,
                               dataSourceStr);
    }

    public static void flushSchema(final String catalogName,
                                   final DataSource dataSource) {
        Pool.instance().remove(catalogName,
                               dataSource);
    }

    public static void clearCache() {
        Pool.instance().clear();
    }

    public Cube lookupCube(final String cube, final boolean failIfNotFound) {
        Cube mdxCube = lookupCube(cube);
        if (mdxCube == null && failIfNotFound) {
            throw Util.getRes().newMdxCubeNotFound(cube);
        }
        return mdxCube;
    }

    /**
     * Finds a cube called 'cube' in the current catalog, or return null if no
     * cube exists.
     */
    protected Cube lookupCube(final String cubeName) {
        return (Cube) mapNameToCube.get(cubeName);
    }

    public Cube[] getCubes() {
        return (Cube[]) mapNameToCube.values().toArray(new RolapCube[0]);
    }

    void addCube(final Cube cube) {
        this.mapNameToCube.put(cube.getName(), cube);
    }

    public Hierarchy[] getSharedHierarchies() {
        return (RolapHierarchy[])
                mapSharedHierarchyNameToHierarchy.values().toArray(
                        new RolapHierarchy[0]);
    }

    RolapHierarchy getSharedHierarchy(final String name) {
        return (RolapHierarchy) mapSharedHierarchyNameToHierarchy.get(name);
    }

    public Role lookupRole(final String role) {
        return (Role) mapNameToRole.get(role);
    }

    /**
     * Gets a {@link MemberReader} with which to read a hierarchy. If the
     * hi This is only called by RolapHierarchyerarchy is shared (<code>sharedName</code> is not null), looks up
     * a reader from a cache, or creates one if necessary.
     *
     * This is only called by RolapHierarchy
     *
     * @synchronization thread safe
     */
    synchronized MemberReader createMemberReader(
        final String sharedName,
        final RolapHierarchy hierarchy,
        final String memberReaderClass) {

        MemberReader reader;
        if (sharedName != null) {
            reader = (MemberReader) mapSharedHierarchyToReader.get(sharedName);
            if (reader == null) {
                reader = createMemberReader(hierarchy, memberReaderClass);
                // share, for other uses of the same shared hierarchy
                if (false) mapSharedHierarchyToReader.put(sharedName, reader);
                mapSharedHierarchyNameToHierarchy.put(sharedName, hierarchy);
            } else {
                final RolapHierarchy sharedHierarchy = (RolapHierarchy)
                        mapSharedHierarchyNameToHierarchy.get(sharedName);
                final RolapDimension sharedDimension = (RolapDimension)
                        sharedHierarchy.getDimension();
                final RolapDimension dimension =
                    (RolapDimension) hierarchy.getDimension();
//                Util.assertTrue(
//                        dimension.getGlobalOrdinal() ==
//                        sharedDimension.getGlobalOrdinal());
            }
        } else {
            reader = createMemberReader(hierarchy, memberReaderClass);
        }
        return reader;
    }

    /**
     * Creates a {@link MemberReader} with which to read a hierarchy.
     */
    private MemberReader createMemberReader(
            final RolapHierarchy hierarchy,
            final String memberReaderClass) {

        if (memberReaderClass != null) {
            Exception e2 = null;
            try {
                Properties properties = null;
                Class clazz = Class.forName(memberReaderClass);
                Constructor constructor = clazz.getConstructor(new Class[] {
                    RolapHierarchy.class, Properties.class});
                Object o = constructor.newInstance(
                    new Object[] {hierarchy, properties});
                if (o instanceof MemberReader) {
                    return (MemberReader) o;
                } else if (o instanceof MemberSource) {
                    return new CacheMemberReader((MemberSource) o);
                } else {
                    throw Util.getRes().newInternal(
                        "member reader class " + clazz +
                        " does not implement " + MemberSource.class);
                }
            } catch (ClassNotFoundException e) {
                e2 = e;
            } catch (NoSuchMethodException e) {
                e2 = e;
            } catch (InstantiationException e) {
                e2 = e;
            } catch (IllegalAccessException e) {
                e2 = e;
            } catch (InvocationTargetException e) {
                e2 = e;
            }
            throw Util.getRes().newInternal(
                    "while instantiating member reader '" +
                    memberReaderClass, e2);
        } else {
            SqlMemberSource source = new SqlMemberSource(hierarchy);

            // The following code is disabled bcause
            // counting members is too slow. The test suite
            // runs faster without this. So the optimization here
            // is not to be too clever!

            // Also, the CacheMemberReader is buggy.

            int memberCount;
            if (false) {
                memberCount = source.getMemberCount();
            } else {
                memberCount = Integer.MAX_VALUE;
            }
            int largeDimensionThreshold =
                MondrianProperties.instance().getLargeDimensionThreshold();

            return (memberCount > largeDimensionThreshold)
                ? new SmartMemberReader(source)
                : (MemberReader) new CacheMemberReader(source);
        }
    }

    public SchemaReader getSchemaReader() {
        return new RolapSchemaReader(defaultRole, this) {
            public Cube getCube() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Connection for purposes of parsing and validation. Careful! It won't
     * have the correct locale or access-control profile.
     */
    RolapConnection getInternalConnection() {
        return internalConnection;
    }

    private Role createDefaultRole() {
        Role role = new Role();
        role.grant(this, Access.ALL);
        role.makeImmutable();
        return role;
    }

    /**
     * An <code>Extender</code> is a user-supplied class which extends the
     * system in some way. The <code>Extender</code> interfaces allows it to
     * describe itself, and receive parameters.
    interface Extender {
        String getDescription();
        ExtenderParameter[] getParameterDescriptions();
        void initialize(Properties properties);
    }
     **/

    /**
     * An <code>ExtenderParameter</code> describes a parameter of an {@link
     * Extender}.
    static class ExtenderParameter {
        String name;
        Class clazz;
        String description;
    }
     **/

    /**
     * <code>RolapStarRegistry</code> is a registry for {@link RolapStar}s.
     */
    class RolapStarRegistry {
        private List stars = new ArrayList();

        RolapStarRegistry() {
        }

        /**
         * Looks up a {@link RolapStar}, creating it if it does not exist.
         *
         * <p> {@link RolapStar.Table#addJoin} works in a similar way.
         */
        synchronized RolapStar getOrCreateStar(MondrianDef.Relation fact) {
            for (Iterator iterator = stars.iterator(); iterator.hasNext();) {
                RolapStar star = (RolapStar) iterator.next();
                if (star.getFactTable().getRelation().equals(fact)) {
                    return star;
                }
            }
            DataSource dataSource = getInternalConnection().getDataSource();
            RolapStar star = new RolapStar(RolapSchema.this,
                                           dataSource,
                                           fact);
            //star.factTable = new Table(star, fact, null, null);
            stars.add(star);
            return star;
        }
    }
    private RolapStarRegistry rolapStarRegistry = new RolapStarRegistry();
    public RolapStarRegistry getRolapStarRegistry() {
      return rolapStarRegistry;
    }

}

// End RolapSchema.java
