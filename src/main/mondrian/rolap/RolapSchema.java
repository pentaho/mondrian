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
import mondrian.olap.Member;
import mondrian.olap.fun.*;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;
import mondrian.rolap.aggmatcher.AggTableManager;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;
import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import javax.sql.DataSource;
import java.io.*;
import java.lang.ref.SoftReference;
import java.lang.reflect.*;
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

    private final String md5Bytes;

    /**
     * A schema's aggregation information
     */
    private AggTableManager aggTableManager;

    /**
     * This is basically a unique identifier for this RolapSchema instance
     * used it its equals and hashCode methods.
     */
    private String key;
    /**
     * Maps {@link String names of roles} to {@link Role roles with those names}.
     */
    private final Map mapNameToRole;
    /**
     * Maps {@link String names of sets} to {@link NamedSet named sets}.
     */
    private final Map mapNameToSet = new HashMap();
    /**
     * Table containing all standard MDX functions, plus user-defined functions
     * for this schema.
     */
    private FunTable funTable;

    private MondrianDef.Schema xmlSchema;

    private RolapSchema(final String key,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource,
                        final String md5Bytes) {
        this.key = key;
        this.md5Bytes = md5Bytes;
        // the order of the next two lines is important
        this.defaultRole = createDefaultRole();
        this.internalConnection =
            new RolapConnection(connectInfo, this, dataSource);

        this.mapSharedHierarchyNameToHierarchy = new HashMap();
        this.mapSharedHierarchyToReader = new HashMap();
        this.mapNameToCube = new HashMap();
        this.mapNameToRole = new HashMap();
        this.aggTableManager = new AggTableManager(this);
    }

    /**
     * Loads a schema using a dynamic loader.
     *
     * @param dynProcName
     * @param catalogName
     * @param connectInfo
     */
    private RolapSchema(
            final String key,
            final String catalogName,
            final Util.PropertyList connectInfo,
            final String dynProcName,
            final DataSource dataSource) {
        this(key, connectInfo, dataSource, (String) null);

        String catalogStr = null;

        try {
            final URL url = new URL(catalogName);

            final Class clazz = Class.forName(dynProcName);
            final Constructor ctor = clazz.getConstructor(new Class[0]);
            final DynamicSchemaProcessor dynProc =
                    (DynamicSchemaProcessor) ctor.newInstance(new Object[0]);
            catalogStr = dynProc.processSchema(url, connectInfo);

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
    private RolapSchema(final String key,
                        final String catalogName,
                        final String catalogStr,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {
        this(key, null, catalogName, catalogStr, connectInfo, dataSource);
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
    private RolapSchema(final String key,
                        final String md5Bytes,
                        final String catalogName,
                        final String catalogStr,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {
        this(key, connectInfo, dataSource, md5Bytes);

        load(catalogName, catalogStr);
    }

    private RolapSchema(final String key,
                        final String catalogName,
                        final Util.PropertyList connectInfo,
                        final DataSource dataSource) {

        this(key, connectInfo, dataSource, null);

        load(catalogName, null);
    }

    public boolean equals(Object o) {
        if (!(o instanceof RolapSchema)) {
            return false;
        }
        RolapSchema other = (RolapSchema) o;
        return other.key.equals(key);
    }
    public int hashCode() {
        return key.hashCode();
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

            final DOMWrapper def;
            if (catalogStr == null) {
                URL url = new URL(catalogName);
                def = xmlParser.parse(url);
            } else {
                def = xmlParser.parse(catalogStr);
            }

            xmlSchema = new MondrianDef.Schema(def);

            if (getLogger().isDebugEnabled()) {
                StringWriter sw = new StringWriter(4096);
                PrintWriter pw = new PrintWriter(sw);
                pw.println("RolapSchema.load: dump xmlschema");
                xmlSchema.display(pw, 2);
                pw.flush();
                getLogger().debug(sw.toString());
            }

            load(xmlSchema);

        } catch (MalformedURLException e) {
            throw Util.newError(e, "while parsing catalog " + catalogName);
        } catch (XOMException e) {
            throw Util.newError(e, "while parsing catalog " + catalogName);
        }

        aggTableManager.initialize();
    }

    Role getDefaultRole() {
        return defaultRole;
    }
    MondrianDef.Schema getXMLSchema() {
        return xmlSchema;
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
        // Validate user-defined functions. Must be done before we validate
        // calculated members, because calculated members will need to use the
        // function table.
        final Map mapNameToUdf = new HashMap();
        for (int i = 0; i < xmlSchema.userDefinedFunctions.length; i++) {
            MondrianDef.UserDefinedFunction udf = xmlSchema.userDefinedFunctions[i];
            defineFunction(mapNameToUdf, udf.name, udf.className);
        }
        final RolapSchemaFunctionTable funTable =
                new RolapSchemaFunctionTable(mapNameToUdf.values());
        funTable.init();
        this.funTable = funTable;

        // Validate public dimensions.
        for (int i = 0; i < xmlSchema.dimensions.length; i++) {
            MondrianDef.Dimension xmlDimension = xmlSchema.dimensions[i];
            if (xmlDimension.foreignKey != null) {
                throw MondrianResource.instance()
                        .PublicDimensionMustNotHaveForeignKey.ex(
                                xmlDimension.name);
            }
        }
        for (int i = 0; i < xmlSchema.cubes.length; i++) {
            MondrianDef.Cube xmlCube = xmlSchema.cubes[i];
            if (xmlCube.isEnabled()) {
                RolapCube cube = new RolapCube(this, xmlSchema, xmlCube);
                Util.discard(cube);
            }
        }
        for (int i = 0; i < xmlSchema.virtualCubes.length; i++) {
            MondrianDef.VirtualCube xmlVirtualCube = xmlSchema.virtualCubes[i];
            if (xmlVirtualCube.isEnabled()) {
                RolapCube cube = new RolapCube(this, xmlSchema, xmlVirtualCube);
                Util.discard(cube);
            }
        }
        for (int i = 0; i < xmlSchema.namedSets.length; i++) {
            MondrianDef.NamedSet xmlNamedSet = xmlSchema.namedSets[i];
            mapNameToSet.put(xmlNamedSet.name, createNamedSet(xmlNamedSet));
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

    private NamedSet createNamedSet(MondrianDef.NamedSet xmlNamedSet) {
        final String formulaString = xmlNamedSet.getFormula();
        final Exp exp;
        try {
            exp = getInternalConnection().parseExpression(formulaString);
        } catch (Exception e) {
            throw MondrianResource.instance().NamedSetHasBadFormula.ex(
                    xmlNamedSet.name, e);
        }
        final Formula formula = new Formula(
                new String[] {xmlNamedSet.name},
                exp);
        return formula.getNamedSet();
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
                    MondrianDef.DimensionGrant dimensionGrant =
                        cubeGrant.dimensionGrants[k];
                    Dimension dimension = (Dimension)
                        schemaReader.lookupCompound(
                            cube, Util.explode(dimensionGrant.dimension), true,
                            Category.Dimension);
                    role.grant(dimension,
                        getAccess(dimensionGrant.access, dimensionAllowed));
                }
                for (int k = 0; k < cubeGrant.hierarchyGrants.length; k++) {
                    MondrianDef.HierarchyGrant hierarchyGrant =
                        cubeGrant.hierarchyGrants[k];
                    Hierarchy hierarchy = (Hierarchy)
                        schemaReader.lookupCompound(
                            cube, Util.explode(hierarchyGrant.hierarchy), true,
                            Category.Hierarchy);
                    final int hierarchyAccess =
                        getAccess(hierarchyGrant.access, hierarchyAllowed);
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
        RolapCube cube = new RolapCube(this, xmlSchema, xmlDimension);
        return cube;
    }

    /**
     * A collection of schemas, identified by their connection properties
     * (catalog name, JDBC URL, and so forth).
     *
     * <p>To lookup a schema, call <code>Pool.instance().{@link #get(String, DataSource, Util.PropertyList)}</code>.
     */
    static class Pool {
        private static MessageDigest md = null;
        static {
            try {
                md = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

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
        public static String encodeMD5(final String value) {
            md.reset();
            final byte[] bytes = md.digest(value.getBytes());
            return (bytes != null) ? new String(bytes) : null;
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
         */
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

            final String key = (dataSource == null)
                            ? makeKey(catalogName,
                                      connectionKey,
                                      jdbcUser,
                                      dataSourceStr)
                            : makeKey(catalogName,
                                      dataSource);

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
                    ? new RolapSchema(key,
                                      catalogName,
                                      connectInfo,
                                      dynProc,
                                      dataSource)
                    // Got the catalog string, no need to get it again in the
                    // constructor
                    : new RolapSchema(key,
                                      catalogName,
                                      catalogStr,
                                      connectInfo,
                                      dataSource);

                if (LOGGER.isDebugEnabled()) {
                    String msg = "Pool.get: create schema \"" +
                        catalogName +
                        "\" using dynamic processor";
                    LOGGER.debug(msg);
                }
            } else {

                if (USE_MD5) {
                    // Different catalogNames can actually yield the same
                    // catalogStr! So, we use the MD5 as the key as well as
                    // the key made above - its has two entries in the
                    // mapUrlToSchema Map. We must then also during the
                    // remove operation make sure we remove both.

                    String catalogStr = null;
                    String md5Bytes = null;
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
                    if (md5Bytes != null) {
                        SoftReference ref =
                            (SoftReference) mapUrlToSchema.get(md5Bytes);
                        if (ref != null) {
                            schema = (RolapSchema) ref.get();
                            if (schema == null) {
                                // clear out the reference since schema is null
                                mapUrlToSchema.remove(key);
                                mapUrlToSchema.remove(md5Bytes);
                            }
                        }
                    }

                    if ((schema == null) ||
                        md5Bytes == null ||
                        schema.md5Bytes == null ||
                        ! schema.md5Bytes.equals(md5Bytes)) {

                        schema = new RolapSchema(key,
                                                 md5Bytes,
                                                 catalogName,
                                                 catalogStr,
                                                 connectInfo,
                                                 dataSource);

                        SoftReference ref = new SoftReference(schema);
                        if (md5Bytes != null) {
                            mapUrlToSchema.put(md5Bytes, ref);
                        }
                        mapUrlToSchema.put(key, ref);

                        if (LOGGER.isDebugEnabled()) {
                            String msg = "Pool.get: create schema \"" +
                                catalogName +
                                "\" with MD5";
                            LOGGER.debug(msg);
                        }

                    } else if (LOGGER.isDebugEnabled()) {
                        String msg = "Pool.get: schema \"" +
                            catalogName +
                            "\" exists already with MD5";
                        LOGGER.debug(msg);
                    }

                } else {
                    SoftReference ref = (SoftReference) mapUrlToSchema.get(key);
                    if (ref != null) {
                        schema = (RolapSchema) ref.get();
                        if (schema == null) {
                            // clear out the reference since schema is null
                            mapUrlToSchema.remove(key);
                        }
                    }

                    if (schema == null) {
                        schema = new RolapSchema(key,
                                                 catalogName,
                                                 connectInfo,
                                                 dataSource);

                        mapUrlToSchema.put(key, new SoftReference(schema));

                        if (LOGGER.isDebugEnabled()) {
                            String msg = "Pool.get: create schema \"" +
                                catalogName +
                                "\"";
                            LOGGER.debug(msg);
                        }

                    } else if (LOGGER.isDebugEnabled()) {
                        String msg = "Pool.get: schema \"" +
                            catalogName +
                            "\" exists already ";
                        LOGGER.debug(msg);
                    }

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
            if (LOGGER.isDebugEnabled()) {
                String msg = "Pool.remove: schema \"" +
                     catalogName +
                    "\" and datasource string \"" +
                    dataSourceStr +
                    "\"";
                LOGGER.debug(msg);
            }

            remove(key);
        }
        synchronized void remove(final String catalogName,
                                 final DataSource dataSource) {
            final String key = makeKey(catalogName,
                                       dataSource);
            if (LOGGER.isDebugEnabled()) {
                String msg = "Pool.remove: schema \"" +
                    catalogName +
                    "\" and datasource object";
                LOGGER.debug(msg);
            }

            remove(key);
        }
        private void remove(String key) {
            SoftReference ref = (SoftReference) mapUrlToSchema.get(key);
            if (ref != null) {
                RolapSchema schema = (RolapSchema) ref.get();
                if ((schema != null) && (schema.md5Bytes != null)) {
                    mapUrlToSchema.remove(schema.md5Bytes);
                }
            }
            mapUrlToSchema.remove(key);
        }

        synchronized void clear() {
            if (LOGGER.isDebugEnabled()) {
                String msg = "Pool.clear: clearing all RolapSchemas";
                LOGGER.debug(msg);
            }

            mapUrlToSchema.clear();
        }

        /**
         * This returns an iterator over a copy of the RolapSchema's container.
         *
         * @return Iterator over RolapSchemas.
         */
        synchronized Iterator getRolapSchemas() {
            List list = new ArrayList();
            for (Iterator it = mapUrlToSchema.values().iterator();
                    it.hasNext(); ) {
                SoftReference ref = (SoftReference) it.next();
                RolapSchema schema = (RolapSchema) ref.get();
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
                        LOGGER.warn(ex);
                    }
                }
            }
            return list.iterator();
        }
        synchronized boolean contains(RolapSchema rolapSchema) {
            return mapUrlToSchema.containsKey(rolapSchema.key);
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
    public static Iterator getRolapSchemas() {
        return Pool.instance().getRolapSchemas();
    }
    public static boolean cacheContains(RolapSchema rolapSchema) {
        return Pool.instance().contains(rolapSchema);
    }

    public Cube lookupCube(final String cube, final boolean failIfNotFound) {
        Cube mdxCube = lookupCube(cube);
        if (mdxCube == null && failIfNotFound) {
            throw MondrianResource.instance().MdxCubeNotFound.ex(cube);
        }
        return mdxCube;
    }

    /**
     * Finds a cube called 'cube' in the current catalog, or return null if no
     * cube exists.
     */
    protected Cube lookupCube(final String cubeName) {
        return (Cube) mapNameToCube.get(
                MondrianProperties.instance().CaseSensitive.get() ?
                cubeName :
                cubeName.toUpperCase());
    }

    public List getCubesWithStar(RolapStar star) {
        List list = new ArrayList();
        for (Iterator it = mapNameToCube.values().iterator(); it.hasNext(); ) {
            RolapCube cube = (RolapCube) it.next();
            if (star == cube.getStar()) {
                list.add(cube);
            }
        }
        return list;
    }

    /**
     * Adds a cube to the cube name map.
     * @see #lookupCube(String)
     */
    protected void addCube(final Cube cube) {
        this.mapNameToCube.put(
                MondrianProperties.instance().CaseSensitive.get() ?
                cube.getName() :
                cube.getName().toUpperCase(),
                cube);
    }

    public Cube[] getCubes() {
        return (Cube[]) mapNameToCube.values().toArray(new RolapCube[0]);
    }

    public Iterator getCubeIterator() {
        return mapNameToCube.values().iterator();
    }

    public Hierarchy[] getSharedHierarchies() {
        return (RolapHierarchy[])
                mapSharedHierarchyNameToHierarchy.values().toArray(
                        new RolapHierarchy[0]);
    }

    RolapHierarchy getSharedHierarchy(final String name) {
        return (RolapHierarchy) mapSharedHierarchyNameToHierarchy.get(name);
    }

    public NamedSet getNamedSet(String name) {
        return (NamedSet) mapNameToSet.get(name);
    }

    public Role lookupRole(final String role) {
        return (Role) mapNameToRole.get(role);
    }

    public FunTable getFunTable() {
        return funTable;
    }

    /**
     * Defines a user-defined function in this table.
     *
     * <p>If the function is not valid, throws an error.
     *
     * @param name Name of the function.
     * @param className Name of the class which implements the function.
     *   The class must implement {@link mondrian.spi.UserDefinedFunction}
     *   (otherwise it is a user-error).
     */
    private void defineFunction(
            Map mapNameToUdf,
            String name,
            String className) {
        // Lookup class.
        final Class klass;
        try {
            klass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw MondrianResource.instance().UdfClassNotFound.ex(name,
                    className);
        }
        // Find a constructor.
        Constructor constructor;
        Object[] args = {};
        // 1. Look for a constructor "public Udf(String name)".
        try {
            constructor = klass.getConstructor(new Class[] {String.class});
            if (Modifier.isPublic(constructor.getModifiers())) {
                args = new Object[] {name};
            } else {
                constructor = null;
            }
        } catch (NoSuchMethodException e) {
            constructor = null;
        }
        // 2. Otherwise, look for a constructor "public Udf()".
        if (constructor == null) {
            try {
                constructor = klass.getConstructor(new Class[] {});
                if (Modifier.isPublic(constructor.getModifiers())) {
                    args = new Object[] {};
                } else {
                    constructor = null;
                }
            } catch (NoSuchMethodException e) {
                constructor = null;
            }
        }
        // 3. Else, no constructor suitable.
        if (constructor == null) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(name,
                    className, UserDefinedFunction.class.getName());
        }
        // Instantiate class.
        final UserDefinedFunction udf;
        try {
            udf = (UserDefinedFunction) constructor.newInstance(args);
        } catch (InstantiationException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(name,
                    className, UserDefinedFunction.class.getName());
        } catch (IllegalAccessException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(name,
                    className, UserDefinedFunction.class.getName());
        } catch (ClassCastException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(name,
                    className, UserDefinedFunction.class.getName());
        } catch (InvocationTargetException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(name,
                    className, UserDefinedFunction.class.getName());
        }
        // Validate function.
        validateFunction(udf);
        // Check for duplicate.
        UserDefinedFunction existingUdf =
                (UserDefinedFunction) mapNameToUdf.get(name);
        if (existingUdf != null) {
            throw MondrianResource.instance().UdfDuplicateName.ex(name);
        }
        mapNameToUdf.put(name, udf);
    }

    /**
     * Throws an error if a user-defined function does not adhere to the
     * API.
     */
    private void validateFunction(final UserDefinedFunction udf) {
        // Check that the name is not null or empty.
        final String udfName = udf.getName();
        if (udfName == null || udfName.equals("")) {
            throw Util.newInternal("User-defined function defined by class '" +
                    udf.getClass() + "' has empty name");
        }
        // It's OK for the description to be null.
        final String description = udf.getDescription();
        Util.discard(description);
        final Type[] parameterTypes = udf.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            Type parameterType = parameterTypes[i];
            if (parameterType == null) {
                throw Util.newInternal("Invalid user-defined function '" +
                        udfName + "': parameter type #" + i +
                        " is null");
            }
        }
        // It's OK for the reserved words to be null or empty.
        final String[] reservedWords = udf.getReservedWords();
        Util.discard(reservedWords);
        // Test that the function returns a sensible type when given the FORMAL
        // types. It may still fail when we give it the ACTUAL types, but it's
        // impossible to check that now.
        final Type returnType = udf.getReturnType(parameterTypes);
        if (returnType == null) {
            throw Util.newInternal("Invalid user-defined function '" +
                    udfName + "': return type is null");
        }
        final Syntax syntax = udf.getSyntax();
        if (syntax == null) {
            throw Util.newInternal("Invalid user-defined function '" +
                    udfName + "': syntax is null");
        }
    }

    /**
     * Gets a {@link MemberReader} with which to read a hierarchy. If the
     * hierarchy is shared (<code>sharedName</code> is not null), looks up
     * a reader from a cache, or creates one if necessary.
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
//                final RolapHierarchy sharedHierarchy = (RolapHierarchy)
//                        mapSharedHierarchyNameToHierarchy.get(sharedName);
//                final RolapDimension sharedDimension = (RolapDimension)
//                        sharedHierarchy.getDimension();
//                final RolapDimension dimension =
//                    (RolapDimension) hierarchy.getDimension();
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
                    throw Util.newInternal("member reader class " + clazz +
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
            throw Util.newInternal(e2,
                    "while instantiating member reader '" + memberReaderClass);
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
                    MondrianProperties.instance().LargeDimensionThreshold.get();

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
    public RolapConnection getInternalConnection() {
        return internalConnection;
    }

    private Role createDefaultRole() {
        Role role = new Role();
        role.grant(this, Access.ALL);
        role.makeImmutable();
        return role;
    }

    private RolapStar makeRolapStar(final MondrianDef.Relation fact) {
        DataSource dataSource = getInternalConnection().getDataSource();
        RolapStar star = new RolapStar(this, dataSource, fact);
        return star;
    }

    /**
     * <code>RolapStarRegistry</code> is a registry for {@link RolapStar}s.
     */
    class RolapStarRegistry {
        private final Map stars = new HashMap();

        RolapStarRegistry() {
        }

        /**
         * Looks up a {@link RolapStar}, creating it if it does not exist.
         *
         * <p> {@link RolapStar.Table#addJoin} works in a similar way.
         */
        synchronized RolapStar getOrCreateStar(final MondrianDef.Relation fact) {
            String factTableName = fact.toString();
            RolapStar star = (RolapStar) stars.get(factTableName);
            if (star == null) {
                star = makeRolapStar(fact);
                stars.put(factTableName, star);
            }
            return star;
        }
        synchronized RolapStar getStar(final String factTableName) {
            return (RolapStar) stars.get(factTableName);
        }
        synchronized Iterator getStars() {
            return stars.values().iterator();
        }
    }

    private RolapStarRegistry rolapStarRegistry = new RolapStarRegistry();

    public RolapStarRegistry getRolapStarRegistry() {
        return rolapStarRegistry;
    }

    /**
     * Function table which contains all of the user-defined functions in this
     * schema, plus all of the standard functions.
     */
    class RolapSchemaFunctionTable extends FunTableImpl {
        private final List udfList;

        RolapSchemaFunctionTable(Collection udfs) {
            udfList = new ArrayList(udfs);
        }

        protected void defineFunctions() {
            final FunTable builtinFunTable = BuiltinFunTable.instance();
            final List reservedWords = builtinFunTable.getReservedWords();
            for (int i = 0; i < reservedWords.size(); i++) {
                String reservedWord = (String) reservedWords.get(i);
                defineReserved(reservedWord);
            }
            final List resolvers = builtinFunTable.getResolvers();
            for (int i = 0; i < resolvers.size(); i++) {
                Resolver resolver = (Resolver) resolvers.get(i);
                define(resolver);
            }
            for (int i = 0; i < udfList.size(); i++) {
                UserDefinedFunction udf = (UserDefinedFunction) udfList.get(i);
                define(new UdfResolver(udf));
            }
        }


        public List getFunInfoList() {
            return Collections.unmodifiableList(this.funInfoList);
        }
    }

    public RolapStar getStar(final String factTableName) {
      return getRolapStarRegistry().getStar(factTableName);
    }

    public Iterator getStars() {
      return getRolapStarRegistry().getStars();
    }
    public void flushRolapStarCaches() {
        for (Iterator itStars = getStars(); itStars.hasNext(); ) {
            RolapStar star = (RolapStar) itStars.next();
            // this will only flush the star's aggregate cache if
            // 1) DisableCaching is true or the star's cube has
            // cacheAggregations set to false in the schema.
            star.clearCache();
        }
    }
    public static void flushAllRolapStarCaches() {
        for (Iterator itSchemas = RolapSchema.getRolapSchemas();
                itSchemas.hasNext(); ) {

            RolapSchema schema = (RolapSchema) itSchemas.next();
            schema.flushRolapStarCaches();
        }
    }

}

// End RolapSchema.java
