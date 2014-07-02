/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.MondrianDef.RealOrCalcColumnDef;
import mondrian.olap.NamedSet;
import mondrian.olap.Property;
import mondrian.olap.fun.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapLevel.HideMemberCondition;
import mondrian.rolap.RolapSchema.UnresolvedColumn;
import mondrian.rolap.aggmatcher.ExplicitRules;
import mondrian.server.Locus;
import mondrian.spi.*;
import mondrian.spi.CellFormatter;
import mondrian.spi.MemberFormatter;
import mondrian.spi.PropertyFormatter;
import mondrian.spi.impl.Scripts;
import mondrian.util.*;
import mondrian.util.Bug;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.log4j.Logger;

import org.eigenbase.xom.*;

import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.*;

import javax.sql.DataSource;

import static mondrian.olap.Util.first;

/**
 * Creates a schema from XML.
 *
 * @author jhyde
 */
public class RolapSchemaLoader {
    private static final Logger LOGGER = Logger.getLogger(RolapSchema.class);

    private static final Set<Access> schemaAllowed =
        Olap4jUtil.enumSetOf(
            Access.NONE,
            Access.ALL,
            Access.ALL_DIMENSIONS,
            Access.CUSTOM);

    private static final Set<Access> cubeAllowed =
        Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.CUSTOM);

    private static final Set<Access> dimensionAllowed =
        Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.CUSTOM);

    private static final Set<Access> hierarchyAllowed =
        Olap4jUtil.enumSetOf(Access.NONE, Access.ALL, Access.CUSTOM);

    private static final Set<Access> memberAllowed =
        Olap4jUtil.enumSetOf(Access.NONE, Access.ALL);

    private static final Map<String, Dialect.Datatype> DATATYPE_MAP =
        new HashMap<String, Dialect.Datatype>();

    /**
     * Name of the sole level in the Measures hierarchy.
     *
     * @see Dimension#MEASURES_NAME
     */
    static final String MEASURES_LEVEL_NAME = "MeasuresLevel";

    private static final RolapSchema.RoleFactory DUMMY_ROLE =
        new RolapSchema.ConstantRoleFactory(new RoleImpl());

    static {
        for (Dialect.Datatype datatype : Dialect.Datatype.values()) {
            DATATYPE_MAP.put(datatype.name(), datatype);
        }
    }

    private static final String EMPTY_TABLE_NAME = "_empty";
    private static final String EMPTY_TABLE_SOLE_COLUMN_NAME = "c";
    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("[a-z]+:");

    private static final Object DUMMY = new Object();
    private static final String FACT_COUNT_MEASURE_NAME = "Fact Count";

    RolapSchema schema;
    private PhysSchemaBuilder physSchemaBuilder;
    private final RolapSchemaValidatorImpl validator =
        new RolapSchemaValidatorImpl();

    private final Map<
        Pair<RolapMeasureGroup, RolapCubeDimension>,
        RolapSchema.PhysPath> dimensionPaths =
        new HashMap<
            Pair<RolapMeasureGroup, RolapCubeDimension>,
            RolapSchema.PhysPath>();

    private final Handler handler =
        new RolapSchemaLoaderHandlerImpl() {
            protected List<RolapSchema.MondrianSchemaException> getWarningList()
            {
                return schema == null ? null : schema.warningList;
            }
        };

    private MissingLinkAction missingLinkAction;

    private final List<Util.Function0> postCubeActions =
        new ArrayList<Util.Function0>();

    /**
     * Intentionally not static. SimpleDateFormat is not thread-safe.
     * RolapSchemaLoader always operates on a single thread.
     */
    private final SimpleDateFormat SIMPLE_DATE_FORMAT =
        new SimpleDateFormat("yyyy-MM-dd");

    private final MultiValueMap cubeToDimMap = new MultiValueMap();

    private final List<RolapMember> measureList = new ArrayList<RolapMember>();
    private final List<RolapMember> aggFactCountMeasureList =
        new ArrayList<RolapMember>();

    /** Deferred actions to assign default members of hierarchies, for the
     * cube currently being loaded. */
    private final List<AssignDefaultMember> assignDefaultMembers =
        new ArrayList<AssignDefaultMember>();

    private final Map<RolapHierarchy, List<RolapCubeHierarchy>> cubeHierMap =
        new HashMap<RolapHierarchy, List<RolapCubeHierarchy>>();

    final Map<String, List<Larders.Resource>> resourceMap =
        new HashMap<String, List<Larders.Resource>>();

    final Map<String, BitSet> resourceHierarchyTags =
        new HashMap<String, BitSet>();

    /**
     * Creates a RolapSchemaLoader.
     *
     * @param schema Schema being loaded (may be null, and populated later)
     */
    RolapSchemaLoader(RolapSchema schema) {
        this.schema = schema;
    }

    /**
     * Loads the schema.
     *
     * <p>Called immediately after the RolapSchemaLoader's constructor. Creates
     * the schema, loads the catalog into DOM and builds application MDX and SQL
     * objects.
     *
     * @param key Key
     * @param md5Bytes may be null
     * @param catalogUrl URL of catalog
     * @param catalogStr may be null
     * @param connectInfo Connection properties
     * @param dataSource Data source
     * @return Populated schema
     */
    private RolapSchema loadStage0(
        final SchemaKey key,
        ByteString md5Bytes,
        final String catalogUrl,
        String catalogStr,
        final Util.PropertyList connectInfo,
        final DataSource dataSource)
    {
        assert schema == null;
        try {
            final org.eigenbase.xom.Parser xmlParser =
                XOMUtil.createDefaultParser();
            xmlParser.setKeepPositions(true);

            final DOMWrapper def;
            if (catalogStr == null) {
                InputStream in = null;
                try {
                    in = Util.readVirtualFile(catalogUrl);
                    def = xmlParser.parse(in);
                } finally {
                    if (in != null) {
                        in.close();
                    }
                }

                // Compute catalog string, if needed for debug or for computing
                // Md5 hash.
                if (LOGGER.isDebugEnabled() || md5Bytes == null) {
                    try {
                        catalogStr = Util.readVirtualFileAsString(catalogUrl);
                    } catch (java.io.IOException ex) {
                        LOGGER.debug("RolapSchema.load: ex=" + ex);
                        catalogStr = "?";
                    }
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "RolapSchema.load: content: \n"
                        + catalogStr);
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "RolapSchema.load: catalogStr: \n"
                        + catalogStr);
                }

                def = xmlParser.parse(catalogStr);
            }

            boolean useContentChecksum;
            if (md5Bytes == null) {
                // If a null catalogStr was passed in, we should have
                // computed it above by re-reading the catalog URL.
                assert catalogStr != null;
                md5Bytes = new ByteString(Util.digestMd5(catalogStr));
                useContentChecksum = false;
            } else {
                useContentChecksum = true;
            }

            final MondrianDef.Schema xmlSchema;
            MondrianDef.Handler.THREAD_LOCAL.set(handler);
            final boolean legacy = isLegacy(def);
            if (legacy) {
                LOGGER.warn("Model is in legacy format");
                xmlSchema = RolapSchemaUpgrader.upgrade(
                    this,
                    def,
                    key,
                    md5Bytes,
                    connectInfo,
                    dataSource,
                    useContentChecksum);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "schema after conversion:\n"
                        + xmlSchema.toXML());
                }
            } else {
                checkSchemaVersion(def);
                xmlSchema = new MondrianDef.Schema(def);
            }

            return loadStage1(
                key,
                md5Bytes,
                dirName(catalogUrl),
                useContentChecksum,
                connectInfo,
                dataSource,
                xmlSchema);
        } catch (XOMException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        } catch (IOException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        } finally {
            MondrianDef.Handler.THREAD_LOCAL.set(null);
        }
    }

    private RolapSchema loadStage1(
        SchemaKey key,
        ByteString md5Bytes,
        String catalogDirUrl,
        boolean useContentChecksum,
        Util.PropertyList connectInfo,
        DataSource dataSource,
        MondrianDef.Schema xmlSchema)
        throws XOMException
    {
        if (LOGGER.isDebugEnabled()) {
            StringWriter sw = new StringWriter(4096);
            PrintWriter pw = new PrintWriter(sw);
            pw.println("RolapSchema.load: dump xmlschema");
            xmlSchema.display(pw, 2);
            pw.flush();
            LOGGER.debug(sw.toString());
        }

        // Load localized resources. (Needs to happen before we create Schema's
        // larder.)
        final Set<Locale> locales = new HashSet<Locale>();
        loadResources(catalogDirUrl, xmlSchema, locales);

        schema =
            new RolapSchema(
                key,
                connectInfo,
                dataSource,
                md5Bytes,
                useContentChecksum,
                xmlSchema.name,
                toBoolean(xmlSchema.quoteSql, true),
                Collections.unmodifiableSet(locales),
                createLarder(
                    ".schema",
                    xmlSchema.getAnnotations(),
                    xmlSchema.name,
                    xmlSchema.caption,
                    xmlSchema.description).build());

        validator.putXml(schema, xmlSchema);
        missingLinkAction =
            Util.lookup(
                Util.toUpperCase(xmlSchema.missingLink),
                MissingLinkAction.WARNING);

        loadStage2(
            xmlSchema,
            xmlSchema.getPhysicalSchema(),
            xmlSchema.getUserDefinedFunctions(),
            xmlSchema.getDimensions(),
            xmlSchema.getParameters(),
            xmlSchema.getCubes());
        return schema;
    }

    /**
     * Returns whether a schema is in legacy (that is, pre-Mondrian-4.0) format.
     *
     * @param def Parsed schema
     * @return Whether schema is in legacy format
     */
    private boolean isLegacy(DOMWrapper def) {
        final String metamodelVersion =
            def.getAttribute("metamodelVersion");
        if (metamodelVersion == null) {
            return !hasMondrian4Elements(def);
        }
        final String[] versionParts = metamodelVersion.split("\\.");
        final String majorVersion =
            versionParts.length > 0 ? versionParts[0] : "";
        if (majorVersion.compareTo("4") < 0) {
            return true;
        }
        return false;
    }

    void loadStage2(
        MondrianDef.Schema xmlSchema,
        final MondrianDef.PhysicalSchema xmlPhysicalSchema,
        final List<MondrianDef.UserDefinedFunction> xmlUserDefinedFunctions,
        final List<MondrianDef.Dimension> xmlDimensions,
        final List<MondrianDef.Parameter> xmlParameters,
        final List<MondrianDef.Cube> xmlCubes)
    {
        final Dialect dialect = schema.getDialect();
        schema.physicalSchema =
            validatePhysicalSchema(
                xmlPhysicalSchema, getHandler(), dialect, schema);

        // Validate user-defined functions. Must be done before we validate
        // calculated members, because calculated members will need to use the
        // function table.
        final Map<String, UdfResolver.UdfFactory> mapNameToUdf =
            new HashMap<String, UdfResolver.UdfFactory>();
        for (MondrianDef.UserDefinedFunction udf : xmlUserDefinedFunctions) {
            final Scripts.ScriptDefinition scriptDef = toScriptDef(udf.script);
            schema.defineFunction(
                mapNameToUdf, udf.name, udf.className, scriptDef);
        }
        schema.initFunctionTable(mapNameToUdf.values());

        // Create parameters.
        Set<String> parameterNames = new HashSet<String>();
        for (MondrianDef.Parameter xmlParameter : xmlParameters) {
            String parameterName = xmlParameter.name;
            if (!parameterNames.add(parameterName)) {
                throw MondrianResource.instance().DuplicateSchemaParameter.ex(
                    parameterName);
            }
            Type type;
            if (xmlParameter.type.equals("String")) {
                type = new StringType();
            } else if (xmlParameter.type.equals("Numeric")) {
                type = new NumericType();
            } else {
                type = new MemberType(null, null, null, null);
            }
            final String description = xmlParameter.description;
            final boolean modifiable = toBoolean(xmlParameter.modifiable, true);
            String defaultValue = xmlParameter.defaultValue;
            RolapSchemaParameter param =
                new RolapSchemaParameter(
                    schema, parameterName, defaultValue, description, type,
                    modifiable);
            validator.putXml(param, xmlParameter);
        }

        // Create cubes.
        for (MondrianDef.Cube xmlCube : xmlCubes) {
            final RolapCube cube = createCube(schema, xmlCube, xmlSchema);
            if (cube == null) {
                continue;
            }
            Util.discard(cube);
            for (Util.Function0 action : postCubeActions) {
                action.apply();
            }
            postCubeActions.clear();
        }

        // Create named sets.
        for (MondrianDef.NamedSet xmlNamedSet : xmlSchema.getNamedSets()) {
            schema.addNamedSet(
                xmlNamedSet.name,
                createNamedSet(null, xmlNamedSet));
        }

        // Create roles.
        //
        // Union roles can depend on roles that occur later, so we iterate
        // until we reach a fixed point.
        Map<String, RolapSchema.RoleFactory> rolesByName =
            new LinkedHashMap<String, RolapSchema.RoleFactory>();
        for (MondrianDef.Role xmlRole : xmlSchema.getRoles()) {
            if (rolesByName.containsKey(xmlRole.name)) {
                handler.error(
                    "Duplicate role '" + xmlRole.name + "'",
                    xmlRole,
                    "name");
                continue;
            }
            rolesByName.put(xmlRole.name, DUMMY_ROLE);
        }
        for (;;) {
            final int prevMapSize = rolesByName.size();
            MondrianDef.Role missed = null;
            for (MondrianDef.Role xmlRole : xmlSchema.getRoles()) {
                if (rolesByName.get(xmlRole.name) != DUMMY_ROLE) {
                    continue;
                }
                final RolapSchema.RoleFactory role =
                    createRole(xmlRole, rolesByName);
                if (role == DUMMY_ROLE) {
                    // Dependencies not resolved; cannot make progress.
                    missed = xmlRole;
                    continue;
                }
                rolesByName.put(xmlRole.name, role);
            }
            if (missed == null) {
                break;
            }
            if (rolesByName.size() == prevMapSize) {
                handler.error(
                    "Role '" + missed.name + "' has cyclic dependencies on "
                    + "other roles",
                    missed,
                    null);
                break;
            }
        }

        RolapSchema.RoleFactory defaultRole = null;
        if (xmlSchema.defaultRole != null) {
            defaultRole = schema.mapNameToRole.get(xmlSchema.defaultRole);
            if (defaultRole == null) {
                handler.warning(
                    "Role '" + xmlSchema.defaultRole + "' not found",
                    xmlSchema,
                    "defaultRole");
            }
        }
        if (defaultRole == null) {
            defaultRole = schema.getDefaultRole();
        }
        schema.registerRoles(rolesByName, defaultRole);

        // Set default role.

        schema.aggTableManager.initialize();
        schema.setSchemaLoadDate();
    }

    private void loadResources(
        String catalogDirUrl,
        MondrianDef.Schema xmlSchema,
        Set<Locale> locales)
    {
        final MondrianDef.Localization localization =
            xmlSchema.getLocalization();
        if (localization != null) {
            for (MondrianDef.Locale locale : localization.getLocales()) {
                locales.add(Util.parseLocale(locale.locale));
            }
            // All into one big hash table.
            for (MondrianDef.Translation t : localization.getTranslations()) {
                for (Locale locale : locales) {
                    String path =
                        t.path.replace("${locale}", locale.toString());
                    if (isRelative(path)) {
                        path = catalogDirUrl + path;
                    }
                    try {
                        final InputStream inputStream =
                            Util.readVirtualFile(path);
                        final Properties properties = new Properties();
                        properties.load(inputStream);
                        for (Map.Entry entry : properties.entrySet()) {
                            String key = (String) entry.getKey();
                            LocalizedProperty prop;
                            if (key.endsWith(".caption")) {
                                key = key.substring(
                                    0, key.length() - ".caption".length());
                                prop = LocalizedProperty.CAPTION;
                            } else if (key.endsWith(".description")) {
                                key = key.substring(
                                    0, key.length() - ".description".length());
                                prop = LocalizedProperty.DESCRIPTION;
                            } else {
                                continue;
                            }
                            Util.putMulti(
                                resourceMap,
                                key,
                                new Larders.Resource(
                                    prop,
                                    locale,
                                    (String) entry.getValue()));
                            if (key.endsWith(".member")) {
                                // Given
                                //   [Sales].[Time].[Weekly].[1997].[23].member
                                // find the hierarchy
                                //   [Sales].[Time].[Weekly]
                                int count = 0;
                                String hierarchyTag = null;
                                for (int i = 0; i < key.length(); i++) {
                                    if (key.charAt(i) == '.') {
                                        if (++count == 3) {
                                            hierarchyTag = key.substring(0, i);
                                        }
                                    }
                                }
                                // Record level depth. ('All' = 0.)
                                Util.putMulti(
                                    resourceHierarchyTags, hierarchyTag,
                                    count - 3);
                            }
                        }
                        inputStream.close();
                    } catch (Throwable e) {
                        // TODO: should be 'catch IOException' but VFS handler
                        // throws Error (it shouldn't)
                        handler.warning(
                            "Error reading resource file '" + path
                            + "'; ignoring",
                            t, "path", e);
                    }
                }
            }
        }
    }

    /** Returns the directory part of a path. Ends with '/' or '\'. */
    private static String dirName(String catalogUrl) {
        if (catalogUrl == null) {
            return null;
        }
        int q = catalogUrl.indexOf('?');
        if (q >= 0) {
            catalogUrl = catalogUrl.substring(0, q);
        }
        final int lastForwardSlash = catalogUrl.lastIndexOf('/');
        final int lastBackSlash = catalogUrl.lastIndexOf('\\');
        final int lastSlash = Math.max(lastForwardSlash, lastBackSlash);
        if (lastSlash >= 0) {
            catalogUrl = catalogUrl.substring(0, lastSlash + 1);
        }
        return catalogUrl;
    }

    private static boolean isRelative(String path) {
        final Matcher matcher = PROTOCOL_PATTERN.matcher(path);
        if (matcher.matches()) {
            path = matcher.replaceAll("");
        }
        return !path.startsWith("/");
    }

    private void checkSchemaVersion(final DOMWrapper schemaDom) {
        String schemaVersion = schemaDom.getAttribute("metamodelVersion");
        if (schemaVersion == null) {
            if (hasMondrian4Elements(schemaDom)) {
                throw Util.newError(
                    "Model in 4.x or later format must have"
                    + "'metamodelVersion' attribute.");
            } else {
                schemaVersion = "3.x";
            }
        }

        String[] versionParts = schemaVersion.split("\\.");
        final String schemaMajor =
            versionParts.length > 0 ? versionParts[0] : "";

        MondrianServer.MondrianVersion mondrianVersion =
            MondrianServer.forId(null).getVersion();
        final String serverMajor =
            mondrianVersion.getMajorVersion() + ""; // probably "4"

        if (serverMajor.compareTo(schemaMajor) < 0) {
            String errorMsg =
                "Schema version '" + schemaVersion
                + "' is later than schema version '"
                + serverMajor
                + ".x' supported by this version of Mondrian";
            throw Util.newError(errorMsg);
        }
    }

    private boolean hasMondrian4Elements(final DOMWrapper schemaDom) {
        // check for Mondrian 4 schema elements:
        for (DOMWrapper child : schemaDom.getChildren()) {
            if ("PhysicalSchema".equals(child.getTagName())) {
                // Schema/PhysicalSchema
                return true;
            } else if ("Cube".equals(child.getTagName())) {
                for (DOMWrapper grandchild : child.getChildren()) {
                    if ("MeasureGroups".equals(grandchild.getTagName())) {
                        // Schema/Cube/MeasureGroups
                        return true;
                    }
                }
            }
        }
        // otherwise assume version 3.x
        return false;
    }

    private static Scripts.ScriptDefinition toScriptDef(
        MondrianDef.Script script)
    {
        if (script == null) {
            return null;
        }
        final Scripts.ScriptLanguage language =
            Scripts.ScriptLanguage.lookup(script.language);
        if (language == null) {
            throw Util.newError(
                "Invalid script language '" + script.language + "'");
        }
        return new Scripts.ScriptDefinition(script.cdata, language);
    }

    private RolapSchema.PhysSchema validatePhysicalSchema(
        MondrianDef.PhysicalSchema xmlPhysicalSchema,
        RolapSchemaLoader.Handler handler,
        final Dialect dialect,
        final RolapSchema schema)
    {
        if (xmlPhysicalSchema == null) {
            handler.error("Physical schema required", null, null);
            return null;
        }
        final RolapSchema.PhysSchema physSchema =
            createSyntheticPhysicalSchema();

        // Create a table with 0 rows and 1 column. It is the basis for hanger
        // dimensions.
        final RolapSchema.PhysInlineTable empty =
            new RolapSchema.PhysInlineTable(
                physSchema, EMPTY_TABLE_NAME);
        final RolapSchema.PhysRealColumn c =
            new RolapSchema.PhysRealColumn(
                empty, EMPTY_TABLE_SOLE_COLUMN_NAME, Dialect.Datatype.Integer,
                null, -1);
        empty.columnsByName.put(c.name, c);
        physSchema.tablesByName.put(empty.alias, empty);

        this.physSchemaBuilder = new PhysSchemaBuilder(null, physSchema);

        final Set<ElementDef> skip = new HashSet<ElementDef>();

        // First pass through elements, creating tables, ensuring that
        // table names are unique.
        // Register columns explicitly defined and also load columns from JDBC.
        // Collect calculated columns, to be resolved later.
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList =
            new ArrayList<RolapSchema.UnresolvedColumn>();
        for (MondrianDef.Relation relation : Util.filter(
            xmlPhysicalSchema.children, MondrianDef.Relation.class))
        {
            final String alias = relation.getAlias();
            final RolapSchema.PhysRelationImpl physTable;
            if (relation instanceof MondrianDef.Table) {
                physTable =
                    registerTable(
                        handler,
                        dialect,
                        physSchema,
                        skip,
                        unresolvedColumnList,
                        alias,
                        (MondrianDef.Table) relation);
            } else if (relation instanceof MondrianDef.InlineTable) {
                MondrianDef.InlineTable inlineTable =
                    (MondrianDef.InlineTable) relation;
                physTable =
                    registerInlineTable(
                        handler,
                        dialect,
                        schema,
                        physSchema,
                        skip,
                        unresolvedColumnList,
                        alias,
                        inlineTable);
            } else if (relation instanceof MondrianDef.Query) {
                MondrianDef.Query inlineTable =
                    (MondrianDef.Query) relation;
                physTable =
                    registerView(
                        handler,
                        dialect,
                        schema,
                        physSchema,
                        skip,
                        unresolvedColumnList,
                        alias,
                        inlineTable);
            } else {
                handler.warning(
                    "Invalid element '" + relation.getName()
                    + "' in physical schema",
                    relation,
                    null);
                continue;
            }

            // If the table's alias is not unique, let the previous alias
            // stand; this table will be thrown away, but this check is after
            // the column and key validation so we can produce as many warnings
            // as possible.
            if (physSchema.tablesByName.containsKey(alias)) {
                handler.warning(
                    "Duplicate table alias '" + alias + "'.",
                    relation,
                    null);
            } else {
                physSchema.tablesByName.put(alias, physTable);
            }
        }

        // Second pass, resolve links.
        List<UnresolvedLink> unresolvedLinkList =
            new ArrayList<UnresolvedLink>();
        for (MondrianDef.Link xmlLink
            : Util.filter(xmlPhysicalSchema.children, MondrianDef.Link.class))
        {
            final List<RolapSchema.PhysColumn> columnList;
            int errorCount = 0;
            final RolapSchema.PhysRelation sourceTable =
                physSchema.tablesByName.get(xmlLink.source);
            String keyName = first(xmlLink.key, "primary");
            RolapSchema.PhysKey sourceKey = null;
            if (sourceTable == null) {
                handler.warning(
                    "Link references unknown source table '" + xmlLink.source
                    + "'.",
                    xmlLink,
                    "source");
                ++errorCount;
            } else if ((sourceKey = sourceTable.lookupKey(keyName)) == null) {
                handler.warning(
                    "Source table '" + xmlLink.source
                    + "' of link has no key named '" + keyName + "'.",
                    xmlLink,
                    "source");
                ++errorCount;
            }
            final RolapSchema.PhysRelation targetRelation =
                physSchema.tablesByName.get(xmlLink.target);
            if (targetRelation == null) {
                handler.warning(
                    "Link references unknown target table '"
                    + xmlLink.target + "'.",
                    xmlLink,
                    "target");
                ++errorCount;
                columnList = null;
            } else {
                columnList =
                    createColumnList(
                        xmlLink,
                        "foreignKeyColumn",
                        targetRelation,
                        xmlLink.foreignKeyColumn,
                        xmlLink.foreignKey);
            }
            if (errorCount == 0) {
                unresolvedLinkList.add(
                    new UnresolvedLink(sourceKey, targetRelation, columnList));
            }
        }

        // Third pass, validate calculated columns. Note that a calculated
        // column can reference columns in other tables. Forward references
        // are allowed, but references must not be cyclic.
        for (RolapSchema.UnresolvedColumn unresolvedColumn
            : unresolvedColumnList)
        {
            if (unresolvedColumn.state
                == RolapSchema.UnresolvedColumn.State.RESOLVED)
            {
                continue;
            }
            resolve(physSchema, unresolvedColumn);
        }

        // Add links to the schema now their columns have been resolved.
        for (UnresolvedLink unresolvedLink : unresolvedLinkList) {
            physSchema.addLink(
                unresolvedLink.sourceKey,
                unresolvedLink.targetRelation,
                unresolvedLink.columnList,
                false);
        }

        // Validate keys.
        for (RolapSchema.PhysRelation table : physSchema.tablesByName.values())
        {
            for (RolapSchema.PhysKey key : table.getKeyList()) {
                for (RolapSchema.PhysExpr keyColumn : key.columnList) {
                    if (keyColumn instanceof RolapSchema.PhysCalcColumn) {
                        RolapSchema.PhysCalcColumn physCalcColumn =
                            (RolapSchema.PhysCalcColumn) keyColumn;
                        handler.warning(
                            "Key must not contain calculated column; calculated"
                            + " column '" + physCalcColumn.name
                            + "' in table '"
                            + physCalcColumn.relation.getAlias() + "'.",
                            validator.getXml(table),
                            null);
                    } else if (keyColumn
                        instanceof RolapSchema.UnresolvedColumn)
                    {
                        // have already reported that it is unresolved: continue
                    } else {
                        RolapSchema.PhysRealColumn column =
                            (RolapSchema.PhysRealColumn) keyColumn;
                        if (column.relation != table) {
                            handler.warning(
                                "Columns in primary key must belong to key "
                                + "table; in table '" + table.getAlias() + "'.",
                                validator.getXml(table),
                                null);
                        }
                    }
                }
            }
        }

        return physSchema;
    }

    void resolve(
        RolapSchema.PhysSchema physSchema,
        UnresolvedColumn unresolvedColumn)
    {
        try {
            if (unresolvedColumn.state == UnresolvedColumn.State.ACTIVE) {
                getHandler().warning(
                    "Calculated column '" + unresolvedColumn.name
                    + "' in table '" + unresolvedColumn.tableName
                    + "' has cyclic expression", unresolvedColumn.xml, null);
                return;
            }
            unresolvedColumn.state = UnresolvedColumn.State.ACTIVE;
            final RolapSchema.PhysRelation table =
                physSchema.tablesByName.get(unresolvedColumn.tableName);
            if (table == null) {
                getHandler().warning(
                    "Unknown table '" + unresolvedColumn.tableName + "'"
                    + unresolvedColumn.getContext() + ".",
                    unresolvedColumn.xml,
                    null);
                return;
            }
            final RolapSchema.PhysColumn column =
                table.getColumn(
                    unresolvedColumn.name, false);
            if (column == null) {
                getHandler().warning(
                    "Reference to unknown column '" + unresolvedColumn.name
                    + "' in table '" + unresolvedColumn.tableName + "'"
                    + unresolvedColumn.getContext() + ".",
                    unresolvedColumn.xml,
                    null);
            } else {
                if (column instanceof RolapSchema.PhysCalcColumn) {
                    RolapSchema.PhysCalcColumn physCalcColumn =
                        (RolapSchema.PhysCalcColumn) column;
                    for (Object o : physCalcColumn.list) {
                        if (o instanceof UnresolvedColumn) {
                            resolve(
                                physSchema,
                                (UnresolvedColumn) o);
                        }
                    }
                }
                unresolvedColumn.state = UnresolvedColumn.State.RESOLVED;
                unresolvedColumn.onResolve(column);
            }
        } finally {
            if (unresolvedColumn.state == UnresolvedColumn.State.ACTIVE) {
                unresolvedColumn.state =
                    RolapSchema.UnresolvedColumn.State.ERROR;
            }
        }
    }

    private RolapSchema.PhysInlineTable registerInlineTable(
        Handler handler,
        Dialect dialect,
        RolapSchema schema,
        RolapSchema.PhysSchema physSchema,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        MondrianDef.InlineTable xmlInlineTable)
    {
        RolapSchema.PhysInlineTable physInlineTable =
            new RolapSchema.PhysInlineTable(
                physSchema,
                alias);
        validator.putXml(physInlineTable, xmlInlineTable);

        for (MondrianDef.RealOrCalcColumnDef column
            : xmlInlineTable.getColumnDefs())
        {
            registerColumn(
                handler,
                dialect,
                skip,
                unresolvedColumnList,
                alias,
                physInlineTable,
                xmlInlineTable,
                column);
        }
        registerKey(
            handler, xmlInlineTable, null, xmlInlineTable.getKey(),
            unresolvedColumnList, physInlineTable);
        for (MondrianDef.Row row : xmlInlineTable.getRows()) {
            assert row.values.length == xmlInlineTable.getColumnDefs().size();
            List<String> newRow = new ArrayList<String>();
            for (int i = 0; i < row.values.length; i++) {
                RealOrCalcColumnDef colDef =
                    xmlInlineTable.getColumnDefs().get(i);
                assert colDef.name.equals(row.values[i].column);
                newRow.add(row.values[i].cdata);
            }
            physInlineTable.rowList.add(
                newRow.toArray(new String[newRow.size()]));
        }
        return physInlineTable;
    }

    private RolapSchema.PhysView registerView(
        Handler handler,
        Dialect dialect,
        RolapSchema schema,
        RolapSchema.PhysSchema physSchema,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        MondrianDef.Query xmlQuery)
    {
        final MondrianDef.SQL xmlSql;
        try {
            xmlSql = MondrianDef.SQL.choose(
                xmlQuery.getExpressionView().expressions,
                dialect);
        } catch (MondrianDef.Missing missing) {
            // No ExpressionView element. Error has already been posted.
            return null;
        }
        RolapSchema.PhysView physView =
            new RolapSchema.PhysView(
                physSchema,
                alias,
                getText(xmlSql));
        validator.putXml(physView, xmlQuery);

        // Read columns from JDBC.
        physView.ensurePopulated(this, xmlQuery);

        registerKey(
            handler, xmlQuery, xmlQuery.keyColumn, null, unresolvedColumnList,
            physView);
        for (MondrianDef.Key key : xmlQuery.getKeys()) {
            registerKey(
                handler, xmlQuery, null, key, unresolvedColumnList,
                physView);
        }
        return physView;
    }

    private RolapSchema.PhysTable registerTable(
        Handler handler,
        Dialect dialect,
        RolapSchema.PhysSchema physSchema,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        MondrianDef.Table xmlTable)
    {
        final RolapSchema.PhysTable physTable =
            new RolapSchema.PhysTable(
                physSchema,
                xmlTable.schema,
                xmlTable.name,
                alias,
                buildHintMap(xmlTable.getHints()));
        validator.putXml(physTable, xmlTable);

        if (xmlTable instanceof MondrianDef.AutoGeneratedDateTable) {
            registerAutoDateTable(handler, xmlTable, physTable);
            physTable.ensurePopulated(this, xmlTable);
        } else {
            // Read columns from JDBC.
            physTable.ensurePopulated(this, xmlTable);
            // First pass, register columns. We will resolve and
            // register keys later.
            for (MondrianDef.RealOrCalcColumnDef xmlColumn
                : xmlTable.getColumnDefs())
            {
                registerColumn(
                    handler,
                    dialect,
                    skip,
                    unresolvedColumnList,
                    alias,
                    physTable,
                    xmlTable,
                    xmlColumn);
            }
        }

        registerKey(
            handler, xmlTable, xmlTable.keyColumn, null, unresolvedColumnList,
            physTable);
        for (MondrianDef.Key key : xmlTable.getKeys()) {
            registerKey(
                handler, xmlTable, null, key, unresolvedColumnList, physTable);
        }
        return physTable;
    }

    private void registerAutoDateTable(
        Handler handler,
        MondrianDef.Table xmlTable,
        RolapSchema.PhysTable physTable)
    {
        MondrianDef.AutoGeneratedDateTable xmlDateTable =
            (MondrianDef.AutoGeneratedDateTable) xmlTable;
        final Map<String, TimeColumnRole.Struct> columnRoleMap =
            new HashMap<String, TimeColumnRole.Struct>();
        List<MondrianDef.RealOrCalcColumnDef> xmlColumnDefs;
        if (hasColumnDefs(xmlTable.childArray)) {
            xmlColumnDefs = xmlTable.getColumnDefs();
        } else {
            xmlColumnDefs =
                new ArrayList<MondrianDef.RealOrCalcColumnDef>();
            for (TimeColumnRole role : TimeColumnRole.values()) {
                MondrianDef.ColumnDef xmlColumnDef =
                    new MondrianDef.ColumnDef();
                xmlColumnDef.name = role.columnName;
                xmlColumnDef.type = role.defaultDatatype.name();
                xmlColumnDef.timeDomain = new MondrianDef.TimeDomain();
                xmlColumnDef.timeDomain.role = role.name();
                xmlColumnDefs.add(xmlColumnDef);
            }
        }
        for (MondrianDef.RealOrCalcColumnDef xmlColumnDef : xmlColumnDefs) {
            MondrianDef.TimeDomain domain =
                ((MondrianDef.ColumnDef) xmlColumnDef).timeDomain;
            TimeColumnRole.Struct struct;
            if (domain != null) {
                TimeColumnRole role =
                    Util.lookup(
                        TimeColumnRole.class, domain.role.toUpperCase());
                if (role == null) {
                    handler.error(
                        "Bad role '" + domain + "', in column '"
                        + xmlColumnDef.name + "'. Allowable roles are "
                        + Arrays.toString(TimeColumnRole.values()),
                        xmlColumnDef,
                        "role");
                    continue;
                }
                struct =
                    new TimeColumnRole.Struct(
                        role,
                        parseDate(domain.epoch, domain, "epoch"));
            } else {
                TimeColumnRole role =
                    TimeColumnRole.mapNameToRole.get(
                        xmlColumnDef.name.toUpperCase());
                if (role == null) {
                    handler.error(
                        "Each column in an auto-generated date table must "
                        + "either have a nested <TimeDomain> element, or "
                        + "have a standard name such as 'the_month'; "
                        + "column '" + xmlColumnDef.name + "' is neither",
                        xmlColumnDef,
                        null);
                    continue;
                }
                struct = new TimeColumnRole.Struct(role, null);
            }
            if (xmlColumnDef.type == null) {
                xmlColumnDef.type = struct.role.defaultDatatype.name();
            }
            columnRoleMap.put(xmlColumnDef.name, struct);
        }
        java.sql.Date startDate =
            parseDate(
                xmlDateTable.startDate,
                xmlDateTable,
                "startDate");
        java.sql.Date endDate =
            parseDate(
                xmlDateTable.endDate,
                xmlDateTable,
                "endDate");
        if (startDate == null || endDate == null) {
            // parseDate has posted an error. Soldier on...
            return;
        }
        physTable.setHook(
            new DateTableBuilder(
                columnRoleMap,
                xmlColumnDefs, startDate, endDate));
    }

    /**
     * Returns whether there is a &lt;ColumnDefs&gt; element among
     * {@code children}.
     *
     * @param children Array of child nodes
     * @return Whether there is a ColumnDefs
     */
    private boolean hasColumnDefs(MondrianDef.TableElement[] children) {
        for (MondrianDef.TableElement child : children) {
            if (child instanceof MondrianDef.ColumnDefs) {
                return true;
            }
        }
        return false;
    }

    /**
     * Parses a date in "yyyy-MM-dd" format (e.g. "2012-02-21").
     *
     * <p>If date is
     * not valid, posts an error and returns null.</p>
     *
     * <p>If date is null, returns null.</p>
     *
     * @param dateString Date string
     * @param node XML node
     * @param attributeName XML attribute
     * @return Parsed date, or null if date string is null or not valid
     */
    private java.sql.Date parseDate(
        String dateString,
        NodeDef node,
        String attributeName)
    {
        if (dateString == null) {
            return null;
        }
        try {
            Date date = SIMPLE_DATE_FORMAT.parse(dateString);
            return new java.sql.Date(date.getTime());
        } catch (ParseException e) {
            handler.error(
                "Invalid date '" + dateString + "'",
                node,
                attributeName);
            return null;
        }
    }

    private void registerKey(
        Handler handler,
        ElementDef xmlTable,
        String columnName,
        final MondrianDef.Key xmlKey,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        final RolapSchema.PhysRelationImpl physTable)
    {
        String keyName;
        List<Tcl> columns;
        if (xmlKey == null) {
            if (columnName == null) {
                // If both null, nothing to do.
                return;
            }
            columns = Collections.singletonList(
                new Tcl(null, columnName, xmlTable));
            keyName = "primary";
        } else {
            columns = new AbstractList<Tcl>() {
                public Tcl get(int index) {
                    MondrianDef.Column column = xmlKey.array[index];
                    return new Tcl(column.table, column.name, column);
                }

                public int size() {
                    return xmlKey.array.length;
                }
            };
            keyName = xmlKey.name;
            if (keyName == null) {
                keyName = "primary";
            }
        }
        if (physTable.lookupKey(keyName) != null) {
            handler.error(
                "Table has more than one key with name '" + keyName
                + "'",
                xmlKey,
                null);
            return;
        }
        final RolapSchema.PhysKey key =
            physTable.addKey(
                keyName, new ArrayList<RolapSchema.PhysColumn>());
        int i = 0;
        for (Tcl columnRef : columns) {
            final int index = i++;
            final RolapSchema.UnresolvedColumn unresolvedColumn =
                new RolapSchema.UnresolvedColumn(
                    physTable,
                    columnRef.table != null
                        ? columnRef.table
                        : physTable.alias,
                    columnRef.column,
                    columnRef.xml)
                {
                    public void onResolve(RolapSchema.PhysColumn column) {
                        assert column != null;
                        key.columnList.set(index, column);
                    }

                    public String getContext() {
                        return ", in key of table '"
                            + physTable.alias + "'";
                    }
                };
            key.columnList.add(unresolvedColumn);
            unresolvedColumnList.add(unresolvedColumn);
        }
        if (key.columnList.size() != 1) {
            handler.warning(
                "Key must have precisely one column; key "
                + key.columnList
                + " in table '"
                + physTable.alias + "'.",
                xmlKey,
                null);
        }
    }

    static class Tcl {
        private final String table;
        private final String column;
        private final ElementDef xml;

        Tcl(String table, String column, ElementDef xml) {
            this.table = table;
            this.column = column;
            this.xml = xml;
        }
    }

    private void registerColumn(
        Handler handler,
        Dialect dialect,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        RolapSchema.PhysRelationImpl physRelation,
        MondrianDef.Relation table,
        MondrianDef.RealOrCalcColumnDef xmlColumn)
    {
        boolean inlineTable =
            physRelation instanceof RolapSchema.PhysInlineTable;
        boolean calcColDef =
            xmlColumn instanceof MondrianDef.CalculatedColumnDef;
        if ((inlineTable || calcColDef)
            && physRelation.columnsByName.containsKey(xmlColumn.name))
        {
            // The calc or inlined column is a duplicate.
            handler.warning(
                "Duplicate column '" + xmlColumn.name
                + "' in table '" + alias + "'.",
                xmlColumn,
                null);
            skip.add(xmlColumn);
            return;
        }
        if (inlineTable) {
            RolapSchema.PhysInlineTable physInlineTable =
                (RolapSchema.PhysInlineTable) physRelation;
            if (calcColDef) {
                handler.warning(
                    "Cannot define calculated column in inline table",
                    xmlColumn,
                    null);
            } else {
                RolapSchema.PhysRealColumn physColumn =
                    new RolapSchema.PhysRealColumn(
                        physInlineTable,
                        xmlColumn.name,
                        toType(xmlColumn.type),
                        toInternalType(xmlColumn.internalType),
                        -1);
                validator.putXml(physColumn, xmlColumn);
                physInlineTable.addColumn(physColumn);
            }
            return;
        }
        final RolapSchema.PhysTable physTable =
            (RolapSchema.PhysTable) physRelation;
        if (calcColDef) {
            MondrianDef.CalculatedColumnDef xmlCalcColumnDef =
                (MondrianDef.CalculatedColumnDef) xmlColumn;
            final List<RolapSchema.PhysExpr> list =
                new ArrayList<RolapSchema.PhysExpr>();
            final RolapSchema.PhysCalcColumn physCalcColumn =
                new RolapSchema.PhysCalcColumn(
                    this,
                    xmlCalcColumnDef,
                    physTable,
                    xmlColumn.name,
                    toType(xmlColumn.type),
                    toInternalType(xmlColumn.internalType),
                    list);
            validator.putXml(physCalcColumn, xmlColumn);

            final MondrianDef.SQL sql;
            assert xmlCalcColumnDef.expression != null;
            if (xmlCalcColumnDef.expression
                instanceof MondrianDef.ExpressionView)
            {
                MondrianDef.ExpressionView expressionView =
                    (MondrianDef.ExpressionView)
                        xmlCalcColumnDef.expression;
                sql = MondrianDef.SQL.choose(
                    expressionView.expressions,
                    dialect);
            } else {
                // Create dummy wrapper so it looks as if the
                // column is part of a collection of SQL
                // choices.
                sql = new MondrianDef.SQL();
                sql.children =
                    new NodeDef[] {
                        (MondrianDef.Column)
                            xmlCalcColumnDef.expression
                    };
            }
            for (int i = 0; i < sql.children.length; i++) {
                NodeDef child = sql.children[i];
                if (child instanceof TextDef) {
                    TextDef text = (TextDef) child;
                    String s = text.getText();
                    s = trim(s, i == 0, i == sql.children.length - 1);
                    list.add(new RolapSchema.PhysTextExpr(s));
                } else if (child instanceof MondrianDef.Column) {
                    final int index = list.size();
                    final MondrianDef.Column columnRef =
                        (MondrianDef.Column) child;
                    final RolapSchema.UnresolvedColumn unresolvedColumn =
                        new RolapSchema.UnresolvedCalcColumn(
                            physTable,
                            first(columnRef.table, alias),
                            columnRef,
                            sql,
                            physCalcColumn,
                            list,
                            index);
                    list.add(unresolvedColumn);
                    unresolvedColumnList.add(unresolvedColumn);
                } else {
                    throw new IllegalArgumentException(
                        "illegal expression: " + child);
                }
            }
            physCalcColumn.compute();
            physTable.addColumn(physCalcColumn);
        } else {
            // Check that column exists; throw if not.
            RolapSchema.PhysColumn physColumn =
                physRelation.getColumn(
                    xmlColumn.name,
                    true);
            if (xmlColumn.type != null) {
                physColumn.setDatatype(
                    toType(xmlColumn.type));
            }
            if (xmlColumn.internalType != null) {
                physColumn.setInternalType(
                    toInternalType(xmlColumn.internalType));
            }
        }
    }

    /**
     * Trims whitespace from the left and right ends of a string.
     *
     * @param s String
     * @param left Whether to trim the start of the string
     * @param right Whether to trim the end of the string
     * @return Trimmed string
     */
    private static String trim(String s, boolean left, boolean right) {
        while (left
               && s.length() > 0 && Character.isWhitespace(s.charAt(0)))
        {
            s = s.substring(1);
        }
        while (right
               && s.length() > 0
               && Character.isWhitespace(s.charAt(s.length() - 1)))
        {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    static Dialect.Datatype toType(String type) {
        return DATATYPE_MAP.get(type);
    }

    /**
     * Creates a physical schema for a user schema that has no PhysicalSchema
     * element. (Such a schema is referred to elsewhere as an
     * 'old-style schema'.)
     *
     * @return Physical schema
     */
    private RolapSchema.PhysSchema createSyntheticPhysicalSchema() {
        DataServicesProvider provider =
            DataServicesLocator.getDataServicesProvider(
                schema.getDataServiceProviderName());
        return new RolapSchema.PhysSchema(
            schema.getDialect(),
            schema.getInternalConnection(), provider);
    }

    /**
     * Creates a cube.
     */
    private RolapCube createCube(
        RolapSchema schema,
        MondrianDef.Cube xmlCube,
        MondrianDef.Schema xmlSchema)
    {
        if (!toBoolean(xmlCube.enabled, true)) {
            return null;
        }
        if (schema.getCubeList().get(xmlCube.name) != null) {
            getHandler().error(
                "Duplicate cube '" + xmlCube.name + "'",
                xmlCube,
                null);
            return null;
        }
        measureList.clear();
        aggFactCountMeasureList.clear();
        assignDefaultMembers.clear();
        cubeHierMap.clear();

        RolapCube cube =
            new RolapCube(
                this,
                xmlCube.name,
                toBoolean(xmlCube.visible, true),
                createLarder(
                    Util.quoteMdxIdentifier(xmlCube.name) + ".cube",
                    xmlCube.getAnnotations(),
                    xmlCube.name,
                    xmlCube.caption,
                    xmlCube.description).build(),
                xmlSchema.measuresCaption);
        validator.putXml(cube, xmlCube);
        deferAssignDefaultMember(
            cube,
            cube.getMeasuresHierarchy(),
            xmlCube,
            xmlCube.defaultMeasure);

        dimensionPaths.clear();

        final NamedList<MondrianDef.Dimension> xmlCubeDimensions =
            xmlCube.getDimensions();

        final List<MondrianDef.Dimension> xmlCubeDimensionsPlus;
        if (xmlCube.enableScenarios) {
            final MondrianDef.Dimension xmlDimension =
                new MondrianDef.Dimension();
            xmlDimension.name = "Scenario";
            xmlDimension.hanger = true;
            xmlDimension.key = "Scenario";
            xmlDimension.type =
                org.olap4j.metadata.Dimension.Type.SCENARIO.name();
            final MondrianDef.Attributes xmlAttributes =
                new MondrianDef.Attributes();
            xmlDimension.children.add(xmlAttributes);
            final MondrianDef.Attribute xmlAttribute =
                new MondrianDef.Attribute();
            xmlAttributes.array = new MondrianDef.Attribute[] {xmlAttribute};
            xmlAttribute.name = "Scenario";
            xmlAttribute.levelType =
                org.olap4j.metadata.Level.Type.SCENARIO.name();

            //noinspection unchecked
            xmlCubeDimensionsPlus =
                Composite.of(
                    xmlCubeDimensions,
                    Collections.singletonList(xmlDimension));
        } else {
            xmlCubeDimensionsPlus = xmlCubeDimensions;
        }

        int ordinal = cube.getDimensionList().size();
        for (MondrianDef.Dimension xmlCubeDimension : xmlCubeDimensionsPlus) {
            // Look up usages of shared dimensions in the schema before
            // consulting the XML schema (which may be null).
            RolapCubeDimension dimension =
                getOrCreateDimension(
                    cube,
                    xmlCubeDimension,
                    schema,
                    xmlSchema,
                    ordinal++,
                    cube.hierarchyList,
                    xmlCubeDimension.getAnnotations());
            if (dimension == null) {
                continue;
            }

            cube.addDimension(dimension);
        }

        final NamedList<MondrianDef.MeasureGroup> xmlMeasureGroups =
            xmlCube.getMeasureGroups();
        if (xmlMeasureGroups.size() == 0 && !cube.getName().startsWith("$")) {
            handler.warning(
                "Cube definition must contain a MeasureGroups element, and at "
                + "least one MeasureGroup",
                xmlCube,
                null);
        }

        final Set<String> measureNames = new HashSet<String>();
        final Set<String> measureGroupNames = new HashSet<String>();
        final List<Util.Function0<RolapMeasureGroup.RolapMeasureRef>>
            unresolvedMeasures =
            new ArrayList<Util.Function0<RolapMeasureGroup.RolapMeasureRef>>();
        for (MondrianDef.MeasureGroup xmlMeasureGroup : xmlMeasureGroups) {
            // The name of the measure group defaults to the table alias.
            final String name = xmlMeasureGroup.getNameAttribute();
            if (!measureGroupNames.add(name)) {
                handler.warning(
                    "Duplicate measure group '" + name
                    + "' in cube '" + cube.getName() + "'",
                    xmlMeasureGroup,
                    "name");
            }

            final RolapSchema.PhysRelation fact =
                this.physSchemaBuilder.getPhysRelation(
                    xmlMeasureGroup.table, false);
            if (fact == null) {
                handler.warning(
                    "Unknown fact table '" + xmlMeasureGroup.table + "'",
                    xmlMeasureGroup,
                    "table");
                continue;
            }
            final RolapStar star =
                schema.getRolapStarRegistry().getOrCreateStar(fact);
            final RolapMeasureGroup measureGroup =
                new RolapMeasureGroup(
                    cube,
                    name,
                    toBoolean(xmlMeasureGroup.ignoreUnrelatedDimensions, false),
                    star,
                    xmlMeasureGroup.isAggregate());
            validator.putXml(measureGroup, xmlMeasureGroup);
            cube.addMeasureGroup(measureGroup);

            for (MondrianDef.MeasureOrRef xmlMeasureOrRef
                : xmlMeasureGroup.getMeasures())
            {
                if (xmlMeasureOrRef instanceof MondrianDef.MeasureRef) {
                    if (xmlMeasureGroup.isAggregate()) {
                        final MondrianDef.MeasureRef xmlMeasureRef =
                            (MondrianDef.MeasureRef) xmlMeasureOrRef;
                        unresolvedMeasures.add(
                            new Util.Function0<
                                RolapMeasureGroup.RolapMeasureRef>()
                            {
                                public RolapMeasureGroup.RolapMeasureRef apply()
                                {
                                    return createMeasureRef(
                                        measureList,
                                        xmlMeasureRef,
                                        measureGroup);
                                }
                            }
                        );
                    } else {
                        handler.warning(
                            "MeasureRef must not occur in a fact MeasureGroup",
                            xmlMeasureOrRef,
                            null);
                    }
                    continue;
                }
                MondrianDef.Measure xmlMeasure =
                    (MondrianDef.Measure) xmlMeasureOrRef;
                RolapSchema.PhysRelation relation =
                    xmlMeasureGroup.table == null
                        ? null
                        : getPhysRelation(
                            xmlMeasureGroup.table, xmlMeasureGroup, "table");

                if (!measureNames.add(xmlMeasure.name)) {
                    getHandler().error(
                        "Duplicate measure '" + xmlMeasure.name + "' in cube '"
                        + measureGroup.getCube().getName() + "'",
                        xmlMeasure,
                        null);
                    continue;
                }

                final RolapBaseCubeMeasure measure =
                    createMeasure(
                        measureGroup, xmlMeasure, relation);
                if (measure == null) {
                    continue;
                }
                if (measure.getOrdinal() == -1) {
                    measure.setOrdinal(nonSystemMeasures().size());
                    measure.setOrderKey(nonSystemMeasures().size());
                }
                if (measure.getAggregator() == RolapAggregator.Count) {
                    measureGroup.factCountMeasure = measure;
                }
                if (measure.getExpr() != null) {
                    // Only set if different from default (so that if two cubes
                    // share the same fact table, either can turn off caching
                    // and both are affected).
                    if (!toBoolean(xmlCube.cache, true)) {
                        star.setCacheAggregations(false);
                    }
                }

                measureList.add(measure);
            }

            // Ensure that the measure group has an atomic cell count measure
            // even if the schema does not contain one.
            if (measureGroup.factCountMeasure == null) {
                final MondrianDef.Measure xmlMeasure =
                    new MondrianDef.Measure();
                xmlMeasure.aggregator = RolapAggregator.Count.name;
                xmlMeasure.name = FACT_COUNT_MEASURE_NAME;
                xmlMeasure.visible = Boolean.FALSE;
                RolapSchema.PhysRelation relation =
                    xmlMeasureGroup.table == null
                        ? null
                        : getPhysRelation(
                            xmlMeasureGroup.table, xmlMeasureGroup, "table");
                measureGroup.factCountMeasure =
                    createMeasure(measureGroup, xmlMeasure, relation);
                if (measureGroup.factCountMeasure == null) {
                    throw new AssertionError();
                }
                measureList.add(measureGroup.factCountMeasure);
                aggFactCountMeasureList.add(measureGroup.factCountMeasure);
            }

            final Set<RolapCubeDimension> unlinkedDimensions =
                new HashSet<RolapCubeDimension>(
                    cube.getDimensionList());
            unlinkedDimensions.remove(cube.getDimensionList().get(0));
            for (MondrianDef.DimensionLink xmlDimensionLink
                : xmlMeasureGroup.getDimensionLinks())
            {
                final RolapCubeDimension dimension =
                    cube.dimensionList.get(xmlDimensionLink.dimension);
                if (dimension == null) {
                    final MondrianDef.Dimension xmlSchemaDimension =
                        xmlSchema.getDimensions().get(
                            xmlDimensionLink.dimension);
                    if (xmlSchemaDimension != null) {
                        handler.warning(
                            "Dimension '" + xmlDimensionLink.dimension
                            + "' not found in this cube, but there is a schema "
                            + "dimension of that name. Did you intend to "
                            + "create cube dimension referring to that schema "
                            + "dimension?", xmlDimensionLink, "dimension");
                    } else {
                        handler.warning(
                            "Dimension '" + xmlDimensionLink.dimension
                            + "' not found",
                            xmlDimensionLink,
                            "dimension");
                    }
                    continue;
                }
                if (!unlinkedDimensions.remove(dimension)) {
                    handler.warning(
                        "More than one link for dimension '"
                        + xmlDimensionLink.dimension
                        + "' in measure group '"
                        + name
                        + "'",
                        xmlDimensionLink,
                        "dimension");
                }
                if (xmlDimensionLink
                    instanceof MondrianDef.ForeignKeyLink)
                {
                    addForeignKeyLink(
                        fact,
                        measureGroup,
                        dimension,
                        (MondrianDef.ForeignKeyLink) xmlDimensionLink);
                    RolapCubeDimension closedDimension =
                        findClosedPeerDimension(dimension);
                    if (closedDimension != null) {
                        addForeignKeyLink(
                            fact,
                            measureGroup,
                            closedDimension,
                            (MondrianDef.ForeignKeyLink) xmlDimensionLink);
                    }
                } else if (xmlDimensionLink
                    instanceof MondrianDef.FactLink)
                {
                    addFactLink(
                        measureGroup,
                        dimension);
                } else if (xmlDimensionLink
                    instanceof MondrianDef.ReferenceLink)
                {
                    // TODO: implement
                } else if (xmlDimensionLink
                    instanceof MondrianDef.CopyLink)
                {
                    addCopyLink(
                        measureGroup,
                        dimension,
                        (MondrianDef.CopyLink) xmlDimensionLink);
                } else if (xmlDimensionLink
                    instanceof MondrianDef.NoLink)
                {
                    // safe to ignore dimension
                } else {
                    throw Util.newInternal(
                        "Unknown link type " + xmlDimensionLink);
                }
            }

            for (RolapCubeDimension dimension : unlinkedDimensions) {
                // Hanger dimensions are not linked.
                if (dimension.hanger) {
                    continue;
                }
                missingLinkAction.handle(
                    handler,
                    "No link for dimension '" + dimension.getName()
                    + "' in measure group '" + measureGroup.getName() + "'",
                    xmlMeasureGroup,
                    null);
            }
        }

        // Now we've seen all measures, process MeasureRefs.
        for (Util.Function0 unresolvedMeasure : unresolvedMeasures) {
            unresolvedMeasure.apply();
        }

        // A cube must include at least one measure. The [Fact Count] measures
        // that are created for measure groups do not count; they are not
        // visible.

        cube.init(nonSystemMeasures());

        final List<AssignDefaultMember> assignDefaultMembers2 =
            new ArrayList<AssignDefaultMember>(assignDefaultMembers);
        assignDefaultMembers.clear();
        for (AssignDefaultMember assign : assignDefaultMembers2) {
            AssignDefaultMember remaining = assign.apply();
            if (remaining != null) {
                assignDefaultMembers.add(remaining);
            }
        }

        schema.addCube(cube);

//        // Check that every stored measure belongs to its measure group.
//        List<RolapCubeHierarchy.RolapCubeStoredMeasure> storedMeasures =
//            new ArrayList<RolapCubeHierarchy.RolapCubeStoredMeasure>();
//        for (Member measure : cube.getMeasures()) {
//            if (measure instanceof RolapStoredMeasure) {
//                RolapStoredMeasure storedMeasure =
//                    (RolapStoredMeasure) measure;
//                assert ((RolapStoredMeasure) measure).getMeasureGroup()
//                    .measureList
//                    .contains(storedMeasure);
//            }
//            if (measure instanceof RolapCubeHierarchy.RolapCubeStoredMeasure)
//            {
//                storedMeasures.add(
//                    (RolapCubeHierarchy.RolapCubeStoredMeasure) measure);
//            }
//        }
//
//        // Create measures (and stars for them, if necessary).
//        for (RolapCubeHierarchy.RolapCubeStoredMeasure storedMeasure
//            : storedMeasures)
//        {
//            Util.discard(storedMeasure.getMeasureGroup().getStar());
//            final RolapSchema.PhysRelation relation =
//                storedMeasure.getMeasureGroup().getFactRelation();
//            final RolapSchema.PhysExpr expr = storedMeasure.getExpr();
//            if (expr instanceof RolapSchema.PhysColumn) {
//                RolapSchema.PhysColumn column =
//                    (RolapSchema.PhysColumn) expr;
//                assert relation == column.relation;
//            }
//            RolapStar star =
//                schema.getRolapStarRegistry().getOrCreateStar(relation);
//            RolapStar.Table table = star.getFactTable();
//            table.makeMeasure(
//                (RolapBaseCubeMeasure) storedMeasure.getRolapMember());
//        }


        for (RolapMeasureGroup measureGroup : cube.getMeasureGroups()) {
            // Create a RolapStar.Measure for each measure in this group.
            for (RolapStoredMeasure measure : measureGroup.measureList) {
                final RolapSchema.PhysColumn expr = measure.getExpr();
                if (expr != null) {
                    assert measureGroup.getFactRelation() == expr.relation;
                }
                RolapStar star = measureGroup.getStar();
                RolapStar.Table table = star.getFactTable();
                table.makeMeasure((RolapBaseCubeMeasure) measure);
            }

            // Create a RolapStar.Measure for each measure-reference. It has
            // the same definition (agg function) as the referenced measure (in
            // the fact table), but it uses the column in the aggregate table.
            for (RolapMeasureGroup.RolapMeasureRef measureRef
                : measureGroup.measureRefList)
            {
                assert measureGroup.getFactRelation()
                       == measureRef.aggColumn.relation;

                RolapStar star = measureGroup.getStar();
                RolapStar.Table table = star.getFactTable();
                table.makeMeasure(
                    measureRef.measure, measureRef.aggColumn, true);
            }

            for (RolapCubeDimension dimension : cube.dimensionList) {
                if (measureGroup.existsLink(dimension)) {
                    registerDimension(
                        measureGroup,
                        dimension,
                        dimensionPaths.get(Pair.of(measureGroup, dimension)));
                }
            }
        }

        // Load calculated members and named sets.
        // (We cannot do this in the constructor,
        // because cannot parse the generated query,
        // because the schema has not been set in the cube at this point.)
        createCalcMembersAndNamedSets(
            xmlCube.getCalculatedMembers(),
            xmlCube.getNamedSets(),
            measureList,
            cube.calculatedMemberList,
            cube.namedSetList,
            cube,
            true);

        checkOrdinals(xmlCube);
        if (Util.deprecated(false, false)) {
            cube.setAggGroup(
                ExplicitRules.Group.make(
                    cube, (Mondrian3Def.Cube) (Object) xmlCube));
        }

        cube.init2();

        for (AssignDefaultMember assignDefaultMember : assignDefaultMembers) {
            assignDefaultMember.apply2();
        }

        // Check that all hierarchies now have a default member.
        for (RolapCubeHierarchy hierarchy : cube.hierarchyList) {
            assert hierarchy.defaultMember != null : hierarchy;
        }

        return cube;
    }

    private RolapCubeDimension findClosedPeerDimension(
        RolapCubeDimension dimension)
    {
        for (RolapCubeHierarchy hierarchy : dimension.getHierarchyList()) {
            for (RolapCubeLevel level : hierarchy.getLevelList()) {
                if (level.hasClosedPeer()) {
                    return level.getClosedPeer().getDimension();
                }
            }
        }
        return null;
    }

    Larders.LarderBuilder createLarder(
        String tag,
        NamedList<MondrianDef.Annotation> annotations,
        String name,
        String caption,
        String description)
    {
        return new Larders.LarderBuilder()
            .populate(
                Larders.create(
                    annotations,
                    name,
                    caption,
                    description,
                    tag != null ? resourceMap.get(tag) : null));
    }

    private List<RolapMember> nonSystemMeasures() {
        return minus(measureList, aggFactCountMeasureList);
    }

    private <T> List<T> minus(List<T> list0, Collection<T> remove) {
        final List<T> list = new ArrayList<T>();
        for (T t : list0) {
            if (!containsByIdentity(remove, t)) {
                list.add(t);
            }
        }
        return list;
    }

    private <T> boolean containsByIdentity(Collection<T> remove, T next) {
        boolean b = false;
        for (T o : remove) {
            if (next == o) {
                b = true;
                break;
            }
        }
        return b;
    }

    /**
     * Creates a reference to a measure from a measure group.
     *
     * <p>Usually a measure is defined at the finest granularity fact table.
     * But you can create it in the "aggregate table" and reference from the
     * "fact table", if you really want to.</p>
     *
     * <p>This function is called after all measure groups have been created.
     * This allows for forward references.</p>
     *
     * @param measureList List of measures in this cube (across all measure
     *                    groups, but not including calc measures)
     * @param xmlMeasureRef XML MeasureRef element
     * @param measureGroup Measure group
     * @return Created measure reference
     */
    private RolapMeasureGroup.RolapMeasureRef createMeasureRef(
        List<RolapMember> measureList,
        MondrianDef.MeasureRef xmlMeasureRef,
        RolapMeasureGroup measureGroup)
    {
        final RolapMember measure =
            findMeasure(measureList, xmlMeasureRef.name);
        if (measure == null) {
            handler.error(
                "Measure '" + xmlMeasureRef.name + "' not found",
                xmlMeasureRef,
                "name");
            return null;
        }
        if (!(measure instanceof RolapBaseCubeMeasure)) {
            handler.error(
                "Measure '" + xmlMeasureRef.name + "' is not a stored measure",
                xmlMeasureRef,
                "name");
            return null;
        }
        final RolapBaseCubeMeasure baseMeasure = (RolapBaseCubeMeasure) measure;
        final RolapSchema.PhysColumn physColumn =
            getPhysColumn(
                measureGroup.getFactRelation(),
                xmlMeasureRef.aggColumn,
                xmlMeasureRef,
                "aggColumn");
        if (physColumn == null) {
            return null;
        }

        final RolapMeasureGroup.RolapMeasureRef measureRef =
            new RolapMeasureGroup.RolapMeasureRef(
                baseMeasure,
                physColumn);
        measureGroup.measureRefList.add(measureRef);
        return measureRef;
    }

    private void addCopyLink(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        MondrianDef.CopyLink xmlCopyLink)
    {
        final RolapSchema.PhysPath path =
            new RolapSchema.PhysPathBuilder(measureGroup.getFactRelation())
                .done();
        dimensionPaths.put(
            Pair.of(measureGroup, dimension),
            path);
        final RolapSchema.PhysRelation fact = measureGroup.getFactRelation();
        for (MondrianDef.Column xmlColumn : xmlCopyLink.columnRefs) {
            final RolapSchema.PhysColumn aggColumn =
                getPhysColumn(
                    fact,
                    xmlColumn.aggColumn,
                    xmlColumn,
                    "aggColumn");
            if (aggColumn == null) {
                // Column not found. Error already posted. Ignore this link to
                // make progress.
                continue;
            }
            final RolapSchema.PhysColumn column =
                getPhysColumn(
                    last(fact, xmlColumn.table, xmlColumn, "table"),
                    xmlColumn.name,
                    xmlColumn,
                    "name");
            if (column == null) {
                // Column not found. Error already posted. Ignore this link to
                // make progress.
                continue;
            }
            RolapStar.Column starAggColumn =
                registerExpr(
                    measureGroup,
                    dimension,
                    path,
                    aggColumn,
                    null,
                    null);
            measureGroup.copyColumnList.add(Pair.of(starAggColumn, column));
        }
    }

    private static RolapMember findMeasure(
        List<RolapMember> memberList, String name)
    {
        for (RolapMember member : memberList) {
            if (Util.equalName(member.getName(), name)) {
                return member;
            }
            if (member.getUniqueName().equals(name)) {
                return member;
            }
        }
        return null;
    }

    private void registerDimension(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        RolapSchema.PhysPath path)
    {
        for (RolapCubeHierarchy hierarchy : dimension.getHierarchyList()) {
            registerHierarchy(measureGroup, hierarchy);
        }
        for (RolapAttribute attribute : dimension.attributeMap.values()) {
            registerAttribute(measureGroup, dimension, attribute, path);
        }
    }

    private void registerHierarchy(
        RolapMeasureGroup measureGroup,
        RolapCubeHierarchy hierarchy)
    {
        for (RolapCubeLevel level : hierarchy.getLevelList()) {
            registerLevel(measureGroup, level);
        }
    }

    private void registerLevel(
        RolapMeasureGroup measureGroup,
        RolapCubeLevel level)
    {
        final RolapCubeLevel peer = level.getClosedPeer();
        if (peer != null) {
            RolapAttribute attribute = peer.getAttribute();
            registerAttribute(
                measureGroup,
                peer.getDimension(),
                attribute,
                dimensionPaths.get(Pair.of(measureGroup, peer.getDimension())));
        }
    }

    private void registerAttribute(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        RolapAttribute attribute,
        RolapSchema.PhysPath path)
    {
        for (RolapSchema.PhysColumn column : attribute.getKeyList()) {
            registerExpr(
                measureGroup, dimension, path, column,
                attribute.getName(), "Key");
        }
        registerExpr(
            measureGroup, dimension, path, attribute.getNameExp(),
            attribute.getName(), "Name");
        registerExpr(
            measureGroup, dimension, path, attribute.getCaptionExp(),
            attribute.getName(), "Caption");
        for (RolapSchema.PhysColumn column : attribute.getOrderByList()) {
            registerExpr(
                measureGroup, dimension, path, column,
                attribute.getName(), "OrderBy");
        }

        // No need to register properties, or the parent attribute. They are all
        // based on attributes in this dimension, so will be registered in due
        // course.
    }

    /**
     * Registers an expression as a column.
     *
     * @param measureGroup Measure group
     * @param dimension Dimension
     * @param path Path from measure group's fact table to root of dimension
     * @param expr Expression to register
     * @param name Name of level (for descriptive purposes)
     * @param property Property of level (for descriptive purposes, may be null)
     */
    private static RolapStar.Column registerExpr(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        RolapSchema.PhysPath path,
        RolapSchema.PhysColumn expr,
        String name,
        String property)
    {
        assert path != null;
        if (expr == null) {
            return null;
        }

        for (Pair<RolapStar.Column, RolapSchema.PhysColumn> pair
            : measureGroup.copyColumnList)
        {
            if (pair.right.equals(expr)) {
                return pair.left;
            }
        }

        final RolapSchema.PhysSchemaGraph graph =
            measureGroup.getFactRelation().getSchema().getGraph();
        RolapSchema.PhysPathBuilder pathBuilder =
            new RolapSchema.PhysPathBuilder(path);
        try {
            graph.findPath(pathBuilder, expr.relation);
        } catch (RolapSchema.PhysSchemaException e) {
            // TODO: user error
            throw Util.newInternal(
                e, "Could not find path to " + expr.relation);
        }
        final RolapStar star = measureGroup.getStar();
        RolapStar.Table starTable = star.getTable(pathBuilder.done());
        final RolapStar.Column starColumn =
            starTable.lookupColumnByExpression(
                expr, true, name, property);
        assert starColumn != null;
        measureGroup.starColumnMap.put(
            Pair.of(dimension, expr),
            starColumn);
        return starColumn;
    }

    /**
     * Creates a measure in the current cube from an XML definition.
     *
     * <p>The measure's ordinal is -1 unless the "MEMBER_ORDINAL" property is
     * set.
     *
     * @param measureGroup Measure group
     * @param xmlMeasure XML measure definition
     * @param measureGroupTable Table for measure group (may be null)
     * @return measure
     */
    private RolapBaseCubeMeasure createMeasure(
        RolapMeasureGroup measureGroup,
        MondrianDef.Measure xmlMeasure,
        final RolapSchema.PhysRelation measureGroupTable)
    {
        // A measure can either have table & column attributes, or an included
        // MeasureExpression element.
        RolapSchema.PhysColumn measureExp;

        RolapSchema.PhysRelation table =
            last(measureGroupTable, xmlMeasure.table, xmlMeasure, "table");
        final RolapCube cube = measureGroup.getCube();
        if (table == null) {
            throw MondrianResource.instance()
                .MeasureWithColumnMustHaveTable.ex(
                    cube.getName(), xmlMeasure.name);
        }

        measureExp =
            createColumn(
                xmlMeasure,
                "column",
                measureGroupTable,
                xmlMeasure.column,
                xmlMeasure.getArguments());

        // Validate aggregator name.
        final RolapAggregator aggregator =
            lookupAggregator(xmlMeasure.aggregator);

        if (measureExp == null) {
            if (aggregator == RolapAggregator.Count) {
                // it's ok if count has no expression; it means 'count(*)'
            } else {
                throw MondrianResource.instance().BadMeasureSource.ex(
                    cube.getName(), xmlMeasure.name);
            }
        }

        final Set<RolapSchema.PhysRelation> relationSet =
            new HashSet<RolapSchema.PhysRelation>();
        PhysSchemaBuilder.collectRelations(
            aggregator, measureExp, measureGroup.getFactRelation(),
            relationSet);
        if (relationSet.size() != 1) {
            handler.error(
                "Measure '" + xmlMeasure.name
                + "' must belong to one and only one relation",
                xmlMeasure,
                xmlMeasure.column != null ? "column" : null);
        }

        // Derive the datatype.
        Dialect.Datatype datatype =
            aggregator.deriveDatatype(
                measureExp == null
                    ? Collections.<Dialect.Datatype>emptyList()
                    : Collections.singletonList(measureExp.getDatatype()));
        final Dialect.Datatype specifiedDatatype;
        if (xmlMeasure.datatype == null) {
            specifiedDatatype = null;
        } else {
            specifiedDatatype = Util.lookup(
                Dialect.Datatype.class,
                xmlMeasure.datatype);
            if (specifiedDatatype == null) {
                handler.error(
                    "Invalid datatype '" + xmlMeasure.datatype + "'",
                    xmlMeasure,
                    "datatype");
            }
        }
        if (specifiedDatatype != null
            && datatype != null
            && specifiedDatatype != datatype)
        {
            handler.error(
                "Datatype '" + datatype + "' of measure '" + xmlMeasure.name
                + "' is inconsistent with stated datatype '"
                + specifiedDatatype + "'",
                xmlMeasure,
                null);
        }
        if (datatype == null) {
            if (specifiedDatatype == null) {
                handler.error(
                    "Datatype of measure '" + xmlMeasure.name
                    + "' cannot be derived, so must be specified",
                    xmlMeasure,
                    null);
            } else {
                datatype = specifiedDatatype;
            }
        }

        // If there were errors, assume numeric, just to make progress.
        if (datatype == null) {
            datatype = Dialect.Datatype.Numeric;
        }

        String formatString = xmlMeasure.formatString;
        if (formatString == null) {
            formatString = "";
        }

        // Set member's visibility, default true.
        final boolean visible = toBoolean(xmlMeasure.visible, true);

        final RolapCubeHierarchy measuresHierarchy =
            cube.getHierarchyList().get(0);
        final RolapCubeLevel level = measuresHierarchy.getLevelList().get(0);
        final Larder larder =
            createLarder(
                cube.getUniqueName() + "."
                    + Util.quoteMdxIdentifier(xmlMeasure.name) + ".measure",
                xmlMeasure.getAnnotations(),
                xmlMeasure.name,
                xmlMeasure.caption,
                xmlMeasure.description)
                .add(
                    Property.FORMAT_EXP_PARSED,
                    Literal.createString(formatString))
                .add(Property.FORMAT_EXP, formatString)
                .add(Property.AGGREGATION_TYPE, aggregator)
                .add(Property.DATATYPE, datatype.name())
                .add(Property.VISIBLE, visible)
                .build();
        final RolapBaseCubeMeasure measure =
            new RolapBaseCubeMeasure(
                measureGroup,
                level,
                xmlMeasure.name,
                Util.makeFqName(level.getHierarchy(), xmlMeasure.name),
                measureExp,
                aggregator,
                datatype,
                larder);
        measureGroup.measureList.add(measure);
        validator.putXml(measure, xmlMeasure);

        CellFormatter cellFormatter =
            makeCellFormatter(xmlMeasure, measure.getUniqueName());
        if (cellFormatter != null) {
            measure.setFormatter(cellFormatter);
        }

        List<String> propNames = new ArrayList<String>();
        List<String> propExprs = new ArrayList<String>();
        validateMemberProps(
            cube,
            xmlMeasure.getCalculatedMemberProperties(),
            propNames, propExprs, xmlMeasure.name);
        for (int j = 0; j < propNames.size(); j++) {
            String propName = propNames.get(j);
            final Object propExpr = propExprs.get(j);
            measure.setProperty(propName, propExpr);
        }
        measure.setOrdinal(-1);
        for (int j = 0; j < propNames.size(); j++) {
            String propName = propNames.get(j);
            final Object propExpr = propExprs.get(j);
            measure.setProperty(propName, propExpr);
            if (propName.equals(Property.MEMBER_ORDINAL.name)
                && propExpr instanceof String)
            {
                final String expr = (String) propExpr;
                if (expr.startsWith("\"")
                    && expr.endsWith("\""))
                {
                    try {
                        Integer ordinal = Integer.valueOf(
                            expr.substring(1, expr.length() - 1));
                        measure.setOrdinal(ordinal);
                        measure.setOrderKey(ordinal);
                    } catch (NumberFormatException e) {
                        Util.discard(e);
                    }
                }
            }
        }
        return measure;
    }

    private boolean hasMeasure(RolapCube cube, String measureName) {
        for (RolapMeasureGroup measureGroup : cube.getMeasureGroups()) {
            if (measureGroup.measureList.get(measureName) != null) {
                return true;
            }
        }
        return false;
    }

    private static RolapAggregator lookupAggregator(String aggregatorName) {
        RolapAggregator aggregator;

        // Lookup aggregator by name. Substitute deprecated "distinct count"
        // with modern "distinct-count".
        if (aggregatorName.equals("distinct count")) {
            aggregator = RolapAggregator.DistinctCount;
        } else {
            aggregator =
                RolapAggregator.enumeration.getValue(aggregatorName, false);
        }
        if (aggregator == null) {
            StringBuilder buf = new StringBuilder();
            for (String aggName : RolapAggregator.enumeration.getNames()) {
                if (buf.length() > 0) {
                    buf.append(", ");
                }
                buf.append('\'');
                buf.append(aggName);
                buf.append('\'');
            }
            throw MondrianResource.instance().UnknownAggregator.ex(
                aggregatorName,
                buf.toString());
        }
        return aggregator;
    }

    private void addForeignKeyLink(
        RolapSchema.PhysRelation fact,
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        MondrianDef.ForeignKeyLink xmlForeignKeyLink)
    {
        final List<RolapSchema.PhysColumn> foreignKeyList =
            createColumnList(
                xmlForeignKeyLink,
                "foreignKeyColumn",
                fact,
                xmlForeignKeyLink.foreignKeyColumn,
                xmlForeignKeyLink.foreignKey);
        if (foreignKeyList == null) {
            // there was an error
            return;
        }

        RolapAttribute keyAttribute;
        RolapSchema.PhysKey sourceKey;

        if (xmlForeignKeyLink.attribute == null) {
            keyAttribute = dimension.rolapDimension.keyAttribute;
            // Note, with the current logic related to degenerate dims and
            // single attribute dimensions in getOrCreateDimension, this
            // codepath can never be reached. Leaving here in case that logic
            // eventually changes.
            if (keyAttribute == null) {
                getHandler().error(
                    "Dimension '"
                    + dimension.getName()
                    + "' is used in a dimension link but has no key attribute. "
                    + "Please specify key.",
                    xmlForeignKeyLink,
                    null);
                return;
            }
            sourceKey = dimension.rolapDimension.key.get();
        } else {
            keyAttribute =
                dimension.rolapDimension.attributeMap
                    .get(xmlForeignKeyLink.attribute);
            if (keyAttribute == null) {
                getHandler().error(
                    "Invalid attribute '"
                    + xmlForeignKeyLink.attribute
                    + "' in ForeignKeyLink. Dimension '"
                    + dimension.getName()
                    + "' has no such attribute.",
                    xmlForeignKeyLink,
                    "attribute");
                return;
            }
            sourceKey =
                lookupKey(
                    null, // TODO:
                    true,
                    keyAttribute);
        }

        final List<RolapSchema.PhysColumn> keyList = keyAttribute.getKeyList();
        if (foreignKeyList.size() != keyList.size()) {
            handler.error(
                "Number of foreign key columns "
                + foreignKeyList.size()
                + " does not match number of key columns "
                + keyList.size(),
                xmlForeignKeyLink,
                "foreignKey");
        }

        final RolapSchema.PhysPathBuilder pathBuilderOrig =
            new RolapSchema.PhysPathBuilder(fact)
                .add(sourceKey, foreignKeyList);
        RolapSchema.PhysPath path = pathBuilderOrig.clone().done();

        measureGroup.addLink(dimension, path);
        dimensionPaths.put(Pair.of(measureGroup, dimension), path);

        // Make sure that every attribute is registered as an expression in
        // the star.
        if (Util.deprecated(false, false))
        for (RolapAttribute attribute : dimension.attributeMap.values()) {
            RolapSchema.PhysSchemaGraph graph =
                measureGroup.getFactRelation().getSchema().getGraph();
            final RolapSchema.PhysRelation relation =
                uniqueTable(attribute.getKeyList());
            if (relation == null && Util.deprecated(false, false)) {
                if (attribute.getKeyList().isEmpty()) {
                    throw Util.newInternal(
                        "attribute " + attribute + " has empty key");
                } else {
                    throw Util.newInternal(
                        "attribute " + attribute + " has key whose columns "
                        + "belong to inconsistent relations "
                        + attribute.getKeyList());
                }
            }
            for (RolapSchema.PhysColumn column : attribute.getKeyList()) {
                final Pair<RolapCubeDimension, RolapSchema.PhysColumn> key =
                    Pair.of(dimension, column);
                if (measureGroup.starColumnMap.containsKey(key)) {
                    continue;
                }
                RolapSchema.PhysPathBuilder pathBuilder =
                    pathBuilderOrig.clone();
                try {
                    graph.findPath(pathBuilder, column.relation);
                } catch (RolapSchema.PhysSchemaException e) {
                    // TODO: user error
                    throw Util.newInternal(
                        "Could not find path to " + column.relation);
                }
                final RolapStar star = measureGroup.getStar();
                RolapStar.Table starTable = star.getTable(pathBuilder.done());
                final RolapStar.Column starColumn =
                    starTable.lookupColumnByExpression(
                        column, true, null, null);
                assert starColumn != null;
                measureGroup.starColumnMap.put(
                    key,
                    starColumn);
            }
        }
    }

    /**
     * Returns the unique relation that a list of columns belong to.
     *
     * <p>Returns null if and the list is empty or if the columns' relations are
     * inconsistent.
     *
     * @param columnList List of columns
     * @return Null if list is empty or columns' relations are inconsistent
     */
    RolapSchema.PhysRelation uniqueTable(
        List<RolapSchema.PhysColumn> columnList)
        throws RuntimeException
    {
        RolapSchema.PhysRelation relation = null;
        for (RolapSchema.PhysColumn column : columnList) {
            if (relation == null) {
                relation = column.relation;
            } else if (relation == column.relation) {
                // ok
            } else {
                return null;
            }
        }
        return relation;
    }

    private void addFactLink(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension)
    {
        final RolapSchema.PhysPath path =
            new RolapSchema.PhysPathBuilder(measureGroup.getFactRelation())
                .done();
        measureGroup.addLink(dimension, path);
        dimensionPaths.put(Pair.of(measureGroup, dimension), path);
    }

    /**
     * Checks that the ordinals of measures (including calculated measures)
     * are unique.
     *
     * @param xmlCube        cube (required for error messages)
     */
    private void checkOrdinals(MondrianDef.Cube xmlCube) {
        Map<Integer, String> ordinals = new HashMap<Integer, String>();
        for (RolapMember measure : nonSystemMeasures()) {
            int ordinal = measure.getOrdinal();
            final String prev = ordinals.put(ordinal, measure.getUniqueName());
            if (prev != null) {
                throw MondrianResource.instance().MeasureOrdinalsNotUnique.ex(
                    xmlCube.name,
                    String.valueOf(ordinal),
                    prev,
                    measure.getUniqueName());
            }
        }
    }

    /**
     * Creates a dimension from its XML definition. If the XML definition's
     * 'source' attribute is set, and the shared dimension is cached in the
     * schema, returns that.
     *
     *
     * @param xmlCubeDimension XML Dimension or DimensionUsage
     * @param schema Schema
     * @param xmlSchema XML Schema
     * @param dimensionOrdinal Ordinal of dimension
     * @param cubeHierarchyList List of hierarchies in cube
     * @param annotations Annotations
     * @return A dimension, or null if shared dimension does not exist
     */
    private RolapCubeDimension getOrCreateDimension(
        RolapCube cube,
        final MondrianDef.Dimension xmlCubeDimension,
        RolapSchema schema,
        final MondrianDef.Schema xmlSchema,
        int dimensionOrdinal,
        List<RolapCubeHierarchy> cubeHierarchyList,
        final NamedList<MondrianDef.Annotation> annotations)
    {
        final String dimensionName =
            first(xmlCubeDimension.name, xmlCubeDimension.source);

        if (cube.dimensionList.get(dimensionName) != null) {
            getHandler().error(
                "Duplicate dimension '" + dimensionName + "'",
                xmlCubeDimension,
                null);
            return null;
        }

        final MondrianDef.Dimension xmlDimension;
        if (xmlCubeDimension.source != null) {
            xmlDimension =
                useSharedDimension(cube, xmlCubeDimension, schema, xmlSchema);
            if (xmlDimension == null) {
                return null;
            }
        } else {
            xmlDimension = xmlCubeDimension;
        }
        final org.olap4j.metadata.Dimension.Type dimensionType =
            xmlDimension.type == null
                ? org.olap4j.metadata.Dimension.Type.OTHER
                : org.olap4j.metadata.Dimension.Type.valueOf(xmlDimension.type);
        final RolapDimension dimension =
            new RolapDimension(
                schema,
                xmlDimension.name,
                toBoolean(xmlDimension.visible, true),
                dimensionType,
                xmlDimension.hanger,
                createLarder(
                    Util.quoteMdxIdentifier(xmlDimension.name) + ".dimension",
                    annotations,
                    xmlDimension.name,
                    xmlDimension.caption,
                    xmlDimension.description).build());
        validator.putXml(dimension, xmlDimension);

        // Must add this dimension to the map of shared dimensions.
        // (it is bad practice to leak references to a constructed object
        // within its constructor, so we do it here.)
        if (xmlCubeDimension.source != null
            && !schema.sharedDimensions.containsKey(dimension.getName()))
        {
            // Add a placeholder to prevent recursion.
            ((Map) schema.sharedDimensions).put(dimension.getName(), DUMMY);

            // Schedule an action for when we finish the current cube.
            postCubeActions.add(
                new Util.Function0() {
                    public Object apply() {
                        sharedDimension(
                            dimension, xmlCubeDimension.source, xmlSchema);
                        return null;
                    }
                });
        }

        // Load attributes before hierarchies, because levels refer to
        // attributes. Likewise create attributes before properties.
        final RolapSchema.PhysRelation dimensionRelation =
            xmlDimension.hanger
                ? getPhysRelation(EMPTY_TABLE_NAME, null, null)
                : xmlDimension.table == null
                ? null
                : getPhysRelation(
                    xmlDimension.table, xmlDimension, "table");

        List<RolapAttribute> attributeList = new ArrayList<RolapAttribute>();
        for (MondrianDef.Attribute xmlAttribute : xmlDimension.getAttributes())
        {
            RolapAttribute attribute =
                createAttribute(
                    xmlAttribute, null, dimensionRelation, dimension);
            if (attribute == null) {
                continue;
            }
            RolapAttribute put =
                dimension.attributeMap.put(attribute.getName(), attribute);
            assert put == null : "validation rule should have caught dup attr";
            attributeList.add(attribute);
        }

        // Attributes and parent-child links.
        int attributeHierarchyCount = 0;
        for (RolapAttribute attribute : attributeList) {
            final MondrianDef.Attribute xmlAttribute =
                validator.getXml(attribute, true);

            createProperties(
                physSchemaBuilder,
                dimension,
                attribute,
                dimensionRelation,
                xmlAttribute,
                attribute.getExplicitProperties());
        }

        // Resolve key, if specified.
        //
        // Dimensions usually have a key. But they don't need one if they
        // are only going to be used as degenerate dimensions. So we will check
        // when the dimension is used.
        if (xmlDimension.key != null) {
            dimension.keyAttribute =
                dimension.attributeMap.get(xmlDimension.key);
            if (dimension.keyAttribute == null) {
                getHandler().error(
                    "Key attribute '"
                    + xmlDimension.key
                    + "' is not a valid attribute of this dimension",
                    xmlDimension,
                    "key");
                return null;
            }
            if (uniqueRelation(dimension) == null) {
                getHandler().error(
                    "Columns in key of dimension's key attribute '"
                    + xmlDimension.key
                    + "' do not belong to same relation",
                    xmlDimension,
                    "key");
                return null;
            }
        } else {
            // degenerate dim, should only be a single attribute
            if (attributeList.size() > 1) {
                getHandler().error(
                    MondrianResource.instance()
                    .DimensionKeyOmitted.ex(xmlDimension.name),
                    validator.getXmls(dimension),
                    null);
            }
            dimension.keyAttribute = attributeList.get(0);
        }

        // Use a Lazy variable to initialize dimension's key on first use.
        // Degenerate dimensions don't need keys.
        // But regular dimensions do, and they need to be validated using
        // information only available right here.
        dimension.key =
            new Lazy<RolapSchema.PhysKey>(
                new Util.Function0<RolapSchema.PhysKey>() {
                    public RolapSchema.PhysKey apply() {
                        return lookupKey(
                            xmlDimension, true, dimension.keyAttribute);
                    }
                }
            );

        for (MondrianDef.Hierarchy xmlHierarchy : xmlDimension.getHierarchies())
        {
            // If hierarchy is not named, name is same as dimension.
            String hierarchyName = first(
                xmlHierarchy.name,
                xmlDimension.name);
            RolapHierarchy hierarchy =
                new RolapHierarchy(
                    dimension,
                    hierarchyName,
                    Util.makeFqName(dimension, hierarchyName),
                    toBoolean(xmlHierarchy.visible, true),
                    toBoolean(xmlHierarchy.hasAll, true),
                    null,
                    null,
                    createLarder(
                        Util.makeFqName(dimension, hierarchyName)
                        + ".hierarchy",
                        xmlHierarchy.getAnnotations(),
                        hierarchyName,
                        xmlHierarchy.caption,
                        xmlHierarchy.description).build());
            validator.putXml(hierarchy, xmlHierarchy);
            dimension.addHierarchy(hierarchy);
            hierarchy.initHierarchy(this, xmlHierarchy.allLevelName);
            if (xmlHierarchy.getLevels().size() == 0) {
                throw MondrianResource.instance().HierarchyHasNoLevels.ex(
                    hierarchy.getUniqueName());
            }
            for (MondrianDef.Level xmlLevel : xmlHierarchy.getLevels()) {
                RolapLevel level =
                    createLevel(
                        cube,
                        hierarchy,
                        dimensionRelation,
                        hierarchy.levelList.size(),
                        xmlLevel);
                if (level == null) {
                    continue;
                }
                hierarchy.levelList.add(level);
            }
            deferAssignDefaultMember(
                cube, hierarchy, xmlHierarchy, xmlHierarchy.defaultMember);
        }

        for (RolapAttribute attribute : attributeList) {
            final MondrianDef.Attribute xmlAttribute =
                validator.getXml(attribute, true);
            if (toBoolean(
                    xmlAttribute.hasHierarchy,
                    countHierarchies(
                        dimension.getHierarchyList(), attribute) == 0))
            {
                if (dimension.getHierarchyList().get(xmlAttribute.name) != null)
                {
                    handler.error(
                        "Cannot create hierarchy for attribute '"
                        + xmlAttribute.name
                        + "'; dimension already has a hierarchy of that name",
                        xmlAttribute,
                        "name");
                    continue;
                }
                ++attributeHierarchyCount;
                // Create attribute hierarchy.
                final String uniqueName =
                    Util.makeFqName(dimension, xmlAttribute.name);
                RolapHierarchy hierarchy =
                    new RolapHierarchy(
                        dimension,
                        xmlAttribute.name,
                        uniqueName,
                        toBoolean(xmlAttribute.visible, true),
                        toBoolean(xmlAttribute.hierarchyHasAll, true),
                        null,
                        attribute,
                        createLarder(
                            cube + "." + uniqueName + ".hierarchy",
                            null,
                            xmlAttribute.name,
                            first(
                                xmlAttribute.hierarchyCaption,
                                xmlAttribute.caption),
                            xmlAttribute.description)
                            .populate(attribute.getLarder())
                            .build());
                dimension.addHierarchy(hierarchy);
                dimension.attributeMap.put(
                    attribute.getName(),
                    attribute);
                hierarchy.initHierarchy(
                    this,
                    hierarchy.hasAll()
                        ? xmlAttribute.hierarchyAllLevelName
                        : null);
                hierarchy.levelList.add(
                    new RolapLevel(
                        hierarchy,
                        xmlAttribute.name,
                        toBoolean(xmlAttribute.visible, true),
                        hierarchy.hasAll() ? 1 : 0,
                        attribute,
                        null,
                        attribute.getOrderByList(),
                        null,
                        null,
                        RolapLevel.HideMemberCondition.Never,
                        createLarder(
                            cube + "."
                                + Util.makeFqName(hierarchy, xmlAttribute.name)
                                + ".level",
                            null,
                            null,
                            null,
                            null)
                            .populate(
                                createLarder(
                                    cube + "."
                                        + Util.makeFqName(
                                            hierarchy, xmlAttribute.name)
                                        + ".level",
                                    xmlAttribute.getAnnotations(),
                                    xmlAttribute.name,
                                    xmlAttribute.caption,
                                    xmlAttribute.description).build())
                            .build(),
                        resourceMap));
                deferAssignDefaultMember(
                    cube, hierarchy, xmlAttribute,
                    xmlAttribute.hierarchyDefaultMember);
            }
        }

        if (dimension.getHierarchyList().isEmpty()
            && attributeHierarchyCount == 0)
        {
            getHandler().error(
                "Dimension '"
                + dimension.getName()
                + "' must have at least one hierarchy (or attribute "
                + "hierarchy).",
                xmlDimension,
                null);
            return null;
        }

        validateDimensionType(dimension);

        // wrap the shared or regular dimension with a
        // rolap cube dimension object
        final RolapCubeDimension cubeDimension =
            new RolapCubeDimension(
                cube,
                dimension,
                dimensionName,
                dimensionOrdinal,
                createLarder(
                    cube.getUniqueName() + "."
                        + Util.quoteMdxIdentifier(dimensionName) + ".dimension",
                    xmlCubeDimension.getAnnotations(),
                    dimensionName,
                    first(xmlCubeDimension.caption, xmlDimension.caption),
                    first(
                        xmlCubeDimension.description,
                        xmlDimension.description))
                    .populate(dimension.getLarder())
                    .build());
        initCubeDimension(
            cubeDimension, xmlCubeDimension.source, cubeHierarchyList);
        validator.putXml(cubeDimension, xmlCubeDimension);

        // Populate attribute map. (REVIEW: Should attributes go ONLY in the
        // cube dimension?)
        cubeDimension.attributeMap.putAll(dimension.attributeMap);

        return cubeDimension;
    }

    /** Ensures that there is a dummy cube for each shared dimension.
     *
     * <p>You must not call this method while a cube is being loaded.</p>
     */
    private void sharedDimension(
        RolapDimension dimension,
        String source,
        MondrianDef.Schema xmlSchema)
    {
        // Check that placeholder is still present. Necessary to prevent
        // recursion.
        final String name = dimension.getName();
        assert ((Map) schema.sharedDimensions).get(name)
            == DUMMY;

        final MondrianDef.Cube xmlCube = new MondrianDef.Cube();
        xmlCube.name = "$" + source;
        xmlCube.enableScenarios = false;
        xmlCube.visible = Boolean.FALSE;
        xmlCube.defaultMeasure = FACT_COUNT_MEASURE_NAME;

        final MondrianDef.Dimension xmlDimension =
            new MondrianDef.Dimension();
        xmlCube.children
            .holder(new MondrianDef.Dimensions())
            .list()
            .add(xmlDimension);
        xmlDimension.source = source;

        final MondrianDef.MeasureGroup xmlMeasureGroup =
            new MondrianDef.MeasureGroup();
        xmlCube.children
            .holder(new MondrianDef.MeasureGroups())
            .list()
            .add(xmlMeasureGroup);
        xmlMeasureGroup.table =
            dimension.keyAttribute.getKeyList().get(0).relation.getAlias();

        final MondrianDef.Measure xmlMeasure = new MondrianDef.Measure();
        xmlMeasureGroup.children
            .holder(new MondrianDef.Measures())
            .list()
            .add(xmlMeasure);
        xmlMeasure.name = FACT_COUNT_MEASURE_NAME;
        xmlMeasure.aggregator = RolapAggregator.Count.name;

        final MondrianDef.FactLink xmlFactLink = new MondrianDef.FactLink();
        xmlMeasureGroup.children
            .holder(new MondrianDef.DimensionLinks())
            .list()
            .add(xmlFactLink);
        xmlFactLink.dimension = name;

        final RolapCube cube =
            createCube(
                schema,
                xmlCube,
                xmlSchema);
        final RolapCubeDimension cubeDimension =
            cube.getDimensionList().get(1);
        schema.sharedDimensions.put(name, cubeDimension);
    }

    private MondrianDef.Dimension useSharedDimension(
        RolapCube cube,
        MondrianDef.Dimension xmlCubeDimension,
        RolapSchema schema,
        MondrianDef.Schema xmlSchema)
    {
        MondrianDef.Dimension xmlDimension;
        if (xmlCubeDimension.key != null) {
            getHandler().warning(
                "Attribute '"
                + "key"
                + "' must not be specified in dimension that references "
                + "other dimension",
                xmlCubeDimension,
                "source");
        }

        MondrianDef.Dimension sharedDimension =
            xmlSchema.getDimensions().get(xmlCubeDimension.source);
        if (sharedDimension == null) {
            getHandler().error(
                "Unknown shared dimension '"
                + xmlCubeDimension.source
                + "' in definition of dimension '"
                + xmlCubeDimension.name
                + "'",
                    xmlCubeDimension,
                "source");
            return null;
        }
        // this section of code handles the case where the schema uses the
        // same shared dimension multiple times.  The SQL generated uses
        // the phys schema objects so we need to create copies of those
        // objects with a different alias
        MondrianDef.Dimension clonedDim = null;
        if (cubeToDimMap.containsKey(cube)
            && cubeToDimMap.getCollection(cube).contains(sharedDimension))
        {
            try {
                LinkedHashMap<String, RolapSchema.PhysRelation> tbls =
                    schema.getPhysicalSchema().tablesByName;
                RolapSchema.PhysRelation physRelation =
                    tbls.get(sharedDimension.table);
                if (physRelation != null) {
                    String newAlias = newTableAlias(physRelation, tbls);
                    RolapSchema.PhysRelation clonedRelation =
                        physRelation.cloneWithAlias(newAlias);
                    tbls.put(newAlias, clonedRelation);
                    clonedDim =
                        new MondrianDef.Dimension(sharedDimension._def);
                    clonedDim.table = newAlias;
                }
            } catch (XOMException e) {
                throw new MondrianException(e);
            }
        }
        cubeToDimMap.put(cube, sharedDimension);
        if (clonedDim != null) {
            xmlDimension = clonedDim;
        } else {
            xmlDimension = sharedDimension;
        }
        return xmlDimension;
    }

    private String newTableAlias(
        RolapSchema.PhysRelation physRelation,
        LinkedHashMap<String, RolapSchema.PhysRelation> tbls)
    {
        String alias = physRelation.getAlias();
        int i = 1;
        while (true) {
            String candidateAlias = alias + "_" + i;
            if (!tbls.containsKey(candidateAlias)) {
                return candidateAlias;
            }
            i++;
        }
    }

    public static boolean toBoolean(Boolean aBoolean, boolean dflt) {
        return aBoolean == null ? dflt : aBoolean;
    }

    /**
     * Returns the number of hierarchies that a given attribute appears as a
     * level of.
     *
     * @param hierarchyList List of hierarchies
     * @param attribute Attribute
     * @return number of levels using attribute
     */
    private int countHierarchies(
        List<? extends RolapHierarchy> hierarchyList, RolapAttribute attribute)
    {
        int n = 0;
        for (RolapHierarchy hierarchy : hierarchyList) {
            for (RolapLevel level : hierarchy.getLevelList()) {
                if (level.attribute == attribute) {
                    ++n;
                }
            }
        }
        return n;
    }

    /**
     * Returns the key by which a dimension is to be linked to a fact table
     * (or fact tables). The key must contain the same columns as the key of the
     * key attribute. It must also be a key defined on its table.
     *
     * <p>Finds a key that is on the has the same columns, in the same order, as
     * the attribute's list of key columns.</p>
     *
     * <p>NOTE: We currently assume that it is unique, but we do not enforce
     * it.</p>
     *
     * <p>If not found, posts an error and returns null.</p>
     *
     * <p>We do not store the key as part of a dimension, because some
     * dimensions are degenerate (i.e. reside in fact table) and so do not need
     * a key.</p>
     *
     * @param xmlDimension Dimension XML element
     * @param create Whether to create a key if not found
     * @param keyAttribute Key attribute of target dimension
     * @return Key, or null if not found
     */
    private RolapSchema.PhysKey lookupKey(
        MondrianDef.Dimension xmlDimension,
        boolean create,
        RolapAttribute keyAttribute)
    {
        // Find the unique relation of the attribute's list of key columns.
        final RolapSchema.PhysRelation relation =
            uniqueRelation(keyAttribute.getDimension());
        if (relation == null) {
            return null;
        }
        for (RolapSchema.PhysKey key : relation.getKeyList()) {
            if (key.columnList.equals(keyAttribute.getKeyList())) {
                return key;
            }
        }
        if (create) {
            return relation.addKey(
                "k$" + relation.getKeyList().size(),
                keyAttribute.getKeyList());
        }
        getHandler().error(
            "The columns of dimension's key attribute do not match "
            + "a known key of table '" + relation + "'",
            xmlDimension,
            "key");
        return null;
    }

    static RolapSchema.PhysRelation uniqueRelation(
        RolapDimension dimension)
    {
        final Set<RolapSchema.PhysRelation> relations =
            new HashSet<RolapSchema.PhysRelation>();
        for (RolapSchema.PhysColumn key : dimension.keyAttribute.getKeyList()) {
            relations.add(key.relation);
        }
        return relations.size() != 1 ? null : relations.iterator().next();
    }

    private void validateDimensionType(
        RolapDimension dimension)
    {
        for (RolapHierarchy hierarchy : dimension.getHierarchyList()) {
            for (RolapLevel level : hierarchy.getLevelList()) {
                if (level.getLevelType().isTime()
                    && dimension.getDimensionType()
                    != org.olap4j.metadata.Dimension.Type.TIME)
                {
                    getHandler().error(
                        MondrianResource.instance().TimeLevelInNonTimeHierarchy
                            .ex(dimension.getUniqueName()),
                        validator.getXmls(level, hierarchy, dimension),
                        null);
                }
                if (!level.getLevelType().isTime()
                    && !level.isAll()
                    && dimension.getDimensionType()
                    == org.olap4j.metadata.Dimension.Type.TIME
                    && false)
                {
                    getHandler().error(
                        MondrianResource.instance().NonTimeLevelInTimeHierarchy
                            .ex(level.getUniqueName()),
                        validator.getXmls(level, hierarchy, dimension),
                        null);
                }
            }
        }
    }

    /**
     * Initializes a {@link RolapCubeDimension}, creating a
     * {@link RolapCubeHierarchy} for each {@link RolapHierarchy} in the
     * underlying dimension.
     *
     * @param cubeDimension Cube dimension
     * @param dimSource Name of source dimension
     * @param hierarchyList List of hierarchies in cube
     */
    void initCubeDimension(
        RolapCubeDimension cubeDimension,
        String dimSource,
        List<RolapCubeHierarchy> hierarchyList)
    {
        final int originalSize = hierarchyList.size();
        for (RolapHierarchy hierarchy
            : cubeDimension.rolapDimension.getHierarchyList())
        {
            final String uniqueName =
                hierarchy.getDimension().isMeasures()
                    ? hierarchy.getUniqueName()
                    : Util.makeFqName(cubeDimension, hierarchy.getName());
            final RolapCubeHierarchy cubeHierarchy =
                new RolapCubeHierarchy(
                    this,
                    cubeDimension,
                    hierarchy,
                    hierarchy.getName(),
                    uniqueName,
                    hierarchyList.size(),
                    createLarder(
                        cubeDimension.cube + "." + uniqueName + ".hierarchy",
                        null,
                        null,
                        null,
                        null)
                        .populate(
                            Larders.prefix(
                                hierarchy.getLarder(),
                                dimSource,
                                cubeDimension.getName()))
                        .build());
            final MondrianDef.Hierarchy xmlHierarchy =
                validator.getXml(hierarchy, false);
            final MondrianDef.Attribute xmlAttribute =
                validator.getXml(hierarchy.attribute, false);
            hierarchyList.add(cubeHierarchy);
            initCubeHierarchy(
                cubeHierarchy,
                first(
                    xmlAttribute != null
                        ? first(
                            xmlAttribute.hierarchyAllMemberName,
                            "All " + xmlAttribute.name)
                        : xmlHierarchy != null
                        ? xmlHierarchy.allMemberName
                        : null,
                    "All " + cubeHierarchy.getName() + "s"),
                xmlAttribute != null
                    ? xmlAttribute.hierarchyAllMemberCaption
                    : xmlHierarchy != null
                    ? xmlHierarchy.allMemberCaption
                    : null);
            if (cubeHierarchy.isScenario) {
                assert cubeDimension.cube.scenarioHierarchy == null;
                cubeDimension.cube.scenarioHierarchy = cubeHierarchy;
            }
        }
        ((List) cubeDimension.getHierarchyList()).addAll(
            hierarchyList.subList(originalSize, hierarchyList.size()));
    }

    /**
     * Initializes a hierarchy within the context of a cube.
     */
    void initCubeHierarchy(
        RolapCubeHierarchy hierarchy,
        String allMemberName,
        String allMemberCaption)
    {
        assert allMemberName != null;
        hierarchy.initCubeHierarchy(
            this, allMemberName, allMemberCaption);
        for (RolapCubeLevel level : hierarchy.getLevelList()) {
            level.initLevel(this);
        }
        hierarchy.memberReader = schema.createMemberReader(hierarchy, null);
        Util.putMulti(cubeHierMap, hierarchy.getRolapHierarchy(), hierarchy);
    }

    void deferAssignDefaultMember(
        RolapCube cube,
        RolapHierarchy hierarchy,
        ElementDef xml,
        String hierarchyDefaultMember)
    {
        if (hierarchy instanceof RolapCubeHierarchy) {
            final RolapCubeHierarchy cubeHierarchy =
                (RolapCubeHierarchy) hierarchy;
            deferAssignDefaultMember(
                cube, cubeHierarchy.getRolapHierarchy(), xml,
                hierarchyDefaultMember);
        } else {
            AssignDefaultMember x;
            if (hierarchy.getDimension().isMeasures()) {
                if (hierarchyDefaultMember == null) {
                    x = new MeasureAssignDefaultMember(
                        cube, hierarchy, (MondrianDef.Cube) xml);
                } else {
                    x = new NamedMeasureAssignDefaultMember(
                        cube, hierarchy, (MondrianDef.Cube) xml);
                }
            } else if (hierarchyDefaultMember == null) {
                x = new NamelessAssignDefaultMember(cube, hierarchy, xml);
            } else {
                x = new NamedAssignDefaultMember(
                    cube, hierarchy, xml, hierarchyDefaultMember);
            }
            assignDefaultMembers.add(x);
        }
    }

    RolapAttribute createAttribute(
        MondrianDef.Attribute xmlAttribute,
        RolapSchema.PhysRelation closureRelation,
        RolapSchema.PhysRelation sourceRelation,
        final RolapDimension dimension)
    {
        if (dimension.attributeMap.get(xmlAttribute.name) != null) {
            getHandler().error(
                "Duplicate attribute '" + xmlAttribute.name + "' in dimension '"
                + dimension.getName() + "'",
                xmlAttribute,
                null);
            return null;
        }
        if (dimension.hanger) {
            final MondrianDef.Attribute a = xmlAttribute;
            if (invalidHangerAttribute(a, a.keyColumn, "keyColumn")
                || invalidHangerAttribute(a, a.nameColumn, "nameColumn")
                || invalidHangerAttribute(a, a.orderByColumn, "orderByColumn")
                || invalidHangerAttribute(a, a.captionColumn, "captionColumn")
                || invalidHangerAttribute(a, a.getKey())
                || invalidHangerAttribute(a, a.getName_())
                || invalidHangerAttribute(a, a.getOrderBy())
                || invalidHangerAttribute(a, a.getCaption()))
            {
                return null;
            }
        }
        final RolapSchema.PhysRelation relation =
            last(
                sourceRelation, xmlAttribute.table, xmlAttribute, "table");
        final List<RolapSchema.PhysColumn> keyList =
            createColumnList(
                xmlAttribute,
                "keyColumn",
                closureRelation != null
                    ? closureRelation
                    : relation,
                dimension.hanger
                    ? "c"
                    : xmlAttribute.keyColumn,
                xmlAttribute.getKey());
        if (keyList == null) {
            // there was an error
            return null;
        }
        if (keyList.size() == 0) {
            getHandler().error(
                "Attribute must have a key",
                xmlAttribute,
                null);
            return null;
        }
        final boolean nameSpecified;
        RolapSchema.PhysColumn nameExpr =
            createColumn(
                xmlAttribute,
                "nameColumn",
                closureRelation != null
                    ? sourceRelation
                    : relation,
                xmlAttribute.nameColumn,
                xmlAttribute.getName_());
        if (nameExpr == null) {
            nameSpecified = false;
            if (keyList.size() == 1) {
                nameExpr = keyList.get(0);
            } else {
                getHandler().error(
                    "Attribute name must be specified. (Name can only be "
                    + "omitted when key contains a single column.)",
                    xmlAttribute,
                    null);
                return null;
            }
        } else {
            nameSpecified = true;
        }
        RolapSchema.PhysColumn captionExpr =
            createColumn(
                xmlAttribute,
                "captionColumn",
                closureRelation != null
                    ? sourceRelation
                    : relation,
                xmlAttribute.captionColumn,
                xmlAttribute.getCaption());
        if (captionExpr == null) {
            captionExpr = nameExpr;
        }

        List<RolapSchema.PhysColumn> orderByList =
            createColumnList(
                xmlAttribute,
                "orderByColumn",
                closureRelation != null
                    ? sourceRelation
                    : relation,
                xmlAttribute.orderByColumn,
                xmlAttribute.getOrderBy());
        if (orderByList == null) {
            // there was an error; try to recover
            orderByList = Collections.emptyList();
        }
        if (orderByList.isEmpty()) {
            if (nameSpecified) {
                orderByList = Collections.singletonList(nameExpr);
            } else {
                orderByList = keyList;
            }
        }

        final int approxRowCount =
                loadApproxRowCount(xmlAttribute.approxRowCount);
            final org.olap4j.metadata.Level.Type levelType =
                stringToLevelType(
                    xmlAttribute.levelType);

        // Here we figure out a proper caption for the attribute. That is, if
        // none was supplied in the schema.
        final String caption;
        if (xmlAttribute.caption == null) {
            StringBuilder sb =
                new StringBuilder(dimension.getName());
            sb.append(" - ");
            if (xmlAttribute.name.startsWith("$")) {
                sb.append(
                    xmlAttribute.name
                        .replaceFirst("\\$", "")
                        .replaceAll("\\$", " - "));
            } else {
                sb.append(
                    xmlAttribute.name
                        .replaceAll("\\$", " - "));
            }
            caption = sb.toString();
        } else {
            caption = xmlAttribute.caption;
        }
        final RolapAttribute attribute =
            new RolapAttributeImpl(
                xmlAttribute.name,
                toBoolean(xmlAttribute.visible, true),
                keyList,
                nameExpr,
                captionExpr,
                orderByList,
                makeMemberFormatter(xmlAttribute),
                levelType,
                approxRowCount,
                createLarder(
                    Util.makeFqName(dimension, xmlAttribute.name)
                        + ".attribute",
                    xmlAttribute.getAnnotations(),
                    xmlAttribute.name,
                    caption,
                    xmlAttribute.description).build())
            {
                public RolapDimension getDimension() {
                    return dimension;
                }
            };

        validator.putXml(attribute, xmlAttribute);
        return attribute;
    }

    /** Posts error and returns true if an attribute in a hanger dimension has
     * a key column when it should not. */
    private boolean invalidHangerAttribute(
        MondrianDef.Attribute xmlAttribute,
        String keyColumn,
        String attributeName)
    {
        if (keyColumn != null) {
            getHandler().error(
                "Attribute '" + xmlAttribute.name + "' in hanger dimension "
                + "must not map to column",
                xmlAttribute,
                attributeName);
            return true;
        }
        return false;
    }

    /** Posts error and returns true if an attribute in a hanger dimension has
     * a nested element when it should not. */
    private boolean invalidHangerAttribute(
        MondrianDef.Attribute xmlAttribute,
        MondrianDef.AttributeElement xml)
    {
        if (xml != null) {
            getHandler().error(
                "Attribute '" + xmlAttribute.name + "' in hanger dimension "
                + "must not map to column",
                xmlAttribute,
                null);
            return true;
        }
        return false;
    }

    private org.olap4j.metadata.Level.Type stringToLevelType(
        String levelTypeString)
    {
        levelTypeString = Util.camelToUpper(levelTypeString);
        if (levelTypeString.equals("TIME_HALF_YEARS")) {
            levelTypeString = "TIME_HALF_YEAR";
        }
        return Util.lookup(
            org.olap4j.metadata.Level.Type.class,
            levelTypeString.toUpperCase());
    }

    public Handler getHandler() {
        return handler;
    }

    RolapLevel createLevel(
        RolapCube cube,
        RolapHierarchy hierarchy,
        RolapSchema.PhysRelation relation,
        int depth,
        MondrianDef.Level xmlLevel)
    {
        final String levelName = first(xmlLevel.name, xmlLevel.attribute);
        if (hierarchy.levelList.get(levelName) != null) {
            getHandler().error(
                MondrianResource.instance().HierarchyLevelNamesNotUnique.ex(
                    hierarchy.getUniqueName(), levelName),
                xmlLevel,
                null);
        }

        if (xmlLevel.attribute == null) {
            getHandler().error(
                "Attribute 'attribute' is required",
                xmlLevel,
                "attribute");
            return null;
        }

        final RolapDimension dimension = hierarchy.getDimension();
        RolapAttribute attribute =
            dimension.attributeMap.get(xmlLevel.attribute);
        if (attribute == null) {
            getHandler().error(
                "Attribute '" + xmlLevel.attribute
                + "' not found in Dimension '"
                + dimension.getName() + "'",
                xmlLevel,
                "attribute");
            return null;
        }

        final RolapAttribute parentAttribute;
        if (xmlLevel.parentAttribute != null) {
            parentAttribute =
                dimension.attributeMap.get(xmlLevel.parentAttribute);
            if (parentAttribute == null) {
                getHandler().error(
                    "Unknown parent attribute ''",
                    xmlLevel,
                    "parentAttribute");
            }
        } else {
            parentAttribute = null;
        }

        // Order-by columns only need to sort within parent. Therefore if this
        // is not the first level of the hierarchy maybe we can trim the list.
        List<RolapSchema.PhysColumn> orderByList =
            new ArrayList<RolapSchema.PhysColumn>(attribute.getOrderByList());
        if (!hierarchy.levelList.isEmpty()) {
            orderByList.removeAll(
                Util.last(hierarchy.levelList).getOrderByList());
        }
        if (orderByList.equals(attribute.getOrderByList())) {
            orderByList = attribute.getOrderByList(); // save some memory
        }

        final RolapClosure closure =
            createClosure(cube, relation, xmlLevel, hierarchy);

        final RolapLevel level =
            new RolapLevel(
                hierarchy,
                levelName,
                toBoolean(xmlLevel.visible, true),
                depth,
                attribute,
                parentAttribute,
                orderByList,
                xmlLevel.nullParentValue,
                closure,
                RolapLevel.HideMemberCondition.valueOf(xmlLevel.hideMemberIf),
                createLarder(
                    Util.makeFqName(hierarchy, levelName) + ".level",
                    xmlLevel.getAnnotations(),
                    xmlLevel.name,
                    xmlLevel.caption,
                    xmlLevel.description).build(),
                resourceMap);
        validator.putXml(level, xmlLevel);
        return level;
    }

    private RolapClosure createClosure(
        RolapCube cube,
        RolapSchema.PhysRelation relation,
        MondrianDef.Level xmlLevel,
        RolapHierarchy hierarchy)
    {
        final MondrianDef.Closure xmlClosure = xmlLevel.getClosure();
        if (xmlClosure == null) {
            return null;
        }
        RolapDimension dimension = hierarchy.getDimension();
        final Map<String, RolapAttribute> attributeMap = dimension.attributeMap;
        MondrianDef.Attribute xmlParentAttribute =
            (MondrianDef.Attribute) validator.map.get(
                attributeMap.get(xmlLevel.parentAttribute));
        MondrianDef.Attribute xmlAttribute =
            (MondrianDef.Attribute) validator.map.get(
                attributeMap.get(xmlLevel.attribute));

        final RolapDimension closureDim =
            new RolapDimension(
                schema,
                dimension.getName()
                    + "$" + xmlParentAttribute.name
                    + "$Parent",
                false,
                dimension.getDimensionType(),
                false,
                dimension.getLarder());
        closureDim.key =
            new Lazy<RolapSchema.PhysKey>(
                new Util.Function0<RolapSchema.PhysKey>() {
                    public RolapSchema.PhysKey apply() {
                        return lookupKey(
                            null, true, closureDim.keyAttribute);
                    }
                }
            );

        final MondrianDef.Dimension xmlClosureDimension =
            new MondrianDef.Dimension();
        xmlClosureDimension.caption = dimension.getCaption();
        xmlClosureDimension.key = "Item";
        xmlClosureDimension.name =
            dimension.getName()
            + "$" + xmlParentAttribute.name
            + "$Parent";
        xmlClosureDimension.type = null;
        xmlClosureDimension.visible = false;
        xmlClosureDimension.hanger = false;
        validator.putXml(dimension, xmlClosureDimension);

        RolapSchema.PhysRelation closureRelation =
            last(relation, xmlClosure.table, xmlClosure, "table");

        final MondrianDef.Attribute xmlClosureAttribute1 =
            new MondrianDef.Attribute();
        xmlClosureAttribute1.name = "Closure";
        xmlClosureAttribute1.approxRowCount =
            xmlParentAttribute.approxRowCount;
        xmlClosureAttribute1.caption = xmlParentAttribute.caption;
        xmlClosureAttribute1.nameColumn = xmlParentAttribute.nameColumn;
        xmlClosureAttribute1.captionColumn =
            xmlParentAttribute.captionColumn;
        xmlClosureAttribute1.orderByColumn =
            xmlParentAttribute.orderByColumn;
        xmlClosureAttribute1.hasHierarchy = false;
        xmlClosureAttribute1.visible = false;
        xmlClosureAttribute1.levelType = xmlParentAttribute.levelType;
        xmlClosureAttribute1.table = xmlClosure.table;
        xmlClosureAttribute1.keyColumn = xmlClosure.parentColumn;
        xmlClosureAttribute1.children.add(xmlParentAttribute.getName_());
        xmlClosureAttribute1.children.add(xmlParentAttribute.getCaption());
        xmlClosureAttribute1.children.add(xmlParentAttribute.getOrderBy());
        xmlClosureAttribute1.children.add(
            xmlParentAttribute.getMemberFormatter());

        final MondrianDef.Attribute xmlClosureAttribute2 =
            new MondrianDef.Attribute();
        xmlClosureAttribute2.name = "Item";
        xmlClosureAttribute2.approxRowCount = xmlAttribute.approxRowCount;
        xmlClosureAttribute2.caption = xmlAttribute.caption;
        xmlClosureAttribute2.nameColumn = null; // see Name(), below
        xmlClosureAttribute2.captionColumn = null; // see Caption(), below
        xmlClosureAttribute2.orderByColumn = null; // see OrderBy(), below
        xmlClosureAttribute2.hasHierarchy = false;
        xmlClosureAttribute2.visible = false;
        xmlClosureAttribute2.levelType = xmlAttribute.levelType;
        xmlClosureAttribute2.table = xmlClosure.table;
        xmlClosureAttribute2.keyColumn = xmlClosure.childColumn;
        xmlClosureAttribute2.children.add(
            MondrianDef.Name.of(
                xmlAttribute.nameColumn,
                xmlAttribute.table,
                xmlAttribute.getName_()));
        xmlClosureAttribute2.children.add(
            MondrianDef.Caption.of(
                xmlAttribute.captionColumn,
                xmlAttribute.table,
                xmlAttribute.getCaption()));
        xmlClosureAttribute2.children.add(
            MondrianDef.OrderBy.of(
                xmlAttribute.orderByColumn,
                xmlAttribute.table,
                xmlAttribute.getOrderBy()));
        xmlClosureAttribute2.children.add(
            xmlAttribute.getMemberFormatter());

        // Copy attributes of parent and child.
        copyAnnotations(
            xmlParentAttribute.getAnnotations(),
            xmlClosureAttribute1.children);
        copyAnnotations(
            xmlAttribute.getAnnotations(),
            xmlClosureAttribute2.children);

        // Copy properties for parent & child.
        xmlClosureAttribute1.children.addAll(
            xmlParentAttribute.getProperties());
        xmlClosureAttribute2.children.addAll(
            xmlAttribute.getProperties());

        // Now create the attributes recursively.
        final RolapAttribute closureAttribute1 =
            createAttribute(
                xmlClosureAttribute1,
                closureRelation,
                relation,
                closureDim);
        final RolapAttribute closureAttribute2 =
            createAttribute(
                xmlClosureAttribute2,
                closureRelation,
                relation,
                closureDim);

        // register the attributes in the dimension.
        closureDim.attributeMap.put(
            closureAttribute1.getName(), closureAttribute1);
        closureDim.attributeMap.put(
            closureAttribute2.getName(), closureAttribute2);
        closureDim.keyAttribute = closureAttribute2;

        // Create a hierarchy where the closure will live.
        RolapHierarchy closureHierarchy =
            new RolapHierarchy(
                closureDim,
                closureDim.getName(),
                Util.makeFqName(closureDim, closureDim.getName()),
                closureDim.isVisible(),
                true,
                hierarchy, // closureFor hierarchy
                null,
                Larders.create(
                    closureDim.getName(),
                    closureDim.getCaption(),
                    closureDim.getDescription()));
        closureDim.addHierarchy(closureHierarchy);
        deferAssignDefaultMember(
            cube, closureHierarchy, null, null);
        closureHierarchy.initHierarchy(this, null);

        // Create two levels, for parent & child each.
        final MondrianDef.Level xmlClosureLevel1 =
            new MondrianDef.Level();
        xmlClosureLevel1.attribute = closureAttribute1.getName();
        xmlClosureLevel1.name = closureAttribute1.getName();
        xmlClosureLevel1.visible = false;
        xmlClosureLevel1.hideMemberIf =
            HideMemberCondition.Never.name(); // FIXME
        final RolapLevel closureLevel1 =
            createLevel(cube, closureHierarchy, relation, 1, xmlClosureLevel1);

        final MondrianDef.Level xmlClosureLevel2 =
            new MondrianDef.Level();
        xmlClosureLevel2.attribute = closureAttribute2.getName();
        xmlClosureLevel2.parentAttribute = closureAttribute1.getName();
        xmlClosureLevel2.name = closureAttribute2.getName();
        xmlClosureLevel2.visible = false;
        xmlClosureLevel2.hideMemberIf =
            HideMemberCondition.Never.name(); // FIXME
        final RolapLevel closureLevel2 =
            createLevel(cube, closureHierarchy, relation, 2, xmlClosureLevel2);

        // Add those levels to the hierarchy.
        closureHierarchy.levelList.add(closureLevel1);
        closureHierarchy.levelList.add(closureLevel2);

        // we have to pass a column object representing the
        // 'distance' column.
        final RolapSchema.PhysColumn distanceExpr;
        if (xmlClosure.distanceColumn == null) {
            distanceExpr = null;
            getHandler().warning(
                "Distance column omitted in closure element. Mondrian will "
                + "assume that the closure table contains only tuples of "
                + "parent-childs who are direct descendants (same as distance "
                + "= 1).",
                xmlClosure,
                "distanceColumn");
        } else {
            distanceExpr =
                new RolapSchema.PhysRealColumn(
                    closureRelation,
                    xmlClosure.distanceColumn,
                    null,
                    null,
                    Integer.MIN_VALUE);
        }
        return new RolapClosure(closureLevel1, distanceExpr);
    }

    private void copyAnnotations(
        NamedList<MondrianDef.Annotation> annotations,
        MondrianDef.Children<MondrianDef.AttributeElement> children)
    {
        if (annotations.size() > 0) {
            try {
                MondrianDef.Annotations anns =
                    new MondrianDef.Annotations();
                for (MondrianDef.Annotation ann : annotations) {
                    anns.addChild(ann);
                }
                children.add(anns);
            } catch (XOMException e) {
                throw new MondrianException(e);
            }
        }
    }

    /**
     * Creates a reference to a column.
     *
     * <p>Example 1. Table inherited, column specified using attribute
     *
     * <blockquote><code>&lt;Dimension ... table="t"&gt;<br/>
     * &nbsp;&nbsp;&lt;Attribute ... column="c"/&gt;<br/>
     * &nbsp;&nbsp;...<br/>
     * &lt;/Dimension&gt;</code></blockquote>
     *
     * would result in the call <code>createColumn(xmlAttribute, "column",
     * "t", "c", null)</code>, where {@code xmlAttribute} is the Attribute XML
     * element.</p>
     *
     * <p>Example 2. Column specified using embedded element
     *
     * <blockquote><code>&lt;Dimension ... table="t"&gt;<br/>
     * &nbsp;&nbsp;&lt;Attribute .../&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;OrderBy&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Column name="c2"
     * table="t2"&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/OrderBy&gt;<br/>
     * &nbsp;&nbsp;&lt;/Attribute&gt;<br/>
     * &nbsp;&nbsp;...<br/>
     * &lt;/Dimension&gt;</code></blockquote>
     *
     * would result in the call <code>createColumn(xmlAttribute, "column",
     * "t", null, xmlOrderBy)</code>, where {@code xmlOrderBy} is the OrderBy
     * element. The inherited table "t" is supplied to the method, but is
     * overridden by the "table" attribute of the Column element, and the
     * resulting column will be "t2"."c2".</p>
     *
     * <p>Throws if the XML element is present and contains more than one
     * column. Also throws if the column name attribute and the XML element
     * are both present. If neither are present, returns null.
     *
     * @param xml XML element that the column belongs to (e.g. an Attribute
     *     element)
     * @param attributeName Name of the attribute holding the column name
     *     (often "column")
     * @param relation Relation
     * @param columnName Name of the column, from an attribute (usually the
     *     "column" attribute); may be null if the attribute is not present
     * @param xmlKey XML element describing the column. Null if that element
     *     is not present.
     *
     * @return Column
     */
    private RolapSchema.PhysColumn createColumn(
        ElementDef xml,
        String attributeName,
        RolapSchema.PhysRelation relation,
        String columnName,
        MondrianDef.Columns xmlKey)
    {
        final List<RolapSchema.PhysColumn> list =
            createColumnList(
                xml, attributeName, relation, columnName, xmlKey);
        if (list == null) {
            return null;
        }
        switch (list.size()) {
        case 0:
            return null;
        case 1:
            return list.get(0);
        default:
            getHandler().error(
                "Only one column allowed",
                xmlKey,
                null);
            return list.get(0);
        }
    }

    /** Converts an XML element or attribute into a list of columns.
     * If there is an error, logs it and returns null. */
    private List<RolapSchema.PhysColumn> createColumnList(
        ElementDef xml,
        String attributeName,
        RolapSchema.PhysRelation table,
        String columnName,
        MondrianDef.Columns xmlColumns)
    {
        if (columnName != null) {
            if (xmlColumns != null) {
                // Post an error, and continue, ignoring xmlKey.
                getHandler().error(
                    "must not specify both "
                    + attributeName
                    + " and "
                    + xmlColumns.getName(),
                    xmlColumns,
                    null);
            }
            if (table == null) {
                getHandler().error(
                    "table must be specified",
                    xml,
                    attributeName);
                return Collections.emptyList();
            }
            RolapSchema.PhysColumn column = table.getColumn(columnName, false);
            if (column == null) {
                getHandler().error(
                    "Column '" + columnName + "' not found in relation '"
                    + table.getAlias() + "'",
                    xml,
                    attributeName);
                return null;
            }
            return Collections.singletonList(column);
        }
        final List<RolapSchema.PhysColumn> list =
            new ArrayList<RolapSchema.PhysColumn>();
        if (xmlColumns != null) {
            for (MondrianDef.Column xmlColumn : xmlColumns.array) {
                RolapSchema.PhysColumn column =
                    getPhysColumn(
                        last(table, xmlColumn.table, xmlColumn, "table"),
                        xmlColumn.name,
                        xmlColumn,
                        "name");
                if (column == null) {
                    return null;
                }
                list.add(column);
            }
        }
        return list;
    }

    /**
     * Gets a column. If the column does not exist, posts an error (it is a
     * user error) and returns null.
     *
     * @param relation Relation
     * @param columnName Name of column
     * @param xmlColumn XML column
     * @param attributeName XML attribute that holds name of column
     * @return Column, or null if does not exist
     */
    RolapSchema.PhysColumn getPhysColumn(
        RolapSchema.PhysRelation relation,
        String columnName,
        ElementDef xmlColumn,
        String attributeName)
    {
        if (relation == null) {
            getHandler().error(
                "Table required. No table is specified or inherited when "
                + "resolving column '" + columnName + "'",
                xmlColumn,
                attributeName);
            return null;
        }
        final RolapSchema.PhysColumn column =
            relation.getColumn(columnName, false);
        if (column == null) {
            getHandler().error(
                "Column '" + columnName + "' not found in relation '"
                + relation + "'",
                xmlColumn,
                attributeName);
        }
        return column;
    }

    private RolapSchema.PhysRelation getPhysRelation(
        String tableName, NodeDef xml, String xmlAttr)
    {
        if (tableName == null) {
            getHandler().error(
                "table must be specified",
                xml,
                xmlAttr);
        }
        final RolapSchema.PhysRelation relation =
            physSchemaBuilder.getPhysRelation(tableName, false);
        if (relation == null) {
            getHandler().error(
                "table '" + tableName + "' not found",
                xml,
                xmlAttr);
        }
        return relation;
    }

    RolapSchema.PhysRelation last(
        RolapSchema.PhysRelation s0,
        String tableName,
        NodeDef xmlTable,
        String xmlAttribute)
    {
        if (tableName != null) {
            return getPhysRelation(tableName, xmlTable, xmlAttribute);
        }
        return s0;
    }

    private void createProperties(
        PhysSchemaBuilder physSchemaBuilder,
        RolapDimension dimension,
        RolapAttribute attribute,
        RolapSchema.PhysRelation relation,
        MondrianDef.Attribute xmlAttribute,
        List<RolapProperty> propertyList)
    {
        // Add defined properties. These are little more than usages of
        // attributes.
        for (MondrianDef.Property xmlProperty : xmlAttribute.getProperties()) {
            final RolapProperty property = createProperty(
                dimension, attribute, xmlProperty);
            if (property == null) {
                return;
            }
            validator.putXml(property, xmlProperty);
            propertyList.add(property);
        }
    }

    private RolapProperty createProperty(
        RolapDimension dimension,
        RolapAttribute attribute,
        MondrianDef.Property xmlProperty)
    {
        RolapAttribute sourceAttribute =
            dimension.attributeMap.get(xmlProperty.attribute);
        if (sourceAttribute == null) {
            handler.error(
                "Unknown attribute '" + xmlProperty.attribute + "'",
                xmlProperty,
                "attribute");
            return null;
        }
        final String name = first(xmlProperty.name, xmlProperty.attribute);
        return new RolapProperty(
            name,
            attribute,
            sourceAttribute,
            dialectToPropertyDatatype(sourceAttribute.getDatatype()),
            makePropertyFormatter(xmlProperty),
            false,
            createLarder(
                Util.makeFqName(dimension, attribute.getName()) + ".property",
                xmlProperty.getAnnotations(),
                xmlProperty.name,
                xmlProperty.caption,
                xmlProperty.description).build());
    }

    private static Property.Datatype dialectToPropertyDatatype(
        Dialect.Datatype type)
    {
        Util.deprecated("obsolete Property.Datatype and this method", false);
        switch (type) {
        case Boolean:
            return Property.Datatype.TYPE_BOOLEAN;
        case Date:
        case Time:
        case Timestamp:
            return Property.Datatype.TYPE_TIMESTAMP;
        case Integer:
        case Numeric:
            return Property.Datatype.TYPE_NUMERIC;
        case String:
            return Property.Datatype.TYPE_STRING;
        default:
            throw Util.unexpected(type);
        }
    }

    private static PropertyFormatter makePropertyFormatter(
        MondrianDef.Property xmlProperty)
    {
        final String propertyFormatterClassName;
        final Scripts.ScriptDefinition scriptDefinition;
        if (xmlProperty.getFormatter() != null) {
            propertyFormatterClassName =
                xmlProperty.getFormatter().className;
            scriptDefinition =
                toScriptDef(xmlProperty.getFormatter().script);
        } else {
            propertyFormatterClassName = xmlProperty.formatter;
            scriptDefinition = null;
        }
        if (propertyFormatterClassName == null && scriptDefinition == null) {
            return null;
        }
        try {
            return getFormatter(
                propertyFormatterClassName,
                PropertyFormatter.class,
                scriptDefinition);
        } catch (Exception e) {
            throw MondrianResource.instance()
                .PropertyFormatterLoadFailed.ex(
                    propertyFormatterClassName, xmlProperty.name, e);
        }
    }

    private static MemberFormatter makeMemberFormatter(
        MondrianDef.Attribute xmlAttribute)
    {
        final MondrianDef.MemberFormatter xmlMemberFormatter =
            xmlAttribute.getMemberFormatter();
        if (xmlMemberFormatter == null) {
            return null;
        }
        final String memberFormatterClassName = xmlMemberFormatter.className;
        final Scripts.ScriptDefinition scriptDefinition =
            toScriptDef(xmlMemberFormatter.script);
        if (memberFormatterClassName == null && scriptDefinition == null) {
            return null;
        }
        try {
            return getFormatter(
                memberFormatterClassName,
                MemberFormatter.class,
                scriptDefinition);
        } catch (Exception e) {
            throw MondrianResource.instance().MemberFormatterLoadFailed
                .ex(
                    memberFormatterClassName, xmlAttribute.name, e);
        }
    }

    private static CellFormatter makeCellFormatter(
        MondrianDef.Measure xmlMeasure, String measureUniqueName)
    {
        final MondrianDef.CellFormatter xmlCellFormatter =
            xmlMeasure.getCellFormatter();
        final String cellFormatterClassName;
        final Scripts.ScriptDefinition scriptDefinition;
        if (xmlCellFormatter != null) {
            cellFormatterClassName = xmlCellFormatter.className;
            scriptDefinition = toScriptDef(xmlCellFormatter.script);
        } else {
            cellFormatterClassName = xmlMeasure.formatter;
            scriptDefinition = null;
        }
        if (cellFormatterClassName == null && scriptDefinition == null) {
            return null;
        }
        try {
            return getFormatter(
                cellFormatterClassName,
                CellFormatter.class,
                scriptDefinition);
        } catch (Exception e) {
            throw MondrianResource.instance().CellFormatterLoadFailed.ex(
                cellFormatterClassName, measureUniqueName, e);
        }
    }

    /**
     * Creates RolapSchema given the MD5 hash, catalog name and string (content)
     * and the connectInfo object.
     *
     * @param md5Bytes may be null
     * @param catalogUrl URL of catalog
     * @param catalogStr may be null
     * @param connectInfo Connection properties
     */
    static RolapSchema createSchema(
        final SchemaKey key,
        final ByteString md5Bytes,
        final String catalogUrl,
        final String catalogStr,
        final Util.PropertyList connectInfo,
        final DataSource dataSource)
    {
        final RolapSchemaLoader schemaLoader = new RolapSchemaLoader(null);
        RolapSchema x = schemaLoader.loadStage0(
            key, md5Bytes, catalogUrl, catalogStr, connectInfo, dataSource);
        schemaLoader.handler.check();
        return x;
    }

    private NamedSet createNamedSet(
        RolapCube cube,
        MondrianDef.NamedSet xmlNamedSet)
    {
        final String formulaString = formula(xmlNamedSet);
        final Exp exp;
        try {
            exp = schema.getInternalConnection().parseExpression(formulaString);
        } catch (Exception e) {
            throw MondrianResource.instance().NamedSetHasBadFormula.ex(
                xmlNamedSet.name, e);
        }
        final Formula formula =
            new Formula(
                new Id(
                    new Id.NameSegment(
                        xmlNamedSet.name,
                        Id.Quoting.UNQUOTED)),
                exp);
        return formula.getNamedSet();
    }

    /** Creates a role after a schema has been created. */
    Pair<RolapSchema.RoleFactory, String> createRole(String role) {
        final Parser xmlParser;
        try {
            xmlParser = XOMUtil.createDefaultParser();
            xmlParser.setKeepPositions(true);
            final DOMWrapper def = xmlParser.parse(role);
            final MondrianDef.Role xmlRole = new MondrianDef.Role(def);
            final RolapSchema.RoleFactory roleFactory =
                createRole(xmlRole, schema.mapNameToRole);
            return Pair.of(roleFactory, xmlRole.name);
        } catch (XOMException e) {
            // TODO: better error
            throw new RuntimeException(
                "while creating role from [" + role + "]", e);
        }
    }

    RolapSchema.RoleFactory createRole(
        MondrianDef.Role xmlRole,
        Map<String, RolapSchema.RoleFactory> mapNameToRole)
    {
        final int count =
            (xmlRole.getUnion() == null ? 0 : 1)
            + (xmlRole.getSchemaGrants().isEmpty() ? 0 : 1)
            + (xmlRole.className == null ? 0 : 1)
            + (xmlRole.getScript() == null ? 0 : 1);
        if (count != 1) {
            // TODO: better message, localized, with location
            if (false) {
                throw new AssertionError(
                    "Role must have precisely one of: className attribute, or "
                    + "Script, SchemaGrant, Union child elements");
            }
            throw MondrianResource.instance().RoleUnionGrants.ex();
        }
        if (xmlRole.getUnion() != null) {
            return createUnionRole(mapNameToRole, xmlRole.getUnion());
        } else if (xmlRole.getScript() != null) {
            return createScriptRole(xmlRole.getScript());
        } else if (xmlRole.className != null) {
            return createClassRole(xmlRole.className);
        } else {
            return createGrantRole(xmlRole.getSchemaGrants());
        }
    }

    private RolapSchema.RoleFactory createClassRole(String className) {
        // Lookup class.
        try {
            final Class<?> clazz =
                ClassResolver.INSTANCE.forName(className, true);
            Object o = clazz.newInstance();

            if (o instanceof Role) {
                return new RolapSchema.ConstantRoleFactory((Role) o);
            }
            if (o instanceof RoleGenerator) {
                return schema.new GeneratingRoleFactory((RoleGenerator) o);
            }
            // TODO: better message
            Util.discard(Bug.BugMondrian1281Fixed);
            throw new RuntimeException("neither role nor role generator");
        } catch (ClassNotFoundException e) {
            // TODO: better error
            Util.discard(Bug.BugMondrian1281Fixed);
            throw new RuntimeException("while creating role", e);
        } catch (InstantiationException e) {
            // TODO: better error
            Util.discard(Bug.BugMondrian1281Fixed);
            throw new RuntimeException("while creating role", e);
        } catch (IllegalAccessException e) {
            // TODO: better error
            Util.discard(Bug.BugMondrian1281Fixed);
            throw new RuntimeException("while creating role", e);
        }
    }

    private RolapSchema.RoleFactory createScriptRole(
        MondrianDef.Script xmlScript)
    {
        Scripts.ScriptDefinition script = toScriptDef(xmlScript);
        RoleGenerator roleGenerator = Scripts.roleGenerator(script);
        return schema.new GeneratingRoleFactory(roleGenerator);
    }

    private RolapSchema.RoleFactory createGrantRole(
        List<MondrianDef.SchemaGrant> xmlSchemaGrants)
    {
        RoleImpl role = new RoleImpl();
        for (MondrianDef.SchemaGrant schemaGrant : xmlSchemaGrants) {
            role.grant(schema, getAccess(schemaGrant.access, schemaAllowed));
            for (MondrianDef.CubeGrant cubeGrant : schemaGrant.cubeGrants) {
                RolapCube cube = schema.lookupCube(cubeGrant.cube);
                if (cube == null) {
                    throw Util.newError(
                        "Unknown cube '" + cubeGrant.cube + "'");
                }
                role.grant(cube, getAccess(cubeGrant.access, cubeAllowed));
                final SchemaReader schemaReader = cube.getSchemaReader(null);
                for (MondrianDef.DimensionGrant dimensionGrant
                    : cubeGrant.dimensionGrants)
                {
                    Dimension dimension = (Dimension)
                        schemaReader.lookupCompound(
                            cube,
                            Util.parseIdentifier(dimensionGrant.dimension),
                            true,
                            Category.Dimension);
                    role.grant(
                        dimension,
                        getAccess(dimensionGrant.access, dimensionAllowed));
                }
                for (MondrianDef.HierarchyGrant hierarchyGrant
                    : cubeGrant.hierarchyGrants)
                {
                    Hierarchy hierarchy = (Hierarchy)
                        schemaReader.lookupCompound(
                            cube,
                            Util.parseIdentifier(hierarchyGrant.hierarchy),
                            true,
                            Category.Hierarchy);
                    final Access hierarchyAccess =
                        getAccess(hierarchyGrant.access, hierarchyAllowed);
                    Level topLevel = null;
                    if (hierarchyGrant.topLevel != null) {
                        if (hierarchyAccess != Access.CUSTOM) {
                            throw Util.newError(
                                "You may only specify 'topLevel' if "
                                + "access='custom'");
                        }
                        topLevel = (Level) schemaReader.lookupCompound(
                            cube,
                            Util.parseIdentifier(hierarchyGrant.topLevel),
                            true,
                            Category.Level);
                    }
                    Level bottomLevel = null;
                    if (hierarchyGrant.bottomLevel != null) {
                        if (hierarchyAccess != Access.CUSTOM) {
                            throw Util.newError(
                                "You may only specify 'bottomLevel' if "
                                + "access='custom'");
                        }
                        bottomLevel = (Level) schemaReader.lookupCompound(
                            cube,
                            Util.parseIdentifier(hierarchyGrant.bottomLevel),
                            true,
                            Category.Level);
                    }
                    Role.RollupPolicy rollupPolicy;
                    if (hierarchyGrant.rollupPolicy != null) {
                        try {
                            rollupPolicy =
                                Role.RollupPolicy.valueOf(
                                    hierarchyGrant.rollupPolicy.toUpperCase());
                        } catch (IllegalArgumentException e) {
                            throw Util.newError(
                                "Illegal rollupPolicy value '"
                                + hierarchyGrant.rollupPolicy
                                + "'");
                        }
                    } else {
                        rollupPolicy = Role.RollupPolicy.FULL;
                    }
                    role.grant(
                        hierarchy, hierarchyAccess, topLevel, bottomLevel,
                        rollupPolicy);
                    for (MondrianDef.MemberGrant memberGrant
                        : hierarchyGrant.memberGrants)
                    {
                        if (hierarchyAccess != Access.CUSTOM) {
                            throw Util.newError(
                                "You may only specify <MemberGrant> if "
                                + "<Hierarchy> has access='custom'");
                        }
                        final boolean ignoreInvalidMembers =
                            MondrianProperties.instance().IgnoreInvalidMembers
                                .get();
                        Member member =
                            schemaReader.withLocus().getMemberByUniqueName(
                                Util.parseIdentifier(memberGrant.member),
                                !ignoreInvalidMembers);
                        if (member == null) {
                            // They asked to ignore members that don't exist
                            // (e.g. [Store].[USA].[Foo]), so ignore this grant
                            // too.
                            assert ignoreInvalidMembers;
                            continue;
                        }
                        if (member.getHierarchy() != hierarchy) {
                            throw Util.newError(
                                "Member '" + member
                                + "' is not in hierarchy '" + hierarchy + "'");
                        }
                        role.grant(
                            member,
                            getAccess(memberGrant.access, memberAllowed));
                    }
                }
            }
        }
        role.makeImmutable();
        return new RolapSchema.ConstantRoleFactory(role);
    }

    private RolapSchema.RoleFactory createUnionRole(
        Map<String, RolapSchema.RoleFactory> mapNameToRole,
        MondrianDef.Union xmlUnion)
    {
        List<RolapSchema.RoleFactory> roleList =
            new ArrayList<RolapSchema.RoleFactory>();
        for (MondrianDef.RoleUsage roleUsage : xmlUnion.list()) {
            final RolapSchema.RoleFactory role =
                mapNameToRole.get(roleUsage.roleName);
            if (role == DUMMY_ROLE) {
                // dependency not resolved yet
                return DUMMY_ROLE;
            }
            if (role == null) {
                throw MondrianResource.instance().UnknownRole.ex(
                    roleUsage.roleName);
            }
            roleList.add(role);
        }
        return new RolapSchema.UnionRoleFactory(roleList);
    }

    private static Access getAccess(String accessString, Set<Access> allowed) {
        final Access access = Access.valueOf(accessString.toUpperCase());
        if (allowed.contains(access)) {
            return access; // value is ok
        }
        throw Util.newError("Bad value access='" + accessString + "'");
    }

    static int loadApproxRowCount(String approxRowCount) {
        boolean notNullAndNumeric =
            approxRowCount != null
                && approxRowCount.matches("^\\d+$");
        if (notNullAndNumeric) {
            return Integer.parseInt(approxRowCount);
        } else {
            // if approxRowCount is not set, return MIN_VALUE to indicate
            return Integer.MIN_VALUE;
        }
    }

    public static Map<String, String> buildHintMap(
        List<MondrianDef.Hint> hints)
    {
        if (hints == null || hints.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> hintMap = new HashMap<String, String>();
        for (MondrianDef.Hint hint : hints) {
            hintMap.put(hint.type, hint.cdata);
        }
        return hintMap;
    }

    public Cube createCube(String xml) {
        // NOTE: This method is only lightly tested. I'm not sure that it works,
        // particularly in the presence of gnarly things like shared dimensions
        // and named sets. The synthetic physical schema is probably not the
        RolapCube cube;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            xmlParser.setKeepPositions(true);
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("Cube")) {
                // Create empty XML schema, to keep the method happy. This is
                // okay, because there are no forward-references to resolve.
                final MondrianDef.Schema xmlSchema = new MondrianDef.Schema();
                MondrianDef.Cube xmlCube = new MondrianDef.Cube(def);
                cube =
                    new RolapCube(
                        this,
                        xmlCube.name,
                        toBoolean(xmlCube.visible, true),
                        createLarder(
                            Util.quoteMdxIdentifier(xmlCube.name) + ".cube",
                            xmlCube.getAnnotations(),
                            xmlCube.name,
                            xmlCube.caption,
                            xmlCube.description).build(),
                        xmlSchema.measuresCaption);
                validator.putXml(cube, xmlCube);
            } else {
                throw new XOMException(
                    "Got <" + tagName + "> when expecting <Cube>");
            }
        } catch (XOMException e) {
            throw Util.newError(
                e,
                "Error while creating cube from XML [" + xml + "]");
        }
        return cube;
    }

    /**
     * Adds a collection of calculated members and named sets to this cube.
     * The members and sets can refer to each other.
     *
     * @param xmlCalcMembers XML objects representing members
     * @param xmlNamedSets Array of XML definition of named set
     * @param memberList Output list of {@link mondrian.olap.Member} objects
     * @param calculatedMemberList Output list of {@link mondrian.olap.Formula}
     *   objects representing calculated members
     * @param namedSetList Output list of {@link mondrian.olap.Formula} objects
     *   representing named sets
     * @param cube the cube that the calculated members originate from
     * @param errOnDups throws an error if a duplicate member is found
     */
    void createCalcMembersAndNamedSets(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        List<MondrianDef.NamedSet> xmlNamedSets,
        List<RolapMember> memberList,
        List<Formula> calculatedMemberList,
        List<Formula> namedSetList,
        RolapCube cube,
        boolean errOnDups)
    {
        final Query queryExp =
            resolveCalcMembers(
                xmlCalcMembers, xmlNamedSets, cube, errOnDups);
        if (queryExp == null) {
            return;
        }

        // Now pick through the formulas.
        Util.assertTrue(
            queryExp.getFormulas().length
            == xmlCalcMembers.size() + xmlNamedSets.size());
        for (int i = 0; i < xmlCalcMembers.size(); i++) {
            postCalcMember(
                cube,
                xmlCalcMembers, calculatedMemberList, i, queryExp, memberList);
        }
        for (int i = 0; i < xmlNamedSets.size(); i++) {
            postNamedSet(
                cube,
                xmlNamedSets, xmlCalcMembers.size(), i, queryExp, namedSetList);
        }
    }

    private Query resolveCalcMembers(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        List<MondrianDef.NamedSet> xmlNamedSets,
        RolapCube cube,
        boolean errOnDups)
    {
        // If there are no objects to create, our generated SQL will be so
        // silly, the parser will laugh.
        if (xmlCalcMembers.size() == 0 && xmlNamedSets.size() == 0) {
            return null;
        }

        final List<Formula> calculatedMemberList = cube.calculatedMemberList;

        StringBuilder buf = new StringBuilder(256);
        buf.append("WITH").append(Util.nl);

        // Check the members individually, and generate SQL.
        final Set<String> fqNames = new LinkedHashSet<String>();
        for (int i = 0; i < xmlCalcMembers.size(); i++) {
            preCalcMember(
                xmlCalcMembers, calculatedMemberList, i, buf, cube, errOnDups,
                fqNames);
        }

        // Check the named sets individually (for uniqueness) and generate SQL.
        Set<String> nameSet = new HashSet<String>();
        for (Formula namedSet : cube.namedSetList) {
            nameSet.add(namedSet.getName());
        }
        for (MondrianDef.NamedSet xmlNamedSet : xmlNamedSets) {
            preNamedSet(xmlNamedSet, cube, nameSet, buf);
        }

        buf.append("SELECT FROM ").append(cube.getUniqueName());

        // Parse and validate this huge MDX query we've created.
        final String queryString = buf.toString();
        final Query queryExp;
        try {
            final RolapConnection conn = schema.getInternalConnection();
            return Locus.execute(
                conn,
                "Validate calculated members in cube",
                new Locus.Action<Query>() {
                    public Query execute() {
                        final Query queryExp = conn.parseQuery(queryString);
                        queryExp.resolve();
                        return queryExp;
                    }
                }
            );
        } catch (Exception e) {
            throw MondrianResource.instance().UnknownNamedSetHasBadFormula.ex(
                cube.getName(), e);
        }
    }

    private void postNamedSet(
        RolapCube cube,
        List<MondrianDef.NamedSet> xmlNamedSets,
        final int offset,
        int i,
        final Query queryExp,
        List<Formula> formulaList)
    {
        MondrianDef.NamedSet xmlNamedSet = xmlNamedSets.get(i);
        Util.discard(xmlNamedSet);
        Formula formula = queryExp.getFormulas()[offset + i];
        final SetBase namedSet = (SetBase) formula.getNamedSet();
        namedSet.setLarder(
            createLarder(
                cube + "." + namedSet.getUniqueName() + ".set",
                xmlNamedSet.getAnnotations(),
                xmlNamedSet.name,
                emptyNull(xmlNamedSet.caption),
                emptyNull(xmlNamedSet.description)).build());

        formulaList.add(formula);
    }

    private String emptyNull(String s) {
        if ("".equals(s)) {
            return null;
        } else {
            return s;
        }
    }

    private void preNamedSet(
        MondrianDef.NamedSet xmlNamedSet,
        RolapCube cube,
        Set<String> nameSet,
        StringBuilder buf)
    {
        if (!nameSet.add(xmlNamedSet.name)) {
            throw MondrianResource.instance().NamedSetNotUnique.ex(
                xmlNamedSet.name, cube.getName());
        }

        buf.append("SET ")
            .append(Util.makeFqName(xmlNamedSet.name))
            .append(Util.nl)
            .append(" AS ");
        String result = formula(xmlNamedSet);
        Util.singleQuoteString(result, buf);
        buf.append(Util.nl);
    }

    private static String formula(MondrianDef.NamedSet xmlNamedSet) {
        final MondrianDef.Formula formula = xmlNamedSet.getFormula();
        if (formula != null) {
            return formula.cdata;
        } else {
            return xmlNamedSet.formula;
        }
    }

    private void postCalcMember(
        RolapCube cube,
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        List<Formula> calculatedMemberList,
        int i,
        final Query queryExp,
        List<RolapMember> memberList)
    {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers.get(i);
        final Formula formula = queryExp.getFormulas()[i];
        calculatedMemberList.add(formula);

        final RolapMember member = (RolapMember) formula.getMdxMember();
        ((RolapCalculatedMember) member).setLarder(
            Larders.LarderBuilder.of(member.getLarder())
                .addAll(xmlCalcMember.getAnnotations())
                .caption(second(xmlCalcMember.name, xmlCalcMember.caption))
                .description(emptyNull(xmlCalcMember.description))
                .addAll(resourceMap.get(cube + "." + member + ".member"))
                .add(Property.VISIBLE, toBoolean(xmlCalcMember.visible, true))
                .build());

        memberList.add(member);
    }

    private String second(String s1, String s2) {
        if (s2 == null || s2.equals("") || s2.equals(s1)) {
            return null;
        }
        return s2;
    }

    private void preCalcMember(
        List<MondrianDef.CalculatedMember> xmlCalcMembers,
        List<Formula> calculatedMemberList,
        int j,
        StringBuilder buf,
        RolapCube cube,
        boolean errOnDup,
        Set<String> fqNames)
    {
        MondrianDef.CalculatedMember xmlCalcMember = xmlCalcMembers.get(j);

        // Lookup hierarchy
        Hierarchy hierarchy = null;
        String dimName = null;
        if (xmlCalcMember.hierarchy != null
            && xmlCalcMember.dimension != null)
        {
            throw MondrianResource.instance()
                .CalcMemberHasBothDimensionAndHierarchy.ex(
                    xmlCalcMember.name, cube.getName());
        } else if (xmlCalcMember.dimension != null) {
            dimName = xmlCalcMember.dimension;
            Dimension dimension = (Dimension)
                cube.getSchemaReader().withLocus().lookupCompound(
                    cube,
                    Util.parseIdentifier(dimName),
                    false,
                    Category.Dimension);
            if (dimension == null) {
                dimension =
                    cube.lookupDimension(
                        new Id.NameSegment(
                            xmlCalcMember.dimension,
                            Id.Quoting.UNQUOTED));
            }
            if (dimension != null) {
                hierarchy = dimension.getHierarchy();
            }
        } else if (xmlCalcMember.hierarchy != null) {
            dimName = xmlCalcMember.hierarchy;
            hierarchy = (Hierarchy)
                cube.getSchemaReader().withLocus().lookupCompound(
                    cube,
                    Util.parseIdentifier(dimName),
                    false,
                    Category.Hierarchy);
        }
        if (hierarchy == null) {
            throw MondrianResource.instance().CalcMemberHasBadDimension.ex(
                dimName, xmlCalcMember.name, cube.getName());
        }

        // Root of fully-qualified name.
        String parentFqName;
        if (xmlCalcMember.parent != null) {
            parentFqName = xmlCalcMember.parent;
        } else {
            parentFqName = hierarchy.getUniqueNameSsas();
        }

        if (!hierarchy.getDimension().isMeasures()) {
            // Check if the parent exists.
            final OlapElement parent =
                new NameResolver().resolve(
                    cube,
                    Util.toOlap4j(
                        Util.parseIdentifier(parentFqName)),
                    false,
                    Category.Unknown,
                    MatchType.EXACT,
                    cube.getSchemaReader().getNamespaces());

            if (parent == null) {
                throw MondrianResource.instance()
                    .CalcMemberHasUnknownParent.ex(
                        parentFqName, xmlCalcMember.name, cube.getName());
            }

            if (parent.getHierarchy() != hierarchy) {
                throw MondrianResource.instance()
                    .CalcMemberHasDifferentParentAndHierarchy.ex(
                        xmlCalcMember.name,
                        cube.getName(),
                        hierarchy.getUniqueName());
            }
        }

        // If we're processing a virtual cube, it's possible that we've
        // already processed this calculated member because it's
        // referenced in another measure; in that case, remove it from the
        // list, since we'll add it back in later; otherwise, in the
        // non-virtual cube case, throw an exception
        final String fqName = Util.makeFqName(parentFqName, xmlCalcMember.name);
        for (int i = 0; i < calculatedMemberList.size(); i++) {
            Formula formula = calculatedMemberList.get(i);
            if (formula.getName().equals(xmlCalcMember.name)
                && formula.getMdxMember().getHierarchy().equals(
                    hierarchy))
            {
                if (errOnDup) {
                    throw MondrianResource.instance().CalcMemberNotUnique.ex(
                        fqName,
                        cube.getName());
                } else {
                    calculatedMemberList.remove(i);
                    --i;
                }
            }
        }

        // Check this calc member doesn't clash with one earlier in this
        // batch.
        if (!fqNames.add(fqName)) {
            throw MondrianResource.instance().CalcMemberNotUnique.ex(
                fqName,
                cube.getName());
        }

        final List<MondrianDef.CalculatedMemberProperty> xmlProperties =
            xmlCalcMember.getCalculatedMemberProperties();
        List<String> propNames = new ArrayList<String>();
        List<String> propExprs = new ArrayList<String>();
        validateMemberProps(
            cube, xmlProperties, propNames, propExprs, xmlCalcMember.name);

        final int measureCount = cube.getMeasures().size();

        // Get formula.
        String result = getFormula(xmlCalcMember, fqName);
        if (result == null) {
            result = "1"; // use dummy formula so we can carry on
        }

        // Generate SQL.
        assert fqName.startsWith("[");
        buf.append("MEMBER ")
            .append(fqName)
            .append(Util.nl)
            .append("  AS ");
        Util.singleQuoteString(result, buf);

        final MondrianDef.CellFormatter xmlCellFormatter =
            xmlCalcMember.getCellFormatter();
        if (xmlCellFormatter != null) {
            if (xmlCellFormatter.className != null) {
                propNames.add(Property.CELL_FORMATTER.name);
                propExprs.add(
                    Util.quoteForMdx(xmlCellFormatter.className));
            }
            if (xmlCellFormatter.script != null) {
                if (xmlCellFormatter.script.language != null) {
                    propNames.add(Property.CELL_FORMATTER_SCRIPT_LANGUAGE.name);
                    propExprs.add(
                        Util.quoteForMdx(
                            xmlCellFormatter.script.language));
                }
                propNames.add(Property.CELL_FORMATTER_SCRIPT.name);
                propExprs.add(
                    Util.quoteForMdx(xmlCellFormatter.script.cdata));
            }
        }

        assert propNames.size() == propExprs.size();
        processFormatStringAttribute(xmlCalcMember, buf);

        for (int i = 0; i < propNames.size(); i++) {
            String name = propNames.get(i);
            String expr = propExprs.get(i);
            buf.append(",").append(Util.nl);
            expr = removeSurroundingQuotesIfNumericProperty(name, expr);
            buf.append(name).append(" = ").append(expr);
        }
        // Flag that the calc members are defined against a cube; will
        // determine the value of Member.isCalculatedInQuery
        buf.append(",")
            .append(Util.nl);
        Util.quoteMdxIdentifier(Property.MEMBER_SCOPE.name, buf);
        buf.append(" = 'CUBE'");

        // Assign the member an ordinal higher than all of the stored measures.
        if (!propNames.contains(Property.MEMBER_ORDINAL.getName())) {
            buf.append(",")
                .append(Util.nl)
                .append(Property.MEMBER_ORDINAL)
                .append(" = ")
                .append(measureCount + j);
        }
        buf.append(Util.nl);
    }

    /** Gets formula of a {@link CalculatedMember} element. On error, posts and
     * returns null. */
    private String getFormula(
        MondrianDef.CalculatedMember xmlCalcMember, String fqName)
    {
        String result1;
        if (xmlCalcMember.getFormula() != null) {
            result1 = xmlCalcMember.getFormula().cdata;
        } else {
            result1 = null;
        }
        String result2 = xmlCalcMember.formula;
        if (result1 != null && result2 != null) {
            handler.error(
                "Must not specify both 'formula' attribute and 'Formula' child element; ignoring member '"
                + fqName + "'",
                xmlCalcMember,
                "formula");
            return null;
        }
        if (result1 == null && result2 == null) {
            handler.error(
                "Must specify either 'formula' attribute or 'Formula' child element; ignoring member '"
                + fqName + "'",
                xmlCalcMember,
                "formula");
            return null;
        }
        return first(result1, result2);
    }

    private String removeSurroundingQuotesIfNumericProperty(
        String name,
        String expr)
    {
        Property prop = Property.lookup(name, false);
        if (prop != null
            && prop.getType() == Property.Datatype.TYPE_NUMERIC
            && isSurroundedWithQuotes(expr)
            && expr.length() > 2)
        {
            return expr.substring(1, expr.length() - 1);
        }
        return expr;
    }

    private boolean isSurroundedWithQuotes(String expr) {
        return expr.startsWith("\"") && expr.endsWith("\"");
    }

    void processFormatStringAttribute(
        MondrianDef.CalculatedMember xmlCalcMember,
        StringBuilder buf)
    {
        if (xmlCalcMember.formatString != null) {
            buf.append(",")
                .append(Util.nl)
                .append(Property.FORMAT_STRING.name)
                .append(" = ")
                .append(Util.quoteForMdx(xmlCalcMember.formatString));
        }
    }

    /**
     * Validates a list of member properties, and populates a list of names
     * and expressions, one for each property.
     *
     * @param xmlProperties Array of property definitions.
     * @param propNames Output array of property names.
     * @param propExprs Output array of property expressions.
     * @param memberName Name of member which the properties belong to.
     */
    private void validateMemberProps(
        RolapCube cube,
        final List<MondrianDef.CalculatedMemberProperty> xmlProperties,
        List<String> propNames,
        List<String> propExprs,
        String memberName)
    {
        if (xmlProperties == null) {
            return;
        }
        for (MondrianDef.CalculatedMemberProperty xmlProperty : xmlProperties) {
            if (xmlProperty.expression == null && xmlProperty.value == null) {
                throw MondrianResource.instance()
                    .NeitherExprNorValueForCalcMemberProperty.ex(
                        xmlProperty.name, memberName, cube.getName());
            }
            if (xmlProperty.expression != null && xmlProperty.value != null) {
                throw MondrianResource.instance().ExprAndValueForMemberProperty
                    .ex(
                        xmlProperty.name, memberName, cube.getName());
            }
            propNames.add(xmlProperty.name);
            if (xmlProperty.expression != null) {
                propExprs.add(xmlProperty.expression);
            } else {
                propExprs.add(Util.quoteForMdx(xmlProperty.value));
            }
        }
    }

    /**
     * Given the name of a formatter class and/or a formatter script,
     * returns a formatter.
     *
     * <p>The formatter is either a MemberFormatter, CellFormatter or
     * PropertyFormatter.</p>
     *
     * @param className Name of cell formatter class
     * @param iface Formatter class
     * @param script Script
     * @return Cell formatter
     * @throws Exception if class cannot be instantiated
     */
    static <T> T getFormatter(
        String className,
        Class<T> iface,
        Scripts.ScriptDefinition script)
        throws Exception
    {
        if (className == null && script == null) {
            throw Util.newError(
                "Must specify either className attribute or Script element");
        }
        if (className != null && script != null) {
            throw Util.newError(
                "Must not specify both className attribute and Script element");
        }
        if (className != null) {
            return ClassResolver.INSTANCE.<T>instantiateSafe(className);
        }
        if (iface == CellFormatter.class) {
            return iface.cast(Scripts.cellFormatter(script));
        } else if (iface == MemberFormatter.class) {
            return iface.cast(Scripts.memberFormatter(script));
        } else if (iface == PropertyFormatter.class) {
            return iface.cast(Scripts.propertyFormatter(script));
        } else {
            throw new RuntimeException("Unknown class " + iface);
        }
    }

    /**
     * Helper for {@link Cube#createCalculatedMember(String)}.
     *
     * @param cube Cube
     * @param xml XML
     * @return Calculated member
     */
    public Member createCalculatedMember(RolapCube cube, String xml) {
        MondrianDef.CalculatedMember xmlCalcMember;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
            final String tagName = def.getTagName();
            if (tagName.equals("CalculatedMember")) {
                xmlCalcMember = new MondrianDef.CalculatedMember(def);
            } else {
                throw new XOMException(
                    "Got <" + tagName + "> when expecting <CalculatedMember>");
            }
        } catch (XOMException e) {
            throw Util.newError(
                e,
                "Error while creating calculated member from XML ["
                + xml + "]");
        }

        final List<RolapMember> memberList = new ArrayList<RolapMember>();
        createCalcMembersAndNamedSets(
            Collections.singletonList(xmlCalcMember),
            Collections.<MondrianDef.NamedSet>emptyList(),
            memberList,
            cube.calculatedMemberList,
            cube.namedSetList,
            cube,
            true);
        assert memberList.size() == 1;
        return memberList.get(0);
    }

    /**
     * Returns the alias of the table that key attributes of a level occupy,
     * or null if there is no unique table.
     */
    static String getTableName(RolapLevel level) {
        Set<String> tableNames = new HashSet<String>();
        for (RolapSchema.PhysColumn expr : level.getAttribute().getKeyList()) {
            if (expr instanceof RolapSchema.PhysRealColumn) {
                RolapSchema.PhysRealColumn mc =
                    (RolapSchema.PhysRealColumn) expr;
                tableNames.add(mc.relation.getAlias());
            }
        }
        return tableNames.size() == 1 ? tableNames.iterator().next() : null;
    }

    private static final Map<String, SqlStatement.Type> VALUES =
        UnmodifiableArrayMap.of(
            "int", SqlStatement.Type.INT,
            "double", SqlStatement.Type.DOUBLE,
            "Object", SqlStatement.Type.OBJECT,
            "String", SqlStatement.Type.STRING,
            "long", SqlStatement.Type.LONG);

    private static SqlStatement.Type toInternalType(String internalTypeName) {
        SqlStatement.Type type = VALUES.get(internalTypeName);
        if (type == null && internalTypeName != null) {
            throw Util.newError(
                "Invalid value '" + internalTypeName
                + "' for attribute 'internalType' of element 'Level'. "
                + "Valid values are: "
                + VALUES.keySet());
        }
        return type;
    }

    static String getText(MondrianDef.SQL sql) {
        final StringBuilder buf = new StringBuilder();
        for (NodeDef child : sql.children) {
            if (child instanceof TextDef) {
                TextDef textDef = (TextDef) child;
                buf.append(textDef.s);
            }
        }
        return buf.toString();
    }

    /**
     * Creates a member reader which enforces the access-control profile of
     * <code>role</code>.
     *
     * <p>This method may not be efficient, so the caller should take care
     * not to call it too often. A cache is a good idea.
     *
     * @param hierarchy Hierarchy
     * @param role Role (not null)
     * @return Member reader that implements access control (never null)
     */
    static MemberReader createMemberReader(
        final RolapCubeHierarchy hierarchy,
        Role role)
    {
        final Access access = role.getAccess(hierarchy);
        switch (access) {
        case NONE:
            role.getAccess(hierarchy); // todo: remove
            throw Util.newInternal(
                "Illegal access to members of hierarchy " + hierarchy);
        case ALL:
            return (hierarchy.isRagged())
                ? new RestrictedMemberReader(hierarchy.getMemberReader(), role)
                : hierarchy.getMemberReader();

        case CUSTOM:
            final Role.HierarchyAccess hierarchyAccess =
                role.getAccessDetails(hierarchy);
            final Role.RollupPolicy rollupPolicy =
                hierarchyAccess.getRollupPolicy();
            final NumericType returnType = new NumericType();
            switch (rollupPolicy) {
            case FULL:
                return new RestrictedMemberReader(
                    hierarchy.getMemberReader(), role);
            case PARTIAL:
                Type memberType1 =
                    new MemberType(
                        hierarchy.getDimension(), hierarchy,
                        null,
                        null);
                SetType setType = new SetType(memberType1);
                ListCalc listCalc =
                    new AbstractListCalc(
                        new DummyExp(setType), new Calc[0])
                    {
                        public TupleList evaluateList(
                            Evaluator evaluator)
                        {
                            return
                                new UnaryTupleList(
                                    hierarchy.getLowestMembersForAccess(
                                        evaluator, hierarchyAccess, null));
                        }

                        public boolean dependsOn(Hierarchy hierarchy) {
                            return true;
                        }
                    };
                final Calc partialCalc =
                    new RolapHierarchy.LimitedRollupAggregateCalc(
                        returnType, listCalc);

                final Exp partialExp =
                    new ResolvedFunCall(
                        new FunDefBase("$x", "x", "In") {
                            public Calc compileCall(
                                ResolvedFunCall call,
                                ExpCompiler compiler)
                            {
                                return partialCalc;
                            }

                            public void unparse(Exp[] args, PrintWriter pw) {
                                pw.print("$RollupAccessibleChildren()");
                            }
                        },
                        new Exp[0],
                        returnType);
                return new RolapHierarchy.LimitedRollupSubstitutingMemberReader(
                    hierarchy.getMemberReader(),
                    role,
                    hierarchyAccess,
                    partialExp);

            case HIDDEN:
                Exp hiddenExp =
                    new ResolvedFunCall(
                        new FunDefBase("$x", "x", "In") {
                            public Calc compileCall(
                                ResolvedFunCall call, ExpCompiler compiler)
                            {
                                return new ConstantCalc(returnType, null);
                            }

                            public void unparse(Exp[] args, PrintWriter pw) {
                                pw.print("$RollupAccessibleChildren()");
                            }
                        },
                        new Exp[0],
                        returnType);
                return new RolapHierarchy.LimitedRollupSubstitutingMemberReader(
                    hierarchy.getMemberReader(),
                    role,
                    hierarchyAccess,
                    hiddenExp);

            default:
                throw Util.unexpected(rollupPolicy);
            }
        default:
            throw Util.badValue(access);
        }
    }

    /**
     * Handler for errors that arise while loading a schema.
     */
    interface Handler extends MondrianDef.Handler {

        /**
         * Returns the location of an element or attribute in an XML document.
         * Returns null if location is not known (for example if node has been
         * created programmatically).
         *
         * @param node Node
         * @param attributeName Attribute name, or null
         * @return Location of node or attribute in an XML document
         */
        RolapSchema.XmlLocation locate(NodeDef node, String attributeName);

        /**
         * Reports a warning.
         *
         * <p>If we are not tolerant of warnings and errors
         * (see {@link mondrian.rolap.RolapConnectionProperties#Ignore}), throws
         * immediately. A thrown exception will typically
         * abort the attempt to create the schema.
         *
         * @param message Message
         * @param node XML element that is the location of caused this exception
         * @param attributeName Name of XML attribute, or null for whole node
         */
        void warning(
            String message,
            NodeDef node,
            String attributeName);

        void warning(
            String message,
            NodeDef node,
            String attributeName,
            Throwable cause);

        /**
         * Reports a non-fatal error. Schema validation can continue, but the
         * schema will not be viable to run queries. Adds the error to the
         * list of warnings that will be returned after validation.
         *
         * <p>If we are not tolerant of warnings and errors
         * (see {@link mondrian.rolap.RolapConnectionProperties#Ignore}), throws
         * immediately. A thrown exception will typically
         * abort the attempt to create the schema.
         *
         * @see #warning
         * @see #fatal
         *
         * @param message Message
         * @param node XML element that is the location of caused this exception
         * @param attributeName Name of XML attribute, or null for whole node
         */
        void error(
            String message,
            NodeDef node,
            String attributeName);

        /**
         * Creates an error using an exception.
         *
         * <p>This is a placeholder method.
         * Eventually, validation errors will be created using a resource,
         * not a throwable (like this method) or a string (like other methods
         * on this class).
         * But until then, this method allows you to use a resourced message
         * using the schema error framework. Use it in preference to the
         * string methods if possible.
         *
         * @param message Exception containing the message
         * @param node XML element that is the location that caused this error
         * @param attributeName Name of XML attribute, or null for whole node
         */
        void error(
            MondrianException message,
            NodeDef node,
            String attributeName);

        /**
         * Reports a fatal error. Always returns an exception, which the caller
         * should throw.
         *
         * @param message Message
         * @param node XML element that is the location of caused this exception
         * @param attributeName Name of XML attribute, or null for whole node
         */
        RuntimeException fatal(
            String message,
            NodeDef node,
            String attributeName);

        /**
         * Checks whether there were (ignored) errors, and if so throws.
         */
        void check() throws MondrianMultipleSchemaException;
    }

    static class UnresolvedLink {
        final RolapSchema.PhysKey sourceKey;
        final RolapSchema.PhysRelation targetRelation;
        final List<RolapSchema.PhysColumn> columnList;

        public UnresolvedLink(
            RolapSchema.PhysKey sourceKey,
            RolapSchema.PhysRelation targetRelation,
            List<RolapSchema.PhysColumn> columnList)
        {
            this.sourceKey = sourceKey;
            this.targetRelation = targetRelation;
            this.columnList = columnList;
        }
    }

    static class PhysSchemaBuilder {
        final RolapSchemaLoader loader;
        RolapCube cube;
        final RolapSchema.PhysSchema physSchema;
        private int nextId = 0;
        Map<String, DimensionLink> dimensionLinks =
            new HashMap<String, DimensionLink>();

        PhysSchemaBuilder(
            RolapSchemaLoader loader,
            RolapSchema.PhysSchema physSchema)
        {
            assert physSchema != null;
            this.loader = loader;
            this.physSchema = physSchema;
        }

        /**
         * Converts an XML expression into a scalar expression. If relation
         * is not specified, all columns in the expression must be qualified
         * by table.
         *
         * @param physRelation Physical relation, or null
         * @param exp XML expression
         * @return Physical expression
         */
        RolapSchema.PhysExpr toPhysExpr(
            RolapSchema.PhysRelation physRelation,
            MondrianDef.Expression exp)
        {
            if (exp == null) {
                return null;
            } else if (exp instanceof MondrianDef.ExpressionView) {
                final MondrianDef.ExpressionView expressionView =
                    (MondrianDef.ExpressionView) exp;
                return toPhysExpr(physRelation, expressionView);
            } else if (exp instanceof MondrianDef.Column) {
                MondrianDef.Column column = (MondrianDef.Column) exp;
                if (column.table != null) {
                    physRelation = getPhysRelation(column.table, false);
                }
                if (physRelation == null) {
                    getHandler().error(
                        "Column must specify table",
                        exp,
                        null);
                    // Dummy expression, to let us proceed with validation. The
                    // error ensures that we don't consider this schema to
                    // be viable for queries.
                    return new RolapSchema.PhysTextExpr("0");
                }
                final RolapSchema.PhysColumn physColumn =
                    physRelation.getColumn(
                        column.name,
                        false);
                if (physColumn == null) {
                    getHandler().error(
                        "Unknown column '" + column.name + "'",
                        column,
                        "name");
                    return new RolapSchema.PhysTextExpr("0");
                }
                return physColumn;
            } else {
                throw Util.newInternal(
                    "Unknown expression type " + exp.getClass());
            }
        }

        RolapSchema.PhysExpr toPhysExpr(
            RolapSchema.PhysRelation physRelation,
            MondrianDef.ExpressionView xmlExpressionView)
        {
            final MondrianDef.SQL sql =
                MondrianDef.SQL.choose(
                    xmlExpressionView.expressions,
                    physSchema.dialect);
            return toPhysExpr(physRelation, sql);
        }

        RolapSchema.PhysExpr toPhysExpr(
            RolapSchema.PhysRelation physRelation,
            MondrianDef.SQL sql)
        {
            if (sql.children.length == 1
                && sql.children[0] instanceof MondrianDef.Column)
            {
                return toPhysExpr(
                    physRelation,
                    (MondrianDef.Column) sql.children[0]);
            } else {
                final List<RolapSchema.PhysExpr> list =
                    new ArrayList<RolapSchema.PhysExpr>();
                for (NodeDef child : sql.children) {
                    if (child instanceof TextDef) {
                        TextDef text = (TextDef) child;
                        list.add(
                            new RolapSchema.PhysTextExpr(
                                text.getText()));
                    } else {
                        list.add(
                            toPhysExpr(
                                physRelation,
                                (MondrianDef.Expression) child));
                    }
                }
                // If all of the sub-expressions belong to the same
                // relation, make the expression a derived column. This is
                // required because level keys must be columns.
                final Set<RolapSchema.PhysRelation> relationSet =
                    new HashSet<RolapSchema.PhysRelation>();
                collectRelations(list, null, relationSet);
                if (relationSet.size() != 1) {
                    return new RolapSchema.PhysCalcExpr(
                        list);
                } else {
                    // TODO: embed expression in calc column
                    final RolapSchema.PhysRelation relation =
                        relationSet.iterator().next();
                    final RolapSchema.PhysCalcColumn physCalcColumn =
                        new RolapSchema.PhysCalcColumn(
                            loader,
                            null,
                            relation,
                            "$" + (nextId++),
                            null,
                            null,
                            list);
                    relation.addColumn(physCalcColumn);
                    return physCalcColumn;
                }
            }
        }

        /**
         * Returns the relations that a list of expressions belong to.
         *
         * @param list List of expressions
         * @param defaultRelation Relation that an expression should belong to
         *     if not explicitly specified
         * @param relationSet Set of relations to add to
         */
        private static void collectRelations(
            List<RolapSchema.PhysExpr> list,
            RolapSchema.PhysRelation defaultRelation,
            Set<RolapSchema.PhysRelation> relationSet)
        {
            for (RolapSchema.PhysExpr expr : list) {
                collectRelations(expr, defaultRelation, relationSet);
            }
        }

        /**
         * Collects the relations that an expression belongs to.
         *
         * @param expr Expression
         * @param defaultRelation Default relation, for expressions that
         *    do not explicitly belong to a relation (e.g. '1' or 'count(*)').
         *    May be null if there are multiple relations to choose from
         * @param relationSet Set of relations to add to
         */
        static void collectRelations(
            RolapSchema.PhysExpr expr,
            RolapSchema.PhysRelation defaultRelation,
            Set<RolapSchema.PhysRelation> relationSet)
        {
            if (expr instanceof RolapSchema.PhysColumn) {
                RolapSchema.PhysColumn column = (RolapSchema.PhysColumn) expr;
                assert column.relation != null;
                relationSet.add(column.relation);
            } else if (expr instanceof RolapSchema.PhysCalcColumn) {
                collectRelations(
                    ((RolapSchema.PhysCalcColumn) expr).getList(),
                    defaultRelation,
                    relationSet);
            } else if (expr instanceof RolapSchema.PhysCalcExpr) {
                collectRelations(
                    ((RolapSchema.PhysCalcExpr) expr).list,
                    defaultRelation,
                    relationSet);
            } else if (defaultRelation != null) {
                relationSet.add(defaultRelation);
            }
        }

        /**
         * Converts an expression, specified as either a column name or
         * an embedded expression element, into a physical expression.
         *
         * <p>Exactly one of the column name and the expression must be
         * specified. The relation must be specified if it is a column
         * expression.
         *
         * @param relation Physical relation, may be null if this is
         *   an expression or fully-qualified column
         * @param exp Expression element
         * @param column Column name
         * @return Physical expression
         */
        RolapSchema.PhysExpr toPhysExpr(
            RolapSchema.PhysRelation relation,
            final MondrianDef.Expression exp,
            String column)
        {
            if (exp != null) {
                return this.toPhysExpr(relation, exp);
            } else if (column != null) {
                if (relation == null) {
                    loader.getHandler().error(
                        "Table must be specified", exp, null);
                    return this.dummyColumn(relation);
                }
                return this.toPhysColumn(relation, column);
            } else {
                return null;
            }
        }

        public RolapSchema.PhysRelation getPhysRelation(
            String alias,
            boolean fail)
        {
            final RolapSchema.PhysRelation physTable =
                physSchema.tablesByName.get(alias);
            if (physTable == null && fail) {
                throw Util.newInternal("Table '" + alias + "' not found");
            }
            return physTable;
        }

        public RolapSchema.PhysColumn toPhysColumn(
            RolapSchema.PhysRelation physRelation,
            String column)
        {
            return physRelation.getColumn(column, true);
        }

        /**
         * Creates a dummy column expression. Generally this is used in the
         * event of an error, to continue the validation process.
         *
         * @param relation Relation, may be null
         * @return dummy column
         */
        public RolapSchema.PhysColumn dummyColumn(
            RolapSchema.PhysRelation relation)
        {
            if (relation == null) {
                final String tableName = "dummyTable$" + (nextId++);
                relation =
                    new RolapSchema.PhysTable(
                        physSchema, null, tableName, tableName,
                        Collections.<String, String>emptyMap());
            }
            return
                new RolapSchema.PhysCalcColumn(
                    loader,
                    null,
                    relation,
                    "dummy$" + (nextId++),
                    null,
                    null,
                    Collections.<RolapSchema.PhysExpr>singletonList(
                        new RolapSchema.PhysTextExpr("0")));
        }

        /**
         * Collects the relationships used in an aggregate expression.
         *
         * @param aggregator Aggregate function
         * @param expr Expression (may be null)
         * @param defaultRelation Default relation
         * @param relationSet Relation set to populate
         */
        public static void collectRelations(
            RolapAggregator aggregator,
            RolapSchema.PhysExpr expr,
            RolapSchema.PhysRelation defaultRelation,
            Set<RolapSchema.PhysRelation> relationSet)
        {
            assert aggregator != null;
            if (aggregator == RolapAggregator.Count
                && expr == null
                && defaultRelation != null)
            {
                relationSet.add(defaultRelation);
            }
            if (expr != null) {
                collectRelations(expr, defaultRelation, relationSet);
            }
        }

        public Handler getHandler() {
            return loader.getHandler();
        }

        public static class DimensionLink extends MondrianDef.DimensionLink {
            public DimensionLink(
                RolapSchemaUpgrader schemaUpgrader,
                String name,
                RolapSchema.PhysRelation fact,
                String foreignKey,
                String primaryKeyTable,
                String primaryKey,
                boolean degenerate)
            {
            }
        }
    }

    interface RolapSchemaValidator {
        /**
         * Returns the XML element for a given schema object, failing if not
         * found.
         *
         * @param o Schema object (e.g. a Dimension)
         * @return XML element
         */
        <T extends NodeDef> T getXml(Object o);

        /**
         * Returns the XML element for a given schema object.
         *
         * @param o Schema object (e.g. a Dimension)
         * @param fail Whether to fail if not found
         * @return XML element, or null if not found and not fail
         */
        <T extends NodeDef> T getXml(Object o, boolean fail);
    }

    static class RolapSchemaValidatorImpl implements RolapSchemaValidator {
        private final Map<Object, NodeDef> map =
            new IdentityHashMap<Object, NodeDef>();

        public <T extends NodeDef> T getXml(Object o) {
            return this.<T>getXml(o, true);
        }

        public <T extends NodeDef> T getXml(Object o, boolean fail) {
            final NodeDef xml = map.get(o);
            assert !(xml == null && fail) : "no xml element fouund for " + o;
            //noinspection unchecked
            return (T) xml;
        }

        public <T extends NodeDef> T getXmls(Object... os) {
            for (Object o1 : os) {
                T t = this.<T>getXml(o1, false);
                if (t != null) {
                    return t;
                }
            }
            return null;
        }

        public void putXml(Object metaElement, NodeDef xml) {
            map.put(metaElement, xml);
        }
    }

    enum MissingLinkAction {
        IGNORE {
            public void handle(
                Handler handler, String message, NodeDef xml, String attr)
            {
                // do nothing
            }
        },
        WARNING {
            public void handle(
                Handler handler, String message, NodeDef xml, String attr)
            {
                handler.warning(message, xml, attr);
            }
        },
        ERROR {
            public void handle(
                Handler handler, String message, NodeDef xml, String attr)
            {
                handler.error(message, xml, attr);
            }
        };

        public abstract void handle(
            Handler handler, String message, NodeDef xml, String attr);
    }

    /**
     * Exception that schema load failed. It contains one or more exceptions.
     */
    public static class MondrianMultipleSchemaException
        extends RuntimeException
    {
        public final List<RolapSchema.MondrianSchemaException> exceptionList;

        /** Creates a MondrianMultipleSchemaException. */
        public MondrianMultipleSchemaException(
            String message,
            List<RolapSchema.MondrianSchemaException> exceptionList)
        {
            super(message);
            this.exceptionList = exceptionList;
            assert exceptionList.size() > 0;
        }
    }

    /** Action to assign a default member of a hierarchy. As objects, such
     * actions can be stored in a list and deferred until the cube is
     * sufficiently initialized. */
    private abstract class AssignDefaultMember {
        final RolapCube cube;
        final RolapHierarchy hierarchy;
        final ElementDef xml;

        AssignDefaultMember(
            RolapCube cube,
            RolapHierarchy hierarchy,
            ElementDef xml)
        {
            this.cube = cube;
            this.hierarchy = hierarchy;
            this.xml = xml;
        }

        /** Resolve default member based on real members. Calculated members are
         * not available yet. */
        abstract AssignDefaultMember apply();

        /** Assigns default member based on calculated members. */
        abstract void apply2();

        protected void setDefaultMember(
            RolapCubeHierarchy hierarchy,
            RolapMember member)
        {
            hierarchy.setDefaultMember(member);
        }

        protected List<RolapCubeHierarchy> cubeHierarchies() {
            final List<RolapCubeHierarchy> list = cubeHierMap.get(hierarchy);
            return list != null
                ? list
                : Collections.<RolapCubeHierarchy>emptyList();
        }
    }

    private class MeasureAssignDefaultMember
        extends NamelessAssignDefaultMember
    {
        public MeasureAssignDefaultMember(
            RolapCube cube,
            RolapHierarchy hierarchy,
            MondrianDef.Cube xml)
        {
            super(cube, hierarchy, xml);
        }

        @Override
        protected RolapMember firstMember(RolapCubeHierarchy cubeHierarchy) {
            // A cube must include at least one measure. The [Fact Count]
            // measures that are created for measure groups do not count;
            // they are not visible.
            for (RolapMember measure : measureList) {
                if (containsByIdentity(aggFactCountMeasureList, measure)) {
                    continue;
                }
                return measure;
            }
            return null;
        }

        @Override
        protected void fail() {
            final MondrianDef.Cube xmlCube = (MondrianDef.Cube) xml;
            getHandler().error(
                "Cube '" + xmlCube.name + "' must have at least one measure",
                xmlCube,
                null);
        }
    }

    private class NamedMeasureAssignDefaultMember
        extends NamedAssignDefaultMember
    {
        public NamedMeasureAssignDefaultMember(
            RolapCube cube,
            RolapHierarchy hierarchy,
            MondrianDef.Cube xml)
        {
            super(cube, hierarchy, xml, xml.defaultMeasure);
        }

        @Override
        protected RolapMember lookup(
            SchemaReader schemaReader,
            RolapCubeHierarchy cubeHierarchy)
        {
            return findMeasure(measureList, defaultMemberName);
        }

        @Override
        protected void fail() {
            final MondrianDef.Cube xmlCube = (MondrianDef.Cube) xml;
            handler.error(
                "Default measure '" + xmlCube.defaultMeasure + "' not found",
                xmlCube,
                "defaultMeasure");
        }
    }

    private class NamelessAssignDefaultMember extends AssignDefaultMember {
        public NamelessAssignDefaultMember(
            RolapCube cube,
            RolapHierarchy hierarchy,
            ElementDef xml)
        {
            super(cube, hierarchy, xml);
        }

        /** Resolve default member based on real members. Calculated members are
         * not available yet. */
        public AssignDefaultMember apply() {
            // Safe starting point.
            for (RolapCubeHierarchy cubeHierarchy : cubeHierarchies()) {
                setDefaultMember(cubeHierarchy, cubeHierarchy.getAllMember());
            }
            if (hierarchy.hasAll()) {
                return null;
            }
            int failedCount = 0;
            for (RolapCubeHierarchy cubeHierarchy : cubeHierarchies()) {
                RolapMember member = firstMember(cubeHierarchy);
                if (member != null) {
                    setDefaultMember(cubeHierarchy, member);
                } else {
                    ++failedCount;
                }
            }

            // Indicate that our job is not done. RolapSchemaLoader will call
            // apply2. In the mean time, the 'all' member will be the default.
            return failedCount > 0 ? this : null;
        }

        protected RolapMember firstMember(RolapCubeHierarchy cubeHierarchy) {
            SchemaReader schemaReader = cube.getSchemaReader();
            return deriveDefaultMember(cubeHierarchy, schemaReader);
        }

        RolapMember deriveDefaultMember(
            RolapCubeHierarchy hierarchy,
            SchemaReader schemaReader)
        {
            List<RolapMember> rootMembers =
                hierarchy.getMemberReader().getRootMembers();
            List<RolapMember> calcMemberList =
                Util.cast(
                    schemaReader.getCalculatedMembers(
                        hierarchy.getLevelList().get(0)));
            for (RolapMember rootMember
                : UnionIterator.over(rootMembers, calcMemberList))
            {
                if (rootMember.isHidden()) {
                    continue;
                }
                // Note: We require that the root member is not a hidden member
                // of a ragged hierarchy, but we do not require that it is
                // visible. In particular, if a cube contains no explicit
                // measures, the default measure will be the implicitly defined
                // [Fact Count] measure, which happens to be non-visible.
                return rootMember;
            }
            return null;
        }

        void apply2() {
            // Usually when default name is not specified, we take the 'all'.
            // We are here because the hierarchy has no 'all' and no real
            // members. Look for calculated members.
            assert !hierarchy.hasAll();
            int failedCount = 0;
            for (RolapCubeHierarchy cubeHierarchy : cubeHierarchies()) {
                RolapMember member = firstMember(cubeHierarchy);
                if (member != null) {
                    setDefaultMember(cubeHierarchy, member);
                } else {
                    ++failedCount;
                }
            }
            if (failedCount > 0) {
                fail();
            }
        }

        protected void fail() {
            getHandler().error(
                MondrianResource.instance().InvalidHierarchyCondition
                    .ex(hierarchy.getUniqueName()),
                xml,
                null);
        }
    }

    private class NamedAssignDefaultMember extends AssignDefaultMember {
        private final List<Id.Segment> uniqueNameParts;
        protected final String defaultMemberName;

        public NamedAssignDefaultMember(
            RolapCube cube,
            RolapHierarchy hierarchy,
            ElementDef xml,
            String defaultMemberName)
        {
            super(cube, hierarchy, xml);
            this.defaultMemberName = defaultMemberName;
            if (defaultMemberName.contains("[")) {
                uniqueNameParts = Util.parseIdentifier(defaultMemberName);
            } else {
                uniqueNameParts =
                    Collections.<Id.Segment>singletonList(
                        new Id.NameSegment(
                            defaultMemberName,
                            Id.Quoting.UNQUOTED));
            }
        }

        /** Resolve default member based on real members. Calculated members are
         * not available yet. */
        public AssignDefaultMember apply() {
            // Safe starting point.
            for (RolapCubeHierarchy cubeHierarchy : cubeHierarchies()) {
                setDefaultMember(cubeHierarchy, cubeHierarchy.getAllMember());
            }

            int failureCount = 0;
            for (RolapCubeHierarchy cubeHierarchy : cubeHierarchies()) {
                RolapMember member =
                    lookup(cube.getSchemaReader(), cubeHierarchy);
                if (member != null) {
                    setDefaultMember(cubeHierarchy, member);
                } else {
                    ++failureCount;
                }
            }

            // Indicate that our job is not done. RolapSchemaLoader will call
            // apply2. In the mean time, the 'all' member will be the default.
            return failureCount > 0 ? this : null;
        }

        public void apply2() {
            int failureCount = 0;
            for (RolapCubeHierarchy cubeHierarchy : cubeHierarchies()) {
                RolapMember member =
                    lookup(cube.getSchemaReader(), cubeHierarchy);
                if (member != null) {
                    setDefaultMember(cubeHierarchy, member);
                } else {
                    ++failureCount;
                }
            }
            if (failureCount > 0) {
                fail();
            }
        }

        protected void fail() {
            handler.error(
                "Can not find Default Member with name \""
                + defaultMemberName + "\"", xml, null);
        }

        protected RolapMember lookup(
            SchemaReader schemaReader,
            RolapCubeHierarchy cubeHierarchy)
        {
            // First look up from within this hierarchy. Works for
            // unqualified names, e.g. [USA].[CA].
            RolapMember member =
                (RolapMember) new NameResolver().resolve(
                    cubeHierarchy,
                    Util.toOlap4j(uniqueNameParts),
                    false,
                    Category.Member,
                    MatchType.EXACT,
                    cube.getSchemaReader().getNamespaces());

            if (member != null) {
                return member;
            }

            // Next look up within global context. Works for qualified
            // names, e.g. [Store].[USA].[CA] or
            // [Time].[Weekly].[1997].[Q2].
            return (RolapMember)
                new NameResolver().resolve(
                    new RolapHierarchy.DummyElement(cubeHierarchy),
                    Util.toOlap4j(uniqueNameParts),
                    false,
                    Category.Member,
                    MatchType.EXACT,
                    cube.getSchemaReader().getNamespaces());
        }
    }
}

// End RolapSchemaLoader.java
