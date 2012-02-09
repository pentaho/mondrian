/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2010-2012 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.DimensionType;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;
import mondrian.olap.Member;
import mondrian.olap.NamedSet;
import mondrian.olap.Property;
import mondrian.olap.fun.UdfResolver;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.aggmatcher.ExplicitRules;
import mondrian.server.Locus;
import mondrian.spi.CellFormatter;
import mondrian.spi.Dialect;
import mondrian.spi.MemberFormatter;
import mondrian.spi.PropertyFormatter;
import mondrian.spi.impl.Scripts;
import mondrian.util.*;

import org.apache.commons.vfs.FileSystemException;
import org.apache.log4j.Logger;

import org.eigenbase.xom.*;
import org.eigenbase.xom.Parser;

import org.olap4j.impl.Olap4jUtil;
import org.olap4j.impl.UnmodifiableArrayMap;
import org.olap4j.metadata.*;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.*;
import javax.sql.DataSource;

/**
 * Creates a schema from XML.
 *
 * @author jhyde
 * @version $Id$
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

    static {
        for (Dialect.Datatype datatype : Dialect.Datatype.values()) {
            DATATYPE_MAP.put(datatype.name(), datatype);
        }
    }

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
        final String key,
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
                        "RolapSchema.load: content: \n" + catalogStr);
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "RolapSchema.load: catalogStr: \n" + catalogStr);
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
            final boolean legacy = isLegacy(def);
            if (legacy) {
                LOGGER.warn("Model is in legacy format");
                Mondrian3Def.Schema xmlLegacySchema =
                    new Mondrian3Def.Schema(def);
                RolapSchema tempSchema =
                    new RolapSchema(
                        key,
                        connectInfo,
                        dataSource,
                        md5Bytes,
                        useContentChecksum,
                        xmlLegacySchema.name,
                        Collections.<String, Annotation>emptyMap());
                tempSchema.physicalSchema =
                    new RolapSchema.PhysSchema(
                        tempSchema.getDialect(),
                        tempSchema.getInternalConnection().getDataSource());
                RolapSchemaUpgrader upgrader =
                    new RolapSchemaUpgrader(
                        this, tempSchema, tempSchema.physicalSchema);
                xmlSchema = upgrader.convertSchema(xmlLegacySchema);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "schema after conversion:\n" + xmlSchema.toXML());
                }
            } else {
                xmlSchema = new MondrianDef.Schema(def);
            }

            return loadStage1(
                key,
                md5Bytes,
                useContentChecksum,
                connectInfo,
                dataSource,
                xmlSchema);
        } catch (XOMException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        } catch (FileSystemException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        } catch (IOException e) {
            throw Util.newError(e, "while parsing catalog " + catalogUrl);
        }
    }

    private RolapSchema loadStage1(
        String key,
        ByteString md5Bytes,
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

        final Map<String, Annotation> annotationMap =
            createAnnotationMap(xmlSchema.getAnnotations());

        schema =
            new RolapSchema(
                key,
                connectInfo,
                dataSource,
                md5Bytes,
                useContentChecksum,
                xmlSchema.name,
                annotationMap);

        validator.putXml(schema, xmlSchema);
        missingLinkAction =
            Util.lookup(
                Util.toUpperCase(xmlSchema.missingLink),
                MissingLinkAction.WARNING);

        loadStage2(
            schema,
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
            return true;
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
        RolapSchema schema,
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

        this.physSchemaBuilder =
            new PhysSchemaBuilder(null, schema.physicalSchema);

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
            final boolean modifiable = xmlParameter.modifiable;
            String defaultValue = xmlParameter.defaultValue;
            RolapSchemaParameter param =
                new RolapSchemaParameter(
                    schema, parameterName, defaultValue, description, type,
                    modifiable);
            validator.putXml(param, xmlParameter);
        }

        // Create cubes.
        for (MondrianDef.Cube xmlCube : xmlCubes) {
            if (xmlCube.enabled == null || xmlCube.enabled) {
                RolapCube cube =
                    new RolapCube(
                        schema,
                        xmlCube.name,
                        toBoolean(xmlCube.visible, true),
                        xmlCube.caption,
                        xmlCube.description,
                        createAnnotationMap(xmlCube.getAnnotations()),
                        xmlSchema.measuresCaption);
                validator.putXml(cube, xmlCube);
                initCube(cube);
                schema.addCube(cube);
            }
        }

        // Create named sets.
        for (MondrianDef.NamedSet xmlNamedSet : xmlSchema.getNamedSets()) {
            schema.addNamedSet(
                xmlNamedSet.name,
                createNamedSet(null, xmlNamedSet));
        }

        // Create roles.
        Map<String, Role> rolesByName = new LinkedHashMap<String, Role>();
        for (MondrianDef.Role xmlRole : xmlSchema.getRoles()) {
            final Role role = createRole(xmlRole, rolesByName);
            rolesByName.put(xmlRole.name, role);
        }

        Role defaultRole = null;
        if (xmlSchema.defaultRole != null) {
            // At this stage, the only roles in mapNameToRole are
            // RoleImpl roles so it is safe to cast.
            defaultRole = (RoleImpl) schema.lookupRole(xmlSchema.defaultRole);
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
        assert xmlPhysicalSchema != null;
        final RolapSchema.PhysSchema physSchema =
            createSyntheticPhysicalSchema();
        final Set<ElementDef> skip = new HashSet<ElementDef>();

        // First pass through elements, creating tables, ensuring that
        // table names are unique.
        // Register columns explicitly defined and also load columns from JDBC.
        // Collect calculated columns, to be resolved later.
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList =
            new ArrayList<RolapSchema.UnresolvedColumn>();
        for (MondrianDef.Relation relation : Util.filter(
            xmlPhysicalSchema.childArray, MondrianDef.Relation.class))
        {
            final String alias = relation.getAlias();
            final RolapSchema.PhysRelationImpl physTable;
            if (relation instanceof MondrianDef.Table) {
                physTable =
                    registerTable(
                        handler,
                        dialect,
                        schema,
                        physSchema,
                        skip,
                        unresolvedColumnList,
                        alias,
                        (MondrianDef.Table) relation);
            } else if (relation instanceof MondrianDef.InlineTable) {
                MondrianDef.InlineTable inlineTable =
                    (MondrianDef.InlineTable) relation;
                if (false) {
                handler.warning(
                    "inline table in physical schema is not "
                    + "currently supported",
                    inlineTable,
                    null);
                continue;
                }
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
        for (MondrianDef.Link link
            : Util.filter(xmlPhysicalSchema.childArray, MondrianDef.Link.class))
        {
            List<RolapSchema.PhysColumn> columnList =
                new ArrayList<RolapSchema.PhysColumn>();
            int errorCount = 0;
            final RolapSchema.PhysRelation sourceTable =
                physSchema.tablesByName.get(link.source);
            String keyName = first(link.key, "primary");
            RolapSchema.PhysKey sourceKey = null;
            if (sourceTable == null) {
                handler.warning(
                    "Link references unknown source table '" + link.source
                    + "'.",
                    link,
                    "source");
                ++errorCount;
            } else if ((sourceKey = sourceTable.lookupKey(keyName)) == null) {
                handler.warning(
                    "Source table '" + link.source
                    + "' of link has no key named '" + keyName + "'.",
                    link,
                    "source");
                ++errorCount;
            }
            final RolapSchema.PhysRelation targetRelation =
                physSchema.tablesByName.get(link.target);
            if (targetRelation == null) {
                handler.warning(
                    "Link references unknown target table '"
                    + link.target + "'.",
                    link,
                    "target");
                ++errorCount;
            } else {
                List<RolapSchema.PhysColumn> targetColumnList =
                    new ArrayList<RolapSchema.PhysColumn>();
                for (MondrianDef.Column foreignKeyColumn
                    : link.foreignKey.array)
                {
                    if (foreignKeyColumn.table != null
                        && !foreignKeyColumn.table.equals(link.target))
                    {
                        handler.warning(
                            "link key column must belong to target table",
                            foreignKeyColumn,
                            "table");
                        ++errorCount;
                    } else {
                        final RolapSchema.PhysColumn column =
                            targetRelation.getColumn(
                                foreignKeyColumn.name, false);
                        if (column == null) {
                            handler.warning(
                                "column '" + foreignKeyColumn.name
                                + "' is unknown in link target table '"
                                + link.target + "'",
                                foreignKeyColumn,
                                "column");
                            ++errorCount;
                        } else {
                            columnList.add(column);
                        }
                    }
                }
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
            schema.resolve(this, physSchema, unresolvedColumn);
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
                            null,
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
                                null,
                                null);
                        }
                    }
                }
            }
        }

        return physSchema;
    }

    private RolapSchema.PhysInlineTable registerInlineTable(
        Handler handler,
        Dialect dialect,
        RolapSchema schema,
        RolapSchema.PhysSchema physSchema,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        MondrianDef.InlineTable inlineTable)
    {
        RolapSchema.PhysInlineTable physInlineTable =
            new RolapSchema.PhysInlineTable(
                physSchema,
                alias);
        for (MondrianDef.RealOrCalcColumnDef column
            : inlineTable.getColumnDefs())
        {
            registerColumn(
                handler,
                dialect,
                skip,
                unresolvedColumnList,
                alias,
                physInlineTable,
                inlineTable,
                column);
        }
        registerKey(
            handler, inlineTable.getKey(), unresolvedColumnList,
            physInlineTable);
        return physInlineTable;
    }

    private RolapSchema.PhysTable registerTable(
        Handler handler,
        Dialect dialect,
        RolapSchema schema,
        RolapSchema.PhysSchema physSchema,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        MondrianDef.Table table)
    {
        final RolapSchema.PhysTable physTable =
            new RolapSchema.PhysTable(
                physSchema,
                table.schema,
                table.name,
                alias,
                buildHintMap(table.getHints()));

        // First pass, register columns. We will resolve and
        // register keys later.
        for (MondrianDef.RealOrCalcColumnDef column : table.getColumnDefs()) {
            registerColumn(
                handler,
                dialect,
                skip,
                unresolvedColumnList,
                alias,
                physTable,
                table,
                column);
        }

        // Read columns from JDBC.
        physTable.ensurePopulated(this, table);

        for (MondrianDef.Key key : table.getKeys()) {
            registerKey(handler, key, unresolvedColumnList, physTable);
        }
        return physTable;
    }

    private void registerKey(
        Handler handler,
        MondrianDef.Key xmlKey,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        final RolapSchema.PhysRelationImpl physTable)
    {
        if (xmlKey == null) {
            return;
        }
        String keyName = xmlKey.name;
        if (keyName == null) {
            keyName = "primary";
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
        for (MondrianDef.Column columnRef : xmlKey.array) {
            final int index = i++;
            final RolapSchema.UnresolvedColumn unresolvedColumn =
                new RolapSchema.UnresolvedColumn(
                    physTable,
                    columnRef.table != null
                        ? columnRef.table
                        : physTable.alias,
                    columnRef.name,
                    columnRef)
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

    private void registerColumn(
        Handler handler,
        Dialect dialect,
        Set<ElementDef> skip,
        List<RolapSchema.UnresolvedColumn> unresolvedColumnList,
        String alias,
        RolapSchema.PhysRelationImpl physRelation,
        MondrianDef.Relation table,
        MondrianDef.RealOrCalcColumnDef column)
    {
        if (physRelation.columnsByName.containsKey(column.name)) {
            handler.warning(
                "Duplicate column '" + column.name
                + "' in table '" + alias + "'.",
                column,
                null);
            skip.add(column);
            return;
        }
        if (physRelation instanceof RolapSchema.PhysInlineTable) {
            RolapSchema.PhysInlineTable physInlineTable =
                (RolapSchema.PhysInlineTable) physRelation;
            if (column instanceof MondrianDef.CalculatedColumnDef) {
                handler.warning(
                    "Cannot define calculated column in inline table",
                    column,
                    null);
            } else {
                physInlineTable.addColumn(
                    new RolapSchema.PhysRealColumn(
                        physInlineTable,
                        column.name,
                        toType(column.type),
                        toInternalType(column.internalType),
                        -1));
            }
            return;
        }
        final RolapSchema.PhysTable physTable =
            (RolapSchema.PhysTable) physRelation;
        if (column instanceof MondrianDef.CalculatedColumnDef) {
            MondrianDef.CalculatedColumnDef calcColumnDef =
                (MondrianDef.CalculatedColumnDef) column;
            final List<RolapSchema.PhysExpr> list =
                new ArrayList<RolapSchema.PhysExpr>();
            final RolapSchema.PhysCalcColumn physCalcColumn =
                new RolapSchema.PhysCalcColumn(
                    physTable,
                    column.name,
                    toType(column.type),
                    toInternalType(column.internalType),
                    list);
            final MondrianDef.SQL sql;
            if (calcColumnDef.expression
                instanceof MondrianDef.ExpressionView)
            {
                MondrianDef.ExpressionView expressionView =
                    (MondrianDef.ExpressionView)
                        calcColumnDef.expression;
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
                            calcColumnDef.expression
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
            physTable.addColumn(physCalcColumn);
        } else {
            // Check that column exists; throw if not.
            RolapSchema.PhysColumn physColumn =
                physRelation.getColumn(
                    column.name,
                    true);
            physColumn.setDatatype(
                toType(column.type));
            physColumn.setInternalType(
                toInternalType(column.internalType));
        }
    }

    private static String trim(String s, boolean left, boolean right) {
        while (left && s.length() > 0 && Character.isWhitespace(s.charAt(0))) {
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

    private Dialect.Datatype toType(String type) {
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
        return new RolapSchema.PhysSchema(
            schema.getDialect(),
            schema.getInternalConnection().getDataSource());
    }

    static DimensionType getDimensionType(MondrianDef.Dimension xmlDimension) {
        if (xmlDimension.type == null) {
            return null; //DimensionType.StandardDimension;
        } else {
            return DimensionType.valueOf(xmlDimension.type);
        }
    }

    /**
     * Initializes a cube.
     */
    void initCube(RolapCube cube)
    {
        dimensionPaths.clear();

        final MondrianDef.Schema xmlSchema = validator.getXml(schema);
        final MondrianDef.Cube xmlCube = validator.getXml(cube);
        final NamedList<MondrianDef.Dimension> xmlCubeDimensions =
            xmlCube.getDimensions();
        int ordinal = cube.getDimensionList().size();
        for (MondrianDef.Dimension xmlCubeDimension : xmlCubeDimensions) {
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
                    createAnnotationMap(xmlCubeDimension.getAnnotations()));
            if (dimension == null) {
                continue;
            }

            cube.addDimension(dimension);
        }

        schema.addCube(cube);

        // Initialize dimensions before measure groups. (Measure groups contain
        // dimension links, and these reference dimensions.)
        for (RolapCubeDimension dimension : cube.getDimensionList()) {
            if (dimension.isMeasures()) {
                // already initialized
                continue;
            }
            initDimension(dimension);
        }

        final NamedList<MondrianDef.MeasureGroup> xmlMeasureGroups =
            xmlCube.getMeasureGroups();
        if (xmlMeasureGroups.size() == 0) {
            handler.warning(
                "Cube definition must contain MeasureGroups element, and at least one MeasureGroup",
                xmlCube,
                null);
        }

        List<RolapMember> measureList = new ArrayList<RolapMember>();
        final Set<String> measureGroupNames = new HashSet<String>();
        for (MondrianDef.MeasureGroup xmlMeasureGroup : xmlMeasureGroups) {
            if (!measureGroupNames.add(xmlMeasureGroup.name)) {
                handler.warning(
                    "Duplicate measure group '" + xmlMeasureGroup.name
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
                    xmlMeasureGroup.name,
                    xmlMeasureGroup.ignoreUnrelatedDimensions != null
                    && xmlMeasureGroup.ignoreUnrelatedDimensions,
                    star);
            validator.putXml(measureGroup, xmlMeasureGroup);
            cube.addMeasureGroup(measureGroup);

            for (MondrianDef.Measure xmlMeasure : xmlMeasureGroup.getMeasures())
            {
                RolapSchema.PhysRelation relation =
                    xmlMeasureGroup.table == null
                        ? null
                        : getPhysRelation(
                            xmlMeasureGroup.table, xmlMeasureGroup, "table");
                final RolapBaseCubeMeasure measure =
                    createMeasure(
                        measureGroup, xmlMeasure, relation);
                if (measure.getOrdinal() == -1) {
                    measure.setOrdinal(measureList.size());
                }
                if (measure.getAggregator() == RolapAggregator.Count) {
                    measureGroup.factCountMeasure = measure;
                }
                if (measure.getExpr() instanceof RolapSchema.PhysColumn) {
                    RolapSchema.PhysColumn column =
                        (RolapSchema.PhysColumn) measure.getExpr();
                    // Only set if different from default (so that if two cubes
                    // share the same fact table, either can turn off caching
                    // and both are affected).
                    if (!xmlCube.cache) {
                        star.setCacheAggregations(false);
                    }
                }

                measureList.add(measure);
                measureGroup.measureList.add(measure);
            }

            // Ensure that the measure group has an atomic cell count measure
            // even if the schema does not contain one.
            if (measureGroup.factCountMeasure == null) {
                final MondrianDef.Measure xmlMeasure =
                    new MondrianDef.Measure();
                xmlMeasure.aggregator = "count";
                xmlMeasure.name = "Fact Count";
                xmlMeasure.visible = Boolean.FALSE;
                RolapSchema.PhysRelation relation =
                    xmlMeasureGroup.table == null
                        ? null
                        : getPhysRelation(
                            xmlMeasureGroup.table, xmlMeasureGroup, "table");
                measureGroup.factCountMeasure =
                    createMeasure(measureGroup, xmlMeasure, relation);
                measureList.add(measureGroup.factCountMeasure);
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
                        + xmlMeasureGroup.name
                        + "'",
                        xmlDimensionLink,
                        "dimension");
                }
                if (xmlDimensionLink
                    instanceof MondrianDef.ForeignKeyLink)
                {
                    addRegularLink(
                        fact,
                        measureGroup,
                        dimension,
                        (MondrianDef.ForeignKeyLink)
                            xmlDimensionLink);
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
                    instanceof MondrianDef.NoLink)
                {
                    // safe to ignore dimension
                } else {
                    throw Util.newInternal(
                        "Unknown link type " + xmlDimensionLink);
                }
            }

            for (RolapCubeDimension dimension : unlinkedDimensions) {
                missingLinkAction.handle(
                    handler,
                    "No link for dimension '" + dimension.getName()
                    + "' in measure group '" + measureGroup.getName() + "'",
                    xmlMeasureGroup,
                    null);
            }
        }

        cube.init(measureList, measureList.get(0));

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
                final RolapSchema.PhysExpr expr = measure.getExpr();
                if (expr instanceof RolapSchema.PhysColumn) {
                    RolapSchema.PhysColumn column =
                        (RolapSchema.PhysColumn) expr;
                    assert measureGroup.getFactRelation() == column.relation;
                }
                RolapStar star = measureGroup.getStar();
                RolapStar.Table table = star.getFactTable();
                final RolapBaseCubeMeasure measure1 =
                    (measure instanceof
                        RolapCubeHierarchy.RolapCubeStoredMeasure
                        ? (RolapBaseCubeMeasure)
                        ((RolapCubeHierarchy.RolapCubeStoredMeasure) measure)
                            .getRolapMember()
                        : (RolapBaseCubeMeasure) measure);
                table.makeMeasure(measure1);
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

        if (xmlCube.defaultMeasure != null) {
            final RolapMember defaultMeasure =
                findMeasure(measureList, xmlCube.defaultMeasure);
            if (defaultMeasure == null) {
                handler.error(
                    "Default measure '" + xmlCube.defaultMeasure
                    + "' not found",
                    xmlCube,
                    "defaultMeasure");
            } else {
                cube.getMeasuresHierarchy().setDefaultMember(defaultMeasure);
            }
        }

        checkOrdinals(cube.getName(), measureList);
        if (Util.deprecated(false, false)) {
            cube.setAggGroup(
                ExplicitRules.Group.make(
                    cube, (Mondrian3Def.Cube) (Object) xmlCube));
        }
    }

    private static RolapMember findMeasure(
        List<RolapMember> memberList, String name)
    {
        for (RolapMember member : memberList) {
            if (Util.equalName(member.getName(), name)) {
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
        for (RolapHierarchy hierarchy : dimension.getHierarchyList()) {
            registerHierarchy(measureGroup, hierarchy);
        }
        for (RolapAttribute attribute : dimension.attributeMap.values()) {
            registerAttribute(measureGroup, dimension, attribute, path);
        }
    }

    private void registerHierarchy(
        RolapMeasureGroup measureGroup,
        RolapHierarchy hierarchy)
    {
        for (RolapLevel level : hierarchy.getLevelList()) {
            registerLevel(measureGroup, level);
        }
    }

    private void registerLevel(
        RolapMeasureGroup measureGroup,
        RolapLevel level)
    {
        final RolapLevel peer = level.getClosedPeer();
        if (peer != null) {
            registerLevel(measureGroup, peer);
        }
    }

    private void registerAttribute(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        RolapAttribute attribute,
        RolapSchema.PhysPath path)
    {
        for (RolapSchema.PhysColumn column : attribute.keyList) {
            registerExpr(
                measureGroup, dimension, path, column, attribute.name, "Key");
        }
        registerExpr(
            measureGroup, dimension, path, attribute.nameExp, attribute.name,
            "Name");
        registerExpr(
            measureGroup, dimension, path, attribute.captionExp, attribute.name,
            "Caption");
        for (RolapSchema.PhysColumn column : attribute.orderByList) {
            registerExpr(
                measureGroup, dimension, path, column, attribute.name,
                "OrderBy");
        }

        // No need to register properties, or the parent attribute. They are all
        // based on attributes in this dimension, so will be registered in due
        // course.
    }

    /**
     * Registers an expression as a column.
     *
     * @param measureGroup Measure group
     * @param dimension
     * @param path Path from measure group's fact table to root of dimension
     * @param expr Expression to register
     * @param name Name of level (for descriptive purposes)
     * @param property Property of level (for descriptive purposes, may be null)
     */
    private static void registerExpr(
        RolapMeasureGroup measureGroup,
        RolapCubeDimension dimension,
        RolapSchema.PhysPath path,
        RolapSchema.PhysColumn expr,
        String name,
        String property)
    {
        assert path != null;
        if (expr == null) {
            return;
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
                "Could not find path to " + expr.relation);
        }
        final RolapStar star = measureGroup.getStar();
        RolapStar.Table starTable = star.getFactTable();
        for (RolapSchema.PhysHop hop : pathBuilder.done().hopList) {
            if (hop.link != null) {
                starTable =
                    starTable.findChild(
                        hop.relation,
                        new RolapStar.Condition(hop.link),
                        true);
            }
        }
        final RolapStar.Column starColumn =
            starTable.lookupColumnByExpression(
                expr, true, name, property);
        assert starColumn != null;
        measureGroup.starColumnMap.put(
            Pair.of(dimension, expr),
            starColumn);
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
        RolapSchema.PhysExpr measureExp;

        RolapSchema.PhysRelation table =
            last(measureGroupTable, xmlMeasure.table, xmlMeasure, "table");
        if (table == null) {
            throw MondrianResource.instance()
                .MeasureWithColumnMustHaveTable.ex(
                    measureGroup.getCube().getName(), xmlMeasure.name);
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
                    measureGroup.getCube().getName(), xmlMeasure.name);
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
                    ? new Dialect.Datatype[0]
                    : new Dialect.Datatype[] {measureExp.getDatatype()});
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

        final RolapBaseCubeMeasure measure =
            new RolapBaseCubeMeasure(
                measureGroup.getCube(),
                measureGroup,
                null,
                measureGroup.getCube()
                    .getHierarchyList().get(0).getLevelList().get(0),
                xmlMeasure.name,
                xmlMeasure.caption,
                xmlMeasure.description,
                xmlMeasure.formatString,
                measureExp,
                aggregator,
                datatype,
                RolapSchemaLoader.createAnnotationMap(
                    xmlMeasure.getAnnotations()));
        measureGroup.measureList.add(measure);
        validator.putXml(measure, xmlMeasure);

        final String cellFormatterClassName;
        final Scripts.ScriptDefinition scriptDefinition;
        final MondrianDef.CellFormatter xmlCellFormatter =
            xmlMeasure.getCellFormatter();
        if (xmlCellFormatter != null) {
            cellFormatterClassName = xmlCellFormatter.className;
            scriptDefinition = toScriptDef(xmlCellFormatter.script);
        } else {
            cellFormatterClassName = xmlMeasure.formatter;
            scriptDefinition = null;
        }
        if (cellFormatterClassName != null || scriptDefinition != null) {
            try {
                CellFormatter cellFormatter =
                    getCellFormatter(
                        cellFormatterClassName,
                        scriptDefinition);
                measure.setFormatter(cellFormatter);
            } catch (Exception e) {
                throw MondrianResource.instance().CellFormatterLoadFailed.ex(
                    cellFormatterClassName, measure.getUniqueName(), e);
            }
        }

        // Set member's caption, if present.
        if (!Util.isEmpty(xmlMeasure.caption)) {
            // there is a special caption string
            measure.setProperty(
                Property.CAPTION.name,
                xmlMeasure.caption);
        }

        // Set member's visibility, default true.
        final boolean visible = toBoolean(xmlMeasure.visible, true);
        measure.setProperty(Property.VISIBLE.name, visible);

        List<String> propNames = new ArrayList<String>();
        List<String> propExprs = new ArrayList<String>();
        validateMemberProps(
            measureGroup.getCube(),
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
                        measure.setOrdinal(
                            Integer.valueOf(
                                expr.substring(1, expr.length() - 1)));
                    } catch (NumberFormatException e) {
                        Util.discard(e);
                    }
                }
            }
        }
        return measure;
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

    private void addRegularLink(
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

        if (dimension.rolapDimension.keyAttribute == null) {
            getHandler().error(
                "Dimension '"
                + dimension.getName()
                + "' is used in a dimension link but has no key attribute. Please "
                + "specify key.", xmlForeignKeyLink,
                null);
            return;
        }

        final List<RolapSchema.PhysColumn> keyList =
            dimension.rolapDimension.keyAttribute.keyList;

        if (foreignKeyList.size() != keyList.size()) {
            handler.error(
                "Number of foreign key columns "
                + foreignKeyList.size()
                + " does not match number of key columns "
                + keyList.size(),
                xmlForeignKeyLink.foreignKey,
                null);
        }

        /*
        // Construct the list of key columns.
        List<RolapSchema.PhysColumn> keyColumnList =
            new ArrayList<RolapSchema.PhysColumn>();
        Set<RolapSchema.PhysRelation> relations =
            new HashSet<RolapSchema.PhysRelation>();
        for (MondrianDef.Column xmlColumn
            : xmlForeignKeyLink.key.columns)
        {
            if (xmlColumn.table == null) {
                schema.error(
                    "Table must be specified",
                    schema.locate(xmlColumn, "table"),
                    null);
                continue;
            }
            final RolapSchema.PhysRelation relation =
                fact.getSchema().tablesByName.get(xmlColumn.table);
            if (relation == null) {
                schema.error(
                    "Unknown table '" + xmlColumn.table + "'",
                    schema.locate(xmlColumn, "table"),
                    null);
                continue;
            }
            relations.add(relation);
            final RolapSchema.PhysColumn column =
                relation.getColumn(xmlColumn.name, false);
            if (column == null) {
                schema.error(
                    "Unknown column '" + xmlColumn.name + "'",
                    schema.locate(xmlColumn, "name"),
                    null);
                continue;
            }
            keyColumnList.add(column);
        }
        if (relations.size() != 1) {
            schema.error(
                "foreign key columns come from different relations",
                schema.locate(xmlForeignKeyLink.foreignKey, null),
                null);
        }
        RolapSchema.PhysKey key =
            relations.iterator().next().lookupKey(
                keyColumnList, true);
        assert key != null;

        // Construct the list of foreign key columns.
        List<RolapSchema.PhysColumn> foreignKeyColumnList =
            new ArrayList<RolapSchema.PhysColumn>();
        for (MondrianDef.Column xmlColumn
            : xmlForeignKeyLink.foreignKey.columns)
        {
            if (xmlColumn.table != null
                && !xmlColumn.table.equals(
                measureGroup.getFactRelation().getAlias()))
            {
                schema.error(
                    "Foreign key columns linking a dimension to a fact table"
                    + " must be in the fact table, but was '"
                    + xmlColumn.table + "'",
                    schema.locate(xmlColumn, "table"),
                    null);
                continue;
            }
            final RolapSchema.PhysColumn column =
                measureGroup.getFactRelation().getColumn(xmlColumn.name, false);
            if (column == null) {
                schema.error(
                    "Unknown column '" + xmlColumn.name + "'",
                    schema.locate(xmlColumn, "name"),
                    null);
                continue;
            }
            foreignKeyColumnList.add(column);
        }

        // Check cardinality.
        if (keyColumnList.size() != foreignKeyColumnList.size()) {
            schema.error(
                "Column count mismatch",
                schema.locate(xmlForeignKeyLink, null),
                null);
            return;
        }
        */

        final RolapSchema.PhysPathBuilder pathBuilderOrig =
            new RolapSchema.PhysPathBuilder(fact)
                .add(dimension.rolapDimension.key.get(), foreignKeyList);
        RolapSchema.PhysPath path = pathBuilderOrig.clone().done();

        measureGroup.addLink(dimension, path);
        dimensionPaths.put(Pair.of(measureGroup, dimension), path);

        // Make sure that every attribute is registered as an expression in
        // the star.
        if (false)
        for (RolapAttribute attribute : dimension.attributeMap.values()) {
            RolapSchema.PhysSchemaGraph graph =
                measureGroup.getFactRelation().getSchema().getGraph();
            final RolapSchema.PhysRelation relation =
                uniqueTable(attribute.keyList);
            if (relation == null && false) {
                if (attribute.keyList.isEmpty()) {
                    throw Util.newInternal(
                        "attribute " + attribute + " has empty key");
                } else {
                    throw Util.newInternal(
                        "attribute " + attribute + " has key whose columns "
                        + "belong to inconsistent relations "
                        + attribute.keyList);
                }
            }
            for (RolapSchema.PhysColumn column : attribute.keyList) {
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
                RolapStar.Table starTable = star.getFactTable();
                for (RolapSchema.PhysHop hop : pathBuilder.done().hopList) {
                    if (hop.link != null) {
                        starTable =
                            starTable.findChild(
                                hop.relation,
                                new RolapStar.Condition(hop.link),
                                true);
                    }
                }
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
     * inconsitent.
     *
     * @param columnList List of columns
     * @return Null if list is empty or columns' relations are inconsitent
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
     * @param cubeName        name of the cube (required for error messages)
     * @param measures        measure list
     */
    private void checkOrdinals(
        String cubeName,
        List<RolapMember> measures)
    {
        Map<Integer, String> ordinals = new HashMap<Integer, String>();
        for (RolapMember measure : measures) {
            Integer ordinal = measure.getOrdinal();
            if (!ordinals.containsKey(ordinal)) {
                ordinals.put(ordinal, measure.getUniqueName());
            } else {
                throw MondrianResource.instance().MeasureOrdinalsNotUnique.ex(
                    cubeName,
                    ordinal.toString(),
                    ordinals.get(ordinal),
                    measure.getUniqueName());
            }
        }
    }

    /**
     * Adds a dimension to an existing cube.
     *
     * @param cube Cube
     * @param xml XML for dimension
     * @param xmlDimensionLinks For each measure group, XML linking measure
     *     group to dimension
     * @return New dimension
     */
    public RolapDimension createDimension(
        RolapCube cube,
        String xml,
        Map<String, String> xmlDimensionLinks)
    {
        MondrianDef.Dimension xmlDimension;
        try {
            final Parser xmlParser = XOMUtil.createDefaultParser();
            final DOMWrapper def = xmlParser.parse(xml);
                xmlDimension = new MondrianDef.Dimension(def);
        } catch (XOMException e) {
            throw Util.newError(
                e,
                "Error while adding dimension to cube '" + cube
                + "' from XML [" + xml + "]");
        }
        return createDimension(this, cube, xmlDimension, null);
    }

    /**
     * Creates a dimension.
     *
     * @param schemaLoader
     * @param cube
     * @param xmlCubeDimension
     * @param xmlSchema
     * @return Dimension; or null on error
     */
    RolapCubeDimension createDimension(
        RolapSchemaLoader schemaLoader,
        RolapCube cube,
        MondrianDef.Dimension xmlCubeDimension,
        MondrianDef.Schema xmlSchema)
    {
        RolapCubeDimension dimension =
            getOrCreateDimension(
                cube,
                xmlCubeDimension,
                schema,
                xmlSchema,
                cube.getDimensionList().size(),
                cube.hierarchyList,
                createAnnotationMap(
                    xmlCubeDimension.getAnnotations()));

        if (dimension == null) {
            return null;
        }

        // add to dimensions array
        cube.addDimension(dimension);

        schemaLoader.initDimension(dimension);

        return dimension;
    }

    /**
     * Creates a dimension from its XML definition. If the XML definition's
     * 'source' attribute is set, and the shared dimension is cached in the
     * schema, returns that.
     *
     * @param xmlCubeDimension XML Dimension or DimensionUsage
     * @param schema Schema
     * @param xmlSchema XML Schema
     * @param dimensionOrdinal Ordinal of dimension
     * @param cubeHierarchyList List of hierarchies in cube
     * @param annotationMap Annotations
     * @return A dimension, or null if shared dimension does not exist
     */
    private RolapCubeDimension getOrCreateDimension(
        RolapCube cube,
        MondrianDef.Dimension xmlCubeDimension,
        RolapSchema schema,
        MondrianDef.Schema xmlSchema,
        int dimensionOrdinal,
        List<RolapCubeHierarchy> cubeHierarchyList,
        final Map<String, Annotation> annotationMap)
    {
        final String dimensionName;
        final String dimensionSource;
        final MondrianDef.Dimension xmlDimension;
        if (xmlCubeDimension.source != null) {
            if (xmlCubeDimension.key != null) {
                getHandler().warning(
                    "Attribute '"
                    + "key"
                    + "' must not be specified in dimension that references "
                    + "other dimension",
                    xmlCubeDimension,
                    "source");
            }
            xmlDimension =
                xmlSchema.getDimensions().get(xmlCubeDimension.source);
            if (xmlDimension == null) {
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
            dimensionName =
                first(xmlCubeDimension.name, xmlCubeDimension.source);
            dimensionSource = xmlCubeDimension.source;
        } else {
            xmlDimension = xmlCubeDimension;
            dimensionName = xmlDimension.name;
            dimensionSource = null;
        }
        final DimensionType dimensionType =
            xmlDimension.type == null
                ? DimensionType.StandardDimension
                : DimensionType.valueOf(xmlDimension.type);
        final RolapDimension dimension =
            new RolapDimension(
                schema,
                xmlDimension.name,
                toBoolean(xmlDimension.visible, true),
                xmlDimension.caption,
                xmlDimension.description,
                dimensionType,
                Collections.<String, Annotation>emptyMap());
        validator.putXml(dimension, xmlDimension);

        // Load attributes before hierarchies, because levels refer to
        // attributes. Likewise create attributes before properties.
        final RolapSchema.PhysRelation dimensionRelation =
            xmlDimension.table == null
                ? null
                : getPhysRelation(
                    xmlDimension.table, xmlDimension, "table");

        List<RolapAttribute> attributeList = new ArrayList<RolapAttribute>();
        for (MondrianDef.Attribute xmlAttribute : xmlDimension.getAttributes())
        {
            RolapAttribute attribute =
                createAttribute(xmlAttribute, dimensionRelation, dimension);
            dimension.attributeMap.put(attribute.name, attribute);
            attributeList.add(attribute);
        }

        // Attributes and parent-child links.
        int attributeHierarchyCount = 0;
        for (RolapAttribute attribute : attributeList) {
            final MondrianDef.Attribute xmlAttribute =
                validator.getXml(attribute, true);

            if (xmlAttribute.parent != null) {
                attribute.parentAttribute =
                    dimension.attributeMap.get(xmlAttribute.parent);
                if (attribute.parentAttribute == null) {
                    getHandler().error(
                        "Unknown parent attribute ''",
                        xmlAttribute,
                        "parent");
                }
            }

            if (toBoolean(xmlAttribute.hasHierarchy, false)) {
                ++attributeHierarchyCount;
            }

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
        }

        // Use a Lazy variable to initialize dimension's key on first use.
        // Degenerate dimensions don't need keys.
        // But regular dimensions do, and they need to be validated using
        // information only available right here.
        dimension.key =
            new Lazy<RolapSchema.PhysKey>(
                new Util.Functor0<RolapSchema.PhysKey>() {
                    public RolapSchema.PhysKey apply() {
                        return lookupKey(
                            xmlDimension,
                            dimension,
                            true);
                    }
                }
            );

        for (MondrianDef.Hierarchy xmlHierarchy : xmlDimension.getHierarchies())
        {
            // If hierarchy is not named, name is same as dimension.
            String hierarchyName = first(xmlHierarchy.name, xmlDimension.name);
            final String uniqueName =
                xmlDimension.getHierarchies().size() == 1
                && hierarchyName.equals(xmlDimension.name)
                    ? dimension.getUniqueName()
                    : Util.makeFqName(dimension, hierarchyName);
            RolapHierarchy hierarchy =
                new RolapHierarchy(
                    dimension,
                    hierarchyName,
                    uniqueName,
                    toBoolean(xmlHierarchy.visible, true),
                    xmlHierarchy.caption,
                    xmlHierarchy.description,
                    toBoolean(xmlHierarchy.hasAll, true),
                    null,
                    createAnnotationMap(xmlHierarchy.getAnnotations()));
            validator.putXml(hierarchy, xmlHierarchy);
            dimension.addHierarchy(hierarchy);
            hierarchy.initHierarchy(
                this,
                xmlHierarchy.allLevelName,
                xmlHierarchy.allMemberName,
                xmlHierarchy.allMemberCaption);
            hierarchy.init1(
                this,
                null);
            if (xmlHierarchy.getLevels().size() == 0) {
                throw MondrianResource.instance().HierarchyHasNoLevels.ex(
                    hierarchy.getUniqueName());
            }
            Set<String> levelNameSet = new HashSet<String>();
            for (MondrianDef.Level xmlLevel : xmlHierarchy.getLevels()) {
                String name = first(xmlLevel.name, xmlLevel.attribute);
                if (!levelNameSet.add(name)) {
                    throw MondrianResource.instance()
                        .HierarchyLevelNamesNotUnique.ex(
                            hierarchy.getUniqueName(), name);
                }
            }
            for (MondrianDef.Level xmlLevel : xmlHierarchy.getLevels()) {
                hierarchy.levelList.add(
                    createLevel(
                        hierarchy, hierarchy.levelList.size(), xmlLevel));
            }
            hierarchy.init2(
                this,
                xmlHierarchy.defaultMember);
        }

        for (RolapAttribute attribute : attributeList) {
            final MondrianDef.Attribute xmlAttribute =
                validator.getXml(attribute, true);
            if (toBoolean(
                    xmlAttribute.hasHierarchy,
                    countHierarchies(
                        dimension.getHierarchyList(), attribute) == 0))
            {
                // Create attribute hierarchy.
                final String uniqueName =
                    Util.makeFqName(dimension, xmlAttribute.name);
                RolapHierarchy hierarchy =
                    new RolapHierarchy(
                        dimension,
                        xmlAttribute.name,
                        uniqueName,
                        toBoolean(xmlAttribute.visible, true),
                        xmlAttribute.caption,
                        xmlAttribute.description,
                        true, // REVIEW: add Attribute@hierarchyHasAll?
                        null,
                        Collections.<String, Annotation>emptyMap());
                dimension.addHierarchy(hierarchy);
                hierarchy.initHierarchy(
                    this,
                    null, // REVIEW: add Attribute@hierarchyAllLevelName?
                    null, // REVIEW: add Attribute@hierarchyAllMemberName?
                    null); // REVIEW: add Attribute@hierarchyAllMemberCaption?
                hierarchy.init1(
                    this,
                    null);
                hierarchy.levelList.add(
                    new RolapLevel(
                        hierarchy,
                        xmlAttribute.name,
                        toBoolean(xmlAttribute.visible, true),
                        xmlAttribute.caption,
                        xmlAttribute.description,
                        hierarchy.hasAll() ? 1 : 0,
                        attribute,
                        RolapLevel.HideMemberCondition.Never,
                        Collections.<String, Annotation>emptyMap()));
                hierarchy.init2(
                    this,
                    null);
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
        final RolapCubeDimension cubeDimension = new RolapCubeDimension(
            this,
            cube,
            dimension,
            dimensionName,
            dimensionSource,
            xmlDimension.caption,
            xmlDimension.description,
            dimensionOrdinal,
            cubeHierarchyList,
            annotationMap);
        validator.putXml(cubeDimension, xmlCubeDimension);

        // Populate attribute map. (REVIEW: Should attributes go ONLY in the
        // cube dimension?)
        cubeDimension.attributeMap.putAll(dimension.attributeMap);

        return cubeDimension;
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
     * @param dimension Dimension
     * @param create Whether to create a key if not found
     * @return Key, or null if not found
     */
    private RolapSchema.PhysKey lookupKey(
        MondrianDef.Dimension xmlDimension,
        RolapDimension dimension,
        boolean create)
    {
        // Find the unique relation of the attribute's list of key columns.
        final RolapSchema.PhysRelation relation = uniqueRelation(dimension);
        if (relation == null) {
            return null;
        }
        for (RolapSchema.PhysKey key : relation.getKeyList()) {
            if (key.columnList.equals(dimension.keyAttribute.keyList)) {
                return key;
            }
        }
        if (create) {
            return relation.addKey(
                "k$" + relation.getKeyList().size(),
                dimension.keyAttribute.keyList);
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
        for (RolapSchema.PhysColumn key : dimension.keyAttribute.keyList) {
            relations.add(key.relation);
        }
        return relations.size() != 1 ? null : relations.iterator().next();
    }

    private void validateDimensionType(
        RolapDimension dimension)
    {
        final DimensionType dimensionType = dimension.getDimensionType();
        for (RolapHierarchy hierarchy : dimension.getHierarchyList()) {
            for (RolapLevel level : hierarchy.getLevelList()) {
                if (level.getLevelType().isTime()
                    && dimensionType != DimensionType.TimeDimension)
                {
                    getHandler().error(
                        MondrianResource.instance().TimeLevelInNonTimeHierarchy
                            .ex(dimension.getUniqueName()),
                        validator.getXmls(level, hierarchy, dimension),
                        null);
                }
                if (!level.getLevelType().isTime()
                    && !level.isAll()
                    && dimensionType == DimensionType.TimeDimension)
                {
                    getHandler().error(
                        MondrianResource.instance().NonTimeLevelInTimeHierarchy
                            .ex(dimension.getUniqueName()),
                        validator.getXmls(level, hierarchy, dimension),
                        null);
                }
            }
        }
    }

    /**
     * Initializes a dimension within the context of a cube.
     */
    void initDimension(RolapCubeDimension dimension) {
        for (RolapCubeHierarchy hierarchy : dimension.getHierarchyList()) {
            initHierarchy(hierarchy);
        }
    }

    /**
     * Initializes a hierarchy within the context of a cube.
     */
    void initHierarchy(RolapCubeHierarchy hierarchy) {
        hierarchy.init1(this, null);
        for (RolapCubeLevel level : hierarchy.getLevelList()) {
            initLevel(level);
        }
        final MondrianDef.Hierarchy xmlHierarchy =
            (MondrianDef.Hierarchy) validator.getXml(
                hierarchy.getRolapHierarchy(), false);
        hierarchy.init2(
            this,
            xmlHierarchy == null
                ? null
                : xmlHierarchy.defaultMember);
    }

    private void initLevel(RolapLevel level)
    {
        level.initLevel(this, false);
    }

    RolapAttribute createAttribute(
        MondrianDef.Attribute xmlAttribute,
        RolapSchema.PhysRelation inheritedRelation,
        RolapDimension dimension)
    {
        final RolapSchema.PhysRelation relation =
            last(
                inheritedRelation, xmlAttribute.table, xmlAttribute, "table");
        final List<RolapSchema.PhysColumn> keyList =
            createColumnList(
                xmlAttribute,
                "keyColumn",
                relation,
                xmlAttribute.keyColumn,
                xmlAttribute.getKey());
        if (keyList.size() == 0) {
            getHandler().error(
                "Attribute must have a key",
                xmlAttribute,
                null);
            return null;
        }
        RolapSchema.PhysColumn nameExpr =
            createColumn(
                xmlAttribute,
                "nameColumn",
                relation,
                xmlAttribute.nameColumn,
                xmlAttribute.getName_());
        if (nameExpr == null) {
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
        }
        RolapSchema.PhysColumn captionExpr =
            createColumn(
                xmlAttribute,
                "captionColumn",
                relation,
                xmlAttribute.captionColumn,
                xmlAttribute.getCaption());
        if (captionExpr == null) {
            captionExpr = nameExpr;
        }
        List<RolapSchema.PhysColumn> orderByList =
            createColumnList(
                xmlAttribute,
                "orderByColumn",
                relation,
                xmlAttribute.orderByColumn,
                xmlAttribute.getOrderBy());

        // Cannot deal with parent attribute yet; it might not have been
        // created. We will do a second pass, after we have created all
        // attributes in this dimension.

        final MondrianDef.Closure closure = xmlAttribute.getClosure();
        if (closure != null) {
            // TODO:
            Util.discard(closure.childColumn);
            Util.discard(closure.parentColumn);
            Util.discard(closure.table);
        }

        final int approxRowCount =
            loadApproxRowCount(xmlAttribute.approxRowCount);
        final org.olap4j.metadata.Level.Type levelType =
            stringToLevelType(
                xmlAttribute.levelType);

        MemberFormatter memberFormatter = null;
        final MondrianDef.MemberFormatter xmlMemberFormatter =
            xmlAttribute.getMemberFormatter();
        if (xmlMemberFormatter != null) {
            final String memberFormatterClassName =
                xmlMemberFormatter.className;
            final Scripts.ScriptDefinition scriptDefinition =
                toScriptDef(xmlMemberFormatter.script);
            if (memberFormatterClassName != null || scriptDefinition != null) {
                try {
                    memberFormatter =
                        RolapSchemaLoader.getMemberFormatter(
                            memberFormatterClassName,
                            scriptDefinition);
                } catch (Exception e) {
                    throw MondrianResource.instance().MemberFormatterLoadFailed
                        .ex(
                            memberFormatterClassName, xmlAttribute.name, e);
                }
            }
        }

        final RolapAttribute attribute =
            new RolapAttribute(
                xmlAttribute.name,
                toBoolean(xmlAttribute.visible, true),
                xmlAttribute.caption,
                xmlAttribute.description,
                keyList,
                nameExpr,
                captionExpr,
                orderByList,
                memberFormatter,
                xmlAttribute.nullValue,
                levelType,
                approxRowCount);

        validator.putXml(attribute, xmlAttribute);
        return attribute;
    }

    private org.olap4j.metadata.Level.Type stringToLevelType(
        String levelTypeString)
    {
        levelTypeString = levelTypeString.toUpperCase();
        // For mondrian-3 compatibility, convert "TIMEYEARS" to "TIME_YEARS" etc
        if (levelTypeString.startsWith("TIME")
            && !levelTypeString.startsWith("TIME_"))
        {
            levelTypeString = "TIME_" + levelTypeString.substring(4);
        }
        return Util.lookup(
            org.olap4j.metadata.Level.Type.class,
            levelTypeString.toUpperCase());
    }

    public Handler getHandler() {
        return handler;
    }

    RolapLevel createLevel(
        RolapHierarchy hierarchy,
        int depth,
        MondrianDef.Level xmlLevel)
    {
        if (xmlLevel.attribute == null) {
            getHandler().error(
                "Attribute 'attribute' is required",
                xmlLevel,
                "attribute");
            return null;
        }

        RolapAttribute attribute =
            hierarchy.getDimension().attributeMap.get(xmlLevel.attribute);
        if (attribute == null) {
            getHandler().error(
                "Attribute '" + xmlLevel.attribute
                + "' not found in Dimension '"
                + hierarchy.getDimension().getName() + "'",
                xmlLevel,
                "attribute");
            return null;
        }

        final RolapLevel level =
            new RolapLevel(
                hierarchy,
                first(xmlLevel.name, xmlLevel.attribute),
                toBoolean(xmlLevel.visible, true),
                xmlLevel.caption,
                xmlLevel.description,
                depth,
                attribute,
                RolapLevel.HideMemberCondition.valueOf(xmlLevel.hideMemberIf),
                createAnnotationMap(xmlLevel.getAnnotations()));
        validator.putXml(level, xmlLevel);
        return level;
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
                xml,
                attributeName,
                relation,
                columnName,
                xmlKey);
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

    private List<RolapSchema.PhysColumn> createColumnList(
        ElementDef xml,
        String attributeName,
        RolapSchema.PhysRelation table,
        String columnName,
        MondrianDef.Columns xmlColumns)
    {
        if (columnName != null) {
            if (xmlColumns != null) {
                // Post an error, and coninue, ignoring xmlKey.
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
            return Collections.singletonList(
                physSchemaBuilder.toPhysColumn(
                    table, columnName));
        }
        final List<RolapSchema.PhysColumn> list =
            new ArrayList<RolapSchema.PhysColumn>();
        if (xmlColumns != null) {
            for (MondrianDef.Column xmlColumn : xmlColumns.array) {
                list.add(
                    getPhysColumn(
                        last(table, xmlColumn.table, xmlColumn, "table"),
                        xmlColumn.name,
                        xmlColumn,
                        "name"));
            }
        }
        return list;
    }

    /**
     * Gets a column. It is a user error if the column does not exist.
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
        MondrianDef.Column xmlColumn,
        String attributeName)
    {
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

    static <T> T last(T s0, T s1) {
        if (s1 != null) {
            return s1;
        }
        return s0;
    }

    static <T> T first(T s0, T s1) {
        if (s0 != null) {
            return s0;
        }
        return s1;
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
            final String name = first(xmlProperty.name, xmlProperty.attribute);
            final RolapAttribute sourceAttribute =
                dimension.attributeMap.get(xmlProperty.attribute);
            if (sourceAttribute == null) {
                getHandler().error(
                    "Unknown attribute '" + xmlProperty.attribute + "'",
                    xmlProperty,
                    "attribute");
                continue;
            }
            final RolapProperty property =
                new RolapProperty(
                    name,
                    attribute,
                    sourceAttribute,
                    dialectToPropertyDatatype(sourceAttribute.getDatatype()),
                    makePropertyFormatter(xmlProperty),
                    xmlProperty.caption,
                    false,
                    xmlProperty.description);
            validator.putXml(property, xmlProperty);
            propertyList.add(property);
        }
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
            return Property.Datatype.TYPE_OTHER;
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
        if (xmlProperty.propertyFormatter != null) {
            propertyFormatterClassName =
                xmlProperty.propertyFormatter.className;
            scriptDefinition =
                toScriptDef(xmlProperty.propertyFormatter.script);
        } else {
            propertyFormatterClassName = xmlProperty.formatter;
            scriptDefinition = null;
        }
        if (propertyFormatterClassName != null
            || scriptDefinition != null)
        {
            try {
                return RolapSchemaLoader.createPropertyFormatter(
                    propertyFormatterClassName,
                    scriptDefinition);
            } catch (Exception e) {
                throw MondrianResource.instance()
                    .PropertyFormatterLoadFailed.ex(
                        propertyFormatterClassName, xmlProperty.name, e);
            }
        } else {
            return null;
        }
    }

    static Map<String, Annotation> createAnnotationMap(
        List<MondrianDef.Annotation> annotations)
    {
        if (annotations == null
            || annotations.size() == 0)
        {
            return Collections.emptyMap();
        }
        // Use linked hash map because it retains order.
        final Map<String, Annotation> map =
            new LinkedHashMap<String, Annotation>();
        for (MondrianDef.Annotation annotation : annotations) {
            final String name = annotation.name;
            final String value = annotation.cdata;
            map.put(
                annotation.name,
                new Annotation() {
                    public String getName() {
                        return name;
                    }

                    public Object getValue() {
                        return value;
                    }
                });
        }
        return map;
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
        final String key,
        final ByteString md5Bytes,
        final String catalogUrl,
        final String catalogStr,
        final Util.PropertyList connectInfo,
        final DataSource dataSource)
    {
        final RolapSchemaLoader schemaLoader = new RolapSchemaLoader(null);
        return schemaLoader.loadStage0(
            key, md5Bytes, catalogUrl, catalogStr, connectInfo, dataSource);
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
                    new Id.Segment(
                        xmlNamedSet.name,
                        Id.Quoting.UNQUOTED)),
                exp);
        return formula.getNamedSet();
    }

    private Role createRole(
        MondrianDef.Role xmlRole,
        Map<String, Role> mapNameToRole)
    {
        final MondrianDef.Union xmlUnion = xmlRole.getUnion();
        if (xmlUnion != null) {
            if (xmlRole.getSchemaGrants().size() > 0) {
                throw MondrianResource.instance().RoleUnionGrants.ex();
            }
            List<Role> roleList = new ArrayList<Role>();
            for (MondrianDef.RoleUsage roleUsage : xmlUnion.roleUsages) {
                final Role role = mapNameToRole.get(roleUsage.roleName);
                if (role == null) {
                    throw MondrianResource.instance().UnknownRole.ex(
                        roleUsage.roleName);
                }
                roleList.add(role);
            }
            return RoleImpl.union(roleList);
        }
        RoleImpl role = new RoleImpl();
        for (MondrianDef.SchemaGrant schemaGrant : xmlRole.getSchemaGrants()) {
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
        return role;
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
                        schema,
                        xmlCube.name,
                        toBoolean(xmlCube.visible, true),
                        xmlCube.caption,
                        xmlCube.description,
                        createAnnotationMap(xmlCube.getAnnotations()),
                        xmlSchema.measuresCaption);
                validator.putXml(cube, xmlCube);
//            } else if (tagName.equals("VirtualCube"))
//                 Need the real schema here.
//                MondrianDef.Schema xmlSchema = getXMLSchema();
//                MondrianDef.VirtualCube xmlDimension =
//                    new MondrianDef.VirtualCube(def);
//                cube = new RolapCube(
//                    this, xmlSchema, syntheticPhysSchema, xmlDimension);
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
                xmlCalcMembers, calculatedMemberList, i, queryExp, memberList);
        }
        for (int i = 0; i < xmlNamedSets.size(); i++) {
            postNamedSet(
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

        final List<Formula> calculatedMemberList = new ArrayList<Formula>();

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
                        final Query queryExp =
                            (Query) conn.parseStatement(
                                Locus.peek().execution.getMondrianStatement(),
                                queryString, null, false);
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
        if (xmlNamedSet.caption != null
            && xmlNamedSet.caption.length() > 0)
        {
            namedSet.setCaption(xmlNamedSet.caption);
        }

        if (xmlNamedSet.description != null
            && xmlNamedSet.description.length() > 0)
        {
            namedSet.setDescription(xmlNamedSet.description);
        }

        namedSet.setAnnotationMap(
            createAnnotationMap(xmlNamedSet.getAnnotations()));

        formulaList.add(formula);
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
        boolean visible = toBoolean(xmlCalcMember.visible, true);
        member.setProperty(Property.VISIBLE.name, visible);

        if (xmlCalcMember.caption != null
            && xmlCalcMember.caption.length() > 0)
        {
            member.setProperty(
                Property.CAPTION.name, xmlCalcMember.caption);
        }

        if (xmlCalcMember.description != null
            && xmlCalcMember.description.length() > 0)
        {
            member.setProperty(
                Property.DESCRIPTION.name, xmlCalcMember.description);
        }

        RolapMember member1 = RolapUtil.strip(member);
        ((RolapCalculatedMember) member1).setAnnotationMap(
            createAnnotationMap(xmlCalcMember.getAnnotations()));

        memberList.add(member);
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
        if (xmlCalcMember.dimension != null) {
            dimName = xmlCalcMember.dimension;
            final Dimension dimension =
                cube.lookupDimension(
                    new Id.Segment(
                        xmlCalcMember.dimension,
                        Id.Quoting.UNQUOTED));
            if (dimension != null) {
                hierarchy = dimension.getHierarchy();
            }
        } else if (xmlCalcMember.hierarchy != null) {
            dimName = xmlCalcMember.hierarchy;
            hierarchy = (Hierarchy)
                cube.getSchemaReader().lookupCompound(
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

        // Generate SQL.
        assert fqName.startsWith("[");
        buf.append("MEMBER ")
            .append(fqName)
            .append(Util.nl)
            .append("  AS ");
        String result;
        if (xmlCalcMember.getFormula() != null) {
            result = xmlCalcMember.getFormula().cdata;
        } else {
            result = xmlCalcMember.formula;
        }
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
     * Given the name of a cell formatter class and/or a cell formatter script,
     * returns a cell formatter.
     *
     * @param className Name of cell formatter class
     * @param script Script
     * @return Cell formatter
     * @throws Exception if class cannot be instantiated
     */
    static CellFormatter getCellFormatter(
        String className,
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
            @SuppressWarnings({"unchecked"})
            Class<CellFormatter> clazz =
                (Class<CellFormatter>) Class.forName(className);
            Constructor<CellFormatter> ctor = clazz.getConstructor();
            return ctor.newInstance();
        } else {
            return Scripts.cellFormatter(script);
        }
    }

    /**
     * Given the name of a member formatter class, returns a member formatter.
     *
     * @param className Name of cell formatter class
     * @param script Script
     * @return Member formatter
     * @throws Exception if class cannot be instantiated
     */
    static MemberFormatter getMemberFormatter(
        String className,
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
            @SuppressWarnings({"unchecked"})
            Class<MemberFormatter> clazz =
                (Class<MemberFormatter>) Class.forName(className);
            Constructor<MemberFormatter> ctor = clazz.getConstructor();
            return ctor.newInstance();
        } else {
            return Scripts.memberFormatter(script);
        }
    }

    /**
     * Given the name of a property formatter class, returns a propert
     * formatter.
     *
     * @param className Name of property formatter class
     * @param script Script
     * @return Property formatter
     * @throws Exception if class cannot be instantiated
     */
    static PropertyFormatter createPropertyFormatter(
        String className,
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
            @SuppressWarnings({"unchecked"})
            Class<PropertyFormatter> clazz =
                (Class<PropertyFormatter>) Class.forName(className);
            Constructor<PropertyFormatter> ctor = clazz.getConstructor();
            return ctor.newInstance();
        } else {
            return Scripts.propertyFormatter(script);
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
            new ArrayList<Formula>(),
            new ArrayList<Formula>(),
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
        for (RolapSchema.PhysColumn expr : level.getAttribute().keyList) {
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

    /**
     * Handler for errors that arise while loading a schema.
     */
    interface Handler {

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
                && sql.children[0] instanceof Mondrian3Def.Column)
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

        private static RolapSchema.PhysRelation soleRelation(
            List<RolapSchema.PhysExpr> list,
            RolapSchema.PhysRelation defaultRelation)
        {
            final HashSet<RolapSchema.PhysRelation> relations =
                new HashSet<RolapSchema.PhysRelation>(2);
            collectRelations(list, defaultRelation, relations);
            if (relations.size() == 1) {
                return relations.iterator().next();
            } else {
                return null;
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
         *    do not explicitly beong to a relation (e.g. '1' or 'count(*)').
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
        private static final Map<Object, NodeDef> map =
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
}

// End RolapSchemaLoader.java
