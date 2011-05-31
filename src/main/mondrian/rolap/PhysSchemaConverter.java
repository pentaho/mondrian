/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.sql.SqlQuery;
import org.eigenbase.xom.*;

import java.util.*;

/**
 * Agent that converts an old-style schema to a new-style schema.
 * It builds a physical schema as it goes.
 */
class PhysSchemaConverter extends RolapSchemaLoader.PhysSchemaBuilder {
    /**
     * Maps converted (new-style) XML elements to the legacy element they
     * were created from. Allows us to tag error messages with the location
     * in the document of the legacy element (new elements did not appear
     * in the document).
     */
    private final Map<ElementDef, ElementDef> legacyMap =
        new HashMap<ElementDef, ElementDef>();

    private RolapSchemaLoader.PhysSchemaBuilder physSchemaBuilder;

    /**
     * Creates a PhysSchemaConverter.
     *
     * @param cube Cube
     * @param physSchema Physical schema
     */
    PhysSchemaConverter(
        RolapCube cube, RolapSchema.PhysSchema physSchema)
    {
        super(cube, physSchema);
    }

    void convertCube(Mondrian3Def.Cube cube)
    {
        final Mondrian3Def.Relation xmlFact = cube.fact;
        RolapSchema.PhysRelation fact =
            xmlFact == null
                ? null
                : RolapSchemaUpgrader.toPhysRelation(
                    physSchemaBuilder,
                    xmlFact);
    }

    /**
     * Converts a dimension from legacy format to new format.
     *
     * <p>For example,
     * <blockquote><pre>
     * &lt;Dimension name="Product"&gt;
     *   &lt;Hierarchy hasAll="true" primaryKey="product_id"
     *          primaryKeyTable="product"&gt;
     *     &lt;Join leftKey="product_class_id"
     *           rightKey="product_class_id"&gt;
     *       &lt;Table schema="sales" name="product" alias="p"/&gt;
     *       &lt;Table schema="sales" name="product_class" alias="pc"/&gt;
     *     &lt;/Join&gt;
     *     &lt;Level name="Product Family" table="product_class"
     *               column="product_family" uniqueMembers="true"/&gt;
     *     &lt;Level name="Product Department" table="product_class"
     *                column="product_department" uniqueMembers="false"/&gt;
     *     &lt;Level name="Product Category" table="product_class"
     *                column="product_category" uniqueMembers="false"/&gt;
     *     &lt;Level name="Product Subcategory" table="product_class"
     *              column="product_subcategory" uniqueMembers="false"/&gt;
     *     &lt;Level name="Brand Name" table="product" column="brand_name"
     *                uniqueMembers="false"/&gt;
     *     &lt;Level name="Product Name" table="product"
     *        column="product_name" ordinal="sku" uniqueMembers="true"/&gt;
     *   &lt;/Hierarchy&gt;
     * &lt;/Dimension&gt;
     * </pre></blockquote>
     *
     * becomes
     * <blockquote><pre>
     * &lt;PhysicalSchema&gt;
     *   &lt;Table schema="sales" name="product" alias="p"/&gt;
     *     &lt;Key&gt;
     *       &lt;Column name="product_id"/&gt;
     *     &lt;/Key&gt;
     *   &lt;/Table&gt;
     *   &lt;Table schema="sales" name="product_class" alias="pc"/&gt;
     *     &lt;Key&gt;
     *       &lt;Column name="product_class_id"/&gt;
     *     &lt;/Key&gt;
     *   &lt;/Table&gt;
     *   &lt;Link source="product_class" target="product"&gt;
     *     &lt;Key&gt;
     *       &lt;Column name="product_class_id"/&gt;
     *     &lt;/Key&gt;
     *   &lt;/Link&gt;
     * &lt;/PhysicalSchema&gt;
     * &lt;Dimension name="Product"&gt;
     *   &lt;Hierarchy hasAll="true"&gt;
     *     &lt;Level name="Product Family" uniqueMembers="true"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_family"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Level&gt;
     *       &lt;Level name="Product Department" uniqueMembers="false"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_department"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Level&gt;
     *     &lt;Level name="Product Category" uniqueMembers="false"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_category"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Level&gt;
     *     &lt;Level name="Product Subcategory" uniqueMembers="false"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="pc" column="product_subcategory"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Level&gt;
     *     &lt;Level name="Brand Name" uniqueMembers="false"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="p" column="brand_name"/&gt;
     *       &lt;/KeyExpression&gt;
     *     &lt;/Level&gt;
     *     &lt;Level name="Product Name" uniqueMembers="true"/&gt;
     *       &lt;KeyExpression&gt;
     *         &lt;Column table="p" column="product_name"/&gt;
     *       &lt;/KeyExpression&gt;
     *       &lt;OrdinalExpression&gt;
     *         &lt;Column table="p" column="sku"/&gt;
     *       &lt;/OrdinalExpression&gt;
     *     &lt;/Level&gt;
     *   &lt;/Hierarchy&gt;
     * &lt;/Dimension&gt;
     * </pre></blockquote>
     *
     *
     * @param xmlLegacyFact XML definition of fact table
     * @param xmlLegacyDimension Dimension in legacy format
     * @return converted dimension
     */
    public Mondrian3Def.Dimension convertDimension(
        List<Link> links,
        Mondrian3Def.RelationOrJoin xmlLegacyFact,
        Mondrian3Def.Dimension xmlLegacyDimension)
    {
        final Mondrian3Def.Dimension xmlDimension =
            new Mondrian3Def.Dimension();
        xmlDimension.name = xmlLegacyDimension.name;
        xmlDimension.hierarchies =
            new Mondrian3Def.Hierarchy[
                xmlLegacyDimension.hierarchies.length];
        xmlDimension.caption = xmlLegacyDimension.caption;
        xmlDimension.highCardinality = xmlLegacyDimension.highCardinality;
        xmlDimension.type = xmlLegacyDimension.type;
        xmlDimension.usagePrefix = xmlLegacyDimension.usagePrefix;
        for (int i = 0; i < xmlLegacyDimension.hierarchies.length; i++) {
            xmlDimension.hierarchies[i] =
                convertHierarchy(
                    links,
                    xmlLegacyFact,
                    xmlLegacyDimension.hierarchies[i]);
        }
        legacyMap.put(xmlDimension, xmlLegacyDimension);
        return xmlDimension;
    }

    private Mondrian3Def.Hierarchy convertHierarchy(
        List<Link> links,
        Mondrian3Def.RelationOrJoin xmlLegacyFact,
        Mondrian3Def.Hierarchy xmlLegacyHierarchy)
    {
        final Mondrian3Def.Hierarchy xmlHierarchy =
            new Mondrian3Def.Hierarchy();
        xmlHierarchy.allLevelName = xmlLegacyHierarchy.allLevelName;
        xmlHierarchy.allMemberCaption = xmlLegacyHierarchy.allMemberCaption;
        xmlHierarchy.allMemberName = xmlLegacyHierarchy.allMemberName;
        xmlHierarchy.caption = xmlLegacyHierarchy.caption;
        xmlHierarchy.defaultMember = xmlLegacyHierarchy.defaultMember;
        xmlHierarchy.hasAll = xmlLegacyHierarchy.hasAll;
        xmlHierarchy.name = xmlLegacyHierarchy.name;
        xmlHierarchy.memberReaderClass =
            xmlLegacyHierarchy.memberReaderClass;
        xmlHierarchy.levels =
            new Mondrian3Def.Level[xmlLegacyHierarchy.levels.length];
        xmlHierarchy.memberReaderParameters =
            xmlLegacyHierarchy.memberReaderParameters.clone();
        Map<String, RolapSchema.PhysRelation> relations =
            new HashMap<String, RolapSchema.PhysRelation>();
        final Mondrian3Def.RelationOrJoin xmlRelation =
            xmlLegacyHierarchy.relation != null
                ? xmlLegacyHierarchy.relation
                : xmlLegacyFact;
        RolapSchema.PhysRelation relation = null;
        if (xmlRelation == null) {
            throw getHandler().fatal(
                "Hierarchy in legacy-style schema must include a relation",
                xmlLegacyHierarchy,
                null);
        }

        final List<String> tableNames = new ArrayList<String>();
        gatherTableNames(xmlRelation, tableNames);
        if (xmlLegacyHierarchy.primaryKeyTable != null
            && !tableNames.contains(xmlLegacyHierarchy.primaryKeyTable))
        {
            getHandler().error(
                "Table '" + xmlLegacyHierarchy.primaryKeyTable
                + "' not found",
                xmlLegacyHierarchy,
                "primaryKeyTable");
            legacyMap.put(xmlHierarchy, xmlLegacyHierarchy);
            return xmlHierarchy;
        }

        for (Link link : links) {
            registerRelation(
                link.fact,
                xmlRelation,
                link.foreignKey,
                link.fact.getAlias(),
                xmlLegacyHierarchy.primaryKey,
                xmlLegacyHierarchy.primaryKeyTable,
                false,
                relations);
        }
        if (relations.size() == 1) {
            relation = relations.values().iterator().next();
        }
        for (int i = 0; i < xmlLegacyHierarchy.levels.length; i++) {
            xmlHierarchy.levels[i] =
                convertLevel(
                    i,
                    xmlLegacyHierarchy.levels[i],
                    relations,
                    relation,
                    links);
        }
        legacyMap.put(xmlHierarchy, xmlLegacyHierarchy);
        return xmlHierarchy;
    }

    private void gatherTableNames(
        Mondrian3Def.RelationOrJoin xmlRelation,
        List<String> tableNames)
    {
        if (xmlRelation instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) xmlRelation;
            gatherTableNames(join.left, tableNames);
            gatherTableNames(join.right, tableNames);
        } else {
            Mondrian3Def.Relation relation =
                (Mondrian3Def.Relation) xmlRelation;
            tableNames.add(relation.getAlias());
        }
    }

    private RolapSchema.PhysRelation registerRelation(
        RolapSchema.PhysRelation leftRelation,
        Mondrian3Def.RelationOrJoin relationOrJoin,
        String leftKeyColumnName,
        String leftAlias, // REVIEW: needed?
        String rightKeyColumnName,
        String rightAlias,
        boolean hardLink,
        Map<String, RolapSchema.PhysRelation> relations)
    {
        assert leftRelation != null;
        assert relationOrJoin != null;
        final RolapSchema.PhysRelation rightRelation;
        final RolapSchema.PhysRelation midRelation;
        if (relationOrJoin instanceof Mondrian3Def.Join) {
            Mondrian3Def.Join join = (Mondrian3Def.Join) relationOrJoin;
            midRelation =
                registerRelation(
                    leftRelation,
                    join.left,
                    leftKeyColumnName,
                    leftAlias,
                    rightKeyColumnName,
                    rightAlias,
                    false,
                    relations);
            rightRelation =
                registerRelation(
                    midRelation,
                    join.right,
                    join.leftKey,
                    join.leftAlias,
                    join.rightKey,
                    join.rightAlias,
                    true,
                    relations);
        } else {
            MondrianDef.Relation relation =
                (MondrianDef.Relation) relationOrJoin;
            // TODO: fail if alias exists
            midRelation = rightRelation = toPhysRelation(relation);
            if (rightRelation instanceof RolapSchema.PhysTable) {
                RolapSchema.PhysTable physTable =
                    (RolapSchema.PhysTable) rightRelation;
                physTable.ensurePopulated(
                    cube.schema,
                    relation);
            }

            RolapSchema.PhysKey rightKey;
            if (rightKeyColumnName != null) {
                final RolapSchema.PhysColumn rightColumn =
                    rightRelation.getColumn(rightKeyColumnName, false);
                if (rightColumn == null) {
                    // REVIEW: user error?
                    throw new IllegalArgumentException();
                }
                final List<RolapSchema.PhysColumn> keyColumnList =
                    Collections.singletonList(rightColumn);
                rightKey = rightRelation.lookupKey(keyColumnList, true);
                if (rightAlias != null && relation.getAlias() != null) {
                    if (!rightAlias.equals(relation.getAlias())) {
                        throw new IllegalArgumentException(
                            "right alias " + rightAlias + " != table alias "
                            + relation.getAlias());
                    }
                }
            } else {
                rightKey = null;
            }
            relations.put(rightRelation.getAlias(), rightRelation);
            if (leftKeyColumnName != null) {
                final RolapSchema.PhysColumn leftColumn =
                    leftRelation.getColumn(leftKeyColumnName, false);
                if (leftColumn == null) {
                    // This is correctly an internal error. Validation
                    // should have given a user error before this point.
                    throw Util.newInternal(
                        "Relation " + leftRelation
                        + " does not contain column " + leftKeyColumnName);
                }
                final List<RolapSchema.PhysColumn> leftKeyColumnList =
                    Collections.singletonList(leftColumn);
                assert rightKey != null;
                physSchema.addLink(
                    rightKey,
                    leftRelation,
                    leftKeyColumnList,
                    hardLink);
            }
        }

        if (rightKeyColumnName != null) {
            // Make sure right relation's key is the one we think we're
            // joining to. If not, since we don't support alternate
            // keys, we're hosed.
            final RolapSchema.PhysColumn column =
                midRelation.getColumn(rightKeyColumnName, false);
            if (column == null) {
                throw new IllegalArgumentException(
                    "right relation=" + rightRelation + ", right key="
                    + rightKeyColumnName); // REVIEW: user error?
            }
            final List<RolapSchema.PhysColumn> keyColumnList =
                Collections.singletonList(column);
            final RolapSchema.PhysKey midKey =
                midRelation.lookupKey(keyColumnList, true);
            // TODO: also create link from leftRelation to midRelation???
        }
        return rightRelation;
    }

    /**
     * Converts an XML level to new format.
     *
     * @param ordinal Level ordinal
     * @param xmlLegacyLevel XML level in original format
     * @param relations Map of relations by alias
     * @param relation Relation
     * @param links Required links from fact table to dimension
     * @return Converted level
     */
    private MondrianDef.Level convertLevel(
        int ordinal,
        Mondrian3Def.Level xmlLegacyLevel,
        Map<String, RolapSchema.PhysRelation> relations,
        RolapSchema.PhysRelation relation,
        List<Link> links)
    {
        final MondrianDef.Level xmlLevel = new MondrianDef.Level();
        xmlLevel.approxRowCount = xmlLegacyLevel.approxRowCount;
        xmlLevel.caption = xmlLegacyLevel.caption;
        if (xmlLegacyLevel.table != null) {
            relation = getPhysRelation(xmlLegacyLevel.table, true);
        }

        final String levelUniqueName = null;
        xmlLevel.name = xmlLegacyLevel.name;
        xmlLevel.captionColumn = xmlLegacyLevel.captionColumn; // ??
        xmlLevel.table = relation == null ? null : relation.getAlias();
        xmlLevel.closure = xmlLegacyLevel.closure;
        xmlLevel.column = xmlLegacyLevel.column;
        xmlLevel.formatter = xmlLegacyLevel.formatter;
        xmlLevel.hideMemberIf = xmlLegacyLevel.hideMemberIf;
        xmlLevel.keyExp = xmlLegacyLevel.keyExp;
        xmlLevel.levelType = xmlLegacyLevel.levelType;
        xmlLevel.nullParentValue = xmlLegacyLevel.nullParentValue;

        // key
        if (xmlLegacyLevel.column != null) {
            assert xmlLegacyLevel.keyExp == null;
            xmlLevel.keyExp = new MondrianDef.KeyExpression();
            xmlLevel.keyExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelColumn(
                        relation,
                        xmlLegacyLevel.column,
                        relations,
                        null)
                };
        } else if (xmlLegacyLevel.keyExp != null) {
            xmlLevel.keyExp = new MondrianDef.KeyExpression();
            xmlLevel.keyExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelExpression(
                        relation,
                        xmlLegacyLevel.keyExp,
                        relations,
                        levelUniqueName)
                };
        } else {
            xmlLevel.keyExp = null;
        }
        xmlLevel.column = null;

        // name
        if (xmlLegacyLevel.nameColumn != null) {
            assert xmlLegacyLevel.nameExp == null;
            xmlLevel.nameExp = new MondrianDef.NameExpression();
            xmlLevel.nameExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelColumn(
                        relation,
                        xmlLegacyLevel.nameColumn,
                        relations,
                        null)
                };
        } else if (xmlLegacyLevel.nameExp != null) {
            xmlLevel.nameExp = new MondrianDef.NameExpression();
            xmlLevel.nameExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelExpression(
                        relation,
                        xmlLegacyLevel.nameExp,
                        relations,
                        levelUniqueName)
                };
        } else {
            xmlLevel.nameExp = null;
        }
        xmlLevel.nameColumn = null;

        // ordinal
        if (xmlLegacyLevel.ordinalColumn != null) {
            assert xmlLegacyLevel.ordinalExp == null;
            xmlLevel.ordinalExp = new MondrianDef.OrdinalExpression();
            xmlLevel.ordinalExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelColumn(
                        relation,
                        xmlLegacyLevel.ordinalColumn,
                        relations, xmlLegacyLevel.name)
                };
        } else if (xmlLegacyLevel.ordinalExp != null) {
            xmlLevel.ordinalExp = new MondrianDef.OrdinalExpression();
            xmlLevel.ordinalExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelExpression(
                        relation,
                        xmlLegacyLevel.ordinalExp,
                        relations,
                        levelUniqueName)
                };
        } else {
            xmlLevel.ordinalExp = null;
        }
        xmlLevel.ordinalColumn = null;

        // parent
        if (xmlLegacyLevel.parentColumn != null) {
            assert xmlLegacyLevel.parentExp == null;
            xmlLevel.parentExp = new MondrianDef.ParentExpression();
            xmlLevel.parentExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelColumn(
                        relation,
                        xmlLegacyLevel.parentColumn,
                        relations,
                        null)
                };
        } else if (xmlLegacyLevel.parentExp != null) {
            xmlLevel.parentExp = new MondrianDef.ParentExpression();
            xmlLevel.parentExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelExpression(
                        relation,
                        xmlLevel.parentExp,
                        relations,
                        levelUniqueName)
                };
        } else {
            xmlLevel.parentExp = null;
        }
        xmlLevel.parentColumn = null;

        // caption
        if (xmlLegacyLevel.captionColumn != null) {
            assert xmlLegacyLevel.captionExp == null;
            xmlLevel.captionExp = new MondrianDef.CaptionExpression();
            xmlLevel.captionExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelColumn(
                        relation,
                        xmlLegacyLevel.captionColumn,
                        relations,
                        null)
                };
        } else if (xmlLegacyLevel.captionExp != null) {
            xmlLevel.captionExp = new MondrianDef.CaptionExpression();
            xmlLevel.captionExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelExpression(
                        relation,
                        xmlLevel.captionExp,
                        relations,
                        levelUniqueName)
                };
        } else {
            xmlLevel.captionExp = null;
        }
        xmlLevel.captionColumn = null;

        xmlLevel.properties =
            new MondrianDef.Property[xmlLegacyLevel.properties.length];
        for (int i = 0; i < xmlLegacyLevel.properties.length; i++) {
            MondrianDef.Property xmlLegacyProperty =
                xmlLegacyLevel.properties[i];
            final MondrianDef.Property xmlProperty =
                new MondrianDef.Property();
            xmlProperty.caption = xmlLegacyProperty.caption;
            xmlProperty.formatter = xmlLegacyProperty.formatter;
            xmlProperty.name = xmlLegacyProperty.name;
            // TODO: obsolete type in new schemas
            xmlProperty.type = xmlLegacyProperty.type;
            if (xmlLegacyProperty.column != null) {
                assert xmlLegacyProperty.exp == null;
                xmlProperty.exp = new MondrianDef.PropertyExpression();
                xmlProperty.exp.expressions =
                    new MondrianDef.SQL[] {
                    registerLevelColumn(
                        relation,
                        xmlLegacyProperty.column,
                        relations,
                        levelUniqueName)
                    };
            } else if (xmlLegacyProperty.exp != null) {
                xmlProperty.exp = new MondrianDef.PropertyExpression();
                xmlProperty.exp.expressions =
                    new MondrianDef.SQL[] {
                        registerLevelExpression(
                            relation,
                            xmlLegacyProperty.exp,
                            relations,
                            levelUniqueName)
                    };
            } else {
                xmlProperty.exp = null;
            }
            xmlProperty.column = null;
            xmlLevel.properties[i] = xmlProperty;
        }

        xmlLevel.type = xmlLegacyLevel.type;
        xmlLevel.uniqueMembers =
            xmlLegacyLevel.uniqueMembers == null
                ? (ordinal == 0)
                : xmlLegacyLevel.uniqueMembers;

        // Register closure table in physical schema, and link to fact
        // table.
        if (xmlLegacyLevel.closure != null) {
            final RolapSchema.PhysRelation physClosureTable =
                physSchemaBuilder.toPhysRelation(
                    this, xmlLegacyLevel.closure.table);

            // Create a key for the closure table. This is a slight fib,
            // since this does columns not uniquely identify rows in the
            // table. But it is consistent with how we use keys in dimension
            // tables: the key is what we join to from the fact table.
            RolapSchema.PhysKey key =
                physClosureTable.addKey(
                    xxx, Collections.singletonList(
                        physClosureTable.getColumn(
                            xmlLegacyLevel.closure.childColumn, true)));
            for (Link link : links) {
                physSchema.addLink(
                    key,
                    link.fact,
                    Collections.singletonList(
                        link.fact.getColumn(link.foreignKey, true)),
                    false);
            }
        }

        legacyMap.put(xmlLevel, xmlLegacyLevel);
        return xmlLevel;
    }

    private MondrianDef.SQL registerLevelExpression(
        RolapSchema.PhysRelation relation,
        MondrianDef.ExpressionView legacyExpression,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName)
    {
        final MondrianDef.SQL legacySql =
            MondrianDef.SQL.choose(
                legacyExpression.expressions,
                relation.getSchema().dialect);
        List<NodeDef> list = new ArrayList<NodeDef>();
        for (NodeDef legacyChild : legacySql.children) {
            if (legacyChild instanceof TextDef) {
                TextDef text = (TextDef) legacyChild;
                list.add(text);
            } else if (legacyChild instanceof MondrianDef.Column) {
                MondrianDef.Column legacyColumn =
                    (MondrianDef.Column) legacyChild;
                if (legacyColumn.table != null) {
                    Util.assertTrue(
                        relations.containsKey(legacyColumn.table));
                    relation = relations.get(legacyColumn.table);
                }
                final MondrianDef.SQL sql =
                    registerLevelColumn(
                        relation,
                        legacyColumn.name,
                        relations,
                        levelUniqueName);
                list.addAll(Arrays.asList(sql.children));
            } else {
                throw Util.newInternal(
                    "unexpected element in expression: "
                    + legacyChild.getName());
            }
        }
        final MondrianDef.SQL sql = new MondrianDef.SQL();
        sql.children = list.toArray(new NodeDef[list.size()]);
        sql.dialect = SqlQuery.getBestName(this.physSchema.dialect);

        // Validate that the expression belongs to a unique relation.
        RolapSchema.PhysExpr measureExp = toPhysExpr(relation, sql);
        final Set<RolapSchema.PhysRelation> relationSet =
            new HashSet<RolapSchema.PhysRelation>();
        RolapSchemaLoader.PhysSchemaBuilder.collectRelations(
            measureExp, relation, relationSet);
        if (relationSet.size() != 1) {
            getHandler().error(
                "Expression must belong to one and only one relation",
                legacyExpression,
                null);
        }
        return sql;
    }

    private MondrianDef.SQL registerLevelColumn(
        RolapSchema.PhysRelation relation,
        final String columnName,
        Map<String, RolapSchema.PhysRelation> relations,
        String levelUniqueName)
    {
        assert relation != null;
        final MondrianDef.Column column = new MondrianDef.Column();
        column.name = columnName;
        column.table = relation.getAlias();
        final MondrianDef.SQL sql = new MondrianDef.SQL();
        sql.children = new ElementDef[] {column};
        sql.dialect = SqlQuery.getBestName(this.physSchema.dialect);
        return sql;
    }

    public MondrianDef.Measure convertMeasure(
        RolapSchema.PhysRelation fact,
        MondrianDef.Measure xmlLegacyMeasure)
    {
        MondrianDef.Measure xmlMeasure = new MondrianDef.Measure();
        xmlMeasure.name = xmlLegacyMeasure.name;
        xmlMeasure.visible = xmlLegacyMeasure.visible;
        xmlMeasure.aggregator = xmlLegacyMeasure.aggregator;
        xmlMeasure.caption = xmlLegacyMeasure.caption;
        xmlMeasure.datatype = xmlLegacyMeasure.datatype;
        xmlMeasure.formatString = xmlLegacyMeasure.formatString;
        xmlMeasure.formatter = xmlLegacyMeasure.formatter;
        // REVIEW: what to do with member properties?
        xmlMeasure.memberProperties =
            clone(xmlLegacyMeasure.memberProperties);

        if (xmlLegacyMeasure.column != null) {
            if (xmlLegacyMeasure.measureExp != null) {
                throw MondrianResource.instance().BadMeasureSource.ex(
                    cube.getName(),
                    xmlLegacyMeasure.name);
            }
            xmlMeasure.measureExp = new MondrianDef.MeasureExpression();
            xmlMeasure.measureExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelColumn(
                        fact,
                        xmlLegacyMeasure.column,
                        null,
                        null)
                };
        } else if (xmlLegacyMeasure.measureExp != null) {
            xmlMeasure.measureExp = new MondrianDef.MeasureExpression();
            xmlMeasure.measureExp.expressions =
                new MondrianDef.SQL[] {
                    registerLevelExpression(
                        fact,
                        xmlLegacyMeasure.measureExp,
                        physSchema.tablesByName,
                        cube.measuresHierarchy.getLevelList().get(0)
                            .getUniqueName())
                };
        } else {
            xmlMeasure.measureExp = null;
        }
        xmlMeasure.column = null;
        return xmlMeasure;
    }

    private static <T> T[] clone(T[] a) {
        return a == null ? null : a.clone();
    }

    /**
     * Link to be made between the bottom level in a hierarchy and a fact
     * table containing one or more measures.
     */
    // TODO: better name
    static class Link {
        final RolapSchema.PhysRelation fact;
        final String foreignKey;

        /**
         * Creates a Link.
         *
         * <p>Fact table must not be null. Foreign key may be null if the
         * dimension lives in the fact table (i.e. is degenerate)
         *
         * @param fact Fact table
         * @param foreignKey Foreign key linking to fact table
         */
        Link(
            RolapSchema.PhysRelation fact,
            String foreignKey)
        {
            this.fact = fact;
            this.foreignKey = foreignKey;
        }
    }
}

// End PhysSchemaConverter.java
