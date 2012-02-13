/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2012 Julian Hyde
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

import org.eigenbase.xom.*;

import java.util.*;

/**
 * Agent that converts an old-style schema to a new-style schema.
 * It builds a physical schema as it goes.
 *
 * @version $Id$
 */
class PhysSchemaConverter extends RolapSchemaLoader.PhysSchemaBuilder {
    /**
     * Maps converted (new-style) XML elements to the legacy element they
     * were created from. Allows us to tag error messages with the location
     * in the document of the legacy element (new elements did not appear
     * in the document).
     */
    final Map<ElementDef, ElementDef> legacyMap =
        new HashMap<ElementDef, ElementDef>();

    Map<String, MondrianDef.Relation> xmlTables =
        new HashMap<String, MondrianDef.Relation>();

    private final Map<RolapSchema.PhysCalcColumn, MondrianDef.Expression>
        calcColumnExprMap =
        new IdentityHashMap<
            RolapSchema.PhysCalcColumn, MondrianDef.Expression>();

    /**
     * Creates a PhysSchemaConverter.
     *
     * @param loader Schema loader
     * @param physSchema Physical schema
     */
    PhysSchemaConverter(
        RolapSchemaLoader loader,
        RolapSchema.PhysSchema physSchema)
    {
        super(loader, physSchema);
    }

    @Override
    RolapSchema.PhysExpr toPhysExpr(
        RolapSchema.PhysRelation physRelation,
        MondrianDef.Expression exp)
    {
        final RolapSchema.PhysExpr physExpr =
            super.toPhysExpr(
                physRelation,
                exp);
        if (physExpr instanceof RolapSchema.PhysCalcColumn) {
            final RolapSchema.PhysCalcColumn physCalcColumn =
                (RolapSchema.PhysCalcColumn) physExpr;
            calcColumnExprMap.put(
                physCalcColumn,
                exp);
            assert physCalcColumn.relation.getColumn(physCalcColumn.name, false)
                   != null
                : "column isn't registered in its own table: " + physCalcColumn;
        }
        return physExpr;
    }

    public MondrianDef.PhysicalSchema toDef(RolapSchema.PhysSchema physSchema) {
        MondrianDef.PhysicalSchema xmlPhysSchema =
            new MondrianDef.PhysicalSchema();
        for (RolapSchema.PhysRelation relation
            : physSchema.tablesByName.values())
        {
            toDef(relation, xmlPhysSchema.children);
        }
        for (RolapSchema.PhysLink physLink : physSchema.linkSet) {
            toDef(physLink, xmlPhysSchema.children);
        }
        return xmlPhysSchema;
    }

    private void toDef(
        RolapSchema.PhysLink physLink,
        MondrianDef.Children<MondrianDef.PhysicalSchemaElement>
            physSchemaChildren)
    {
        final MondrianDef.Link xmlLink = new MondrianDef.Link();
        xmlLink.target = physLink.targetRelation.getAlias();
        xmlLink.source = physLink.sourceKey.relation.getAlias();
        xmlLink.key = physLink.sourceKey.name;
        xmlLink.foreignKey = new MondrianDef.ForeignKey();
        final List<MondrianDef.Column> xmlColumnList =
            new ArrayList<MondrianDef.Column>();
        toDef(physLink.columnList, xmlColumnList);
        xmlLink.foreignKey.array =
            xmlColumnList.toArray(new MondrianDef.Column[xmlColumnList.size()]);
        physSchemaChildren.add(xmlLink);
    }

    private void toDef(
        RolapSchema.PhysRelation physRelation,
        MondrianDef.Children<MondrianDef.PhysicalSchemaElement>
            physSchemaChildren)
    {
        MondrianDef.Relation xmlRelation;
        if (physRelation instanceof RolapSchema.PhysInlineTable) {
            xmlRelation = toDef((RolapSchema.PhysInlineTable) physRelation);
        } else if (physRelation instanceof RolapSchema.PhysTable) {
            xmlRelation = toDef((RolapSchema.PhysTable) physRelation);
        } else if (physRelation instanceof RolapSchema.PhysView) {
            xmlRelation = toDef((RolapSchema.PhysView) physRelation);
        } else {
            throw Util.newInternal("unknown relation type " + physRelation);
        }
        physSchemaChildren.add(xmlRelation);
    }

    private MondrianDef.Relation toDef(
        RolapSchema.PhysView physView)
    {
        Util.deprecated("TODO implement", true);
        MondrianDef.Query xmlQuery = new MondrianDef.Query();
        xmlTables.put(physView.getAlias(), xmlQuery);
        return xmlQuery;
    }

    private MondrianDef.Table toDef(
        RolapSchema.PhysTable physTable)
    {
        MondrianDef.Table xmlTable = new MondrianDef.Table();
        xmlTable.name = physTable.name;
        List<MondrianDef.RealOrCalcColumnDef> columnDefs =
            xmlTable.children.holder(
                new MondrianDef.ColumnDefs()).list();
        for (RolapSchema.PhysColumn physColumn
            : physTable.columnsByName.values())
        {
            toDef(physColumn, columnDefs);
        }
        for (RolapSchema.PhysKey physKey : physTable.keysByName.values()) {
            toDef(physKey, xmlTable);
        }
        xmlTables.put(physTable.getAlias(), xmlTable);
        return xmlTable;
    }

    private void toDef(
        RolapSchema.PhysKey physKey,
        MondrianDef.Table xmlTable)
    {
        MondrianDef.Key xmlKey = new MondrianDef.Key();
        xmlKey.name = physKey.name;
        final List<MondrianDef.Column> xmlColumnList =
            new ArrayList<MondrianDef.Column>();
        toDef(physKey.columnList, xmlColumnList);
        xmlKey.array =
            xmlColumnList.toArray(new MondrianDef.Column[xmlColumnList.size()]);
        xmlTable.children.add(xmlKey);
    }

    private void toDef(
        List<RolapSchema.PhysColumn> physColumnList,
        List<MondrianDef.Column> xmlColumnList)
    {
        for (RolapSchema.PhysColumn physColumn : physColumnList) {
            xmlColumnList.add(toRefDef(physColumn));
        }
    }

    private MondrianDef.Column toRefDef(
        RolapSchema.PhysColumn physColumn)
    {
        MondrianDef.Column xmlColumn = new MondrianDef.Column();
        xmlColumn.name = physColumn.name;
        xmlColumn.table = physColumn.relation.getAlias();
        return xmlColumn;
    }

    private void toDef(
        RolapSchema.PhysColumn physColumn,
        List<MondrianDef.RealOrCalcColumnDef> columnDefs)
    {
        if (physColumn instanceof RolapSchema.PhysCalcColumn) {
            RolapSchema.PhysCalcColumn column =
                (RolapSchema.PhysCalcColumn) physColumn;
            MondrianDef.CalculatedColumnDef xmlCalcColumnDef =
                new MondrianDef.CalculatedColumnDef();
            xmlCalcColumnDef.name = column.name;
            xmlCalcColumnDef.expression = calcColumnExprMap.get(column);
            columnDefs.add(xmlCalcColumnDef);
        } else {
            // ignore regular column
        }
    }

    private MondrianDef.InlineTable toDef(
        RolapSchema.PhysInlineTable physInlineTable)
    {
        MondrianDef.InlineTable xmlInlineTable =
            new MondrianDef.InlineTable();
        xmlInlineTable.alias = physInlineTable.getAlias();
        final List<MondrianDef.RealOrCalcColumnDef> xmlColumnDefs =
            xmlInlineTable.children.holder(new MondrianDef.ColumnDefs()).list();
        for (RolapSchema.PhysColumn elementDef
            : physInlineTable.columnsByName.values())
        {
            MondrianDef.ColumnDef xmlColumn = new MondrianDef.ColumnDef();
            xmlColumn.name = elementDef.name;
            xmlColumn.type = elementDef.datatype.name();
            xmlColumnDefs.add(xmlColumn);
        }
        final List<MondrianDef.Row> xmlRows =
            xmlInlineTable.children.holder(new MondrianDef.Rows()).list();
        for (String[] values : physInlineTable.rowList) {
            MondrianDef.Row xmlRow = new MondrianDef.Row();
            final List<MondrianDef.Value> valueList =
                new ArrayList<MondrianDef.Value>();
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                MondrianDef.Value xmlValue = new MondrianDef.Value();
                xmlValue.column = xmlColumnDefs.get(i).name;
                xmlValue.cdata = value;
                valueList.add(xmlValue);
            }
            xmlRow.values =
                valueList.toArray(new MondrianDef.Value[valueList.size()]);
            xmlRows.add(xmlRow);
        }
        xmlTables.put(physInlineTable.getAlias(), xmlInlineTable);
        return xmlInlineTable;
    }
}

// End PhysSchemaConverter.java
