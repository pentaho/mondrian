/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.rolap.RolapSchema.PhysColumn;
import mondrian.rolap.RolapSchema.PhysKey;
import mondrian.rolap.sql.SqlQuery;

import org.eigenbase.xom.*;

import java.util.*;

/**
 * Agent that converts an old-style schema to a new-style schema.
 * It builds a physical schema as it goes.
 *
 * @author jhyde
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
        MondrianDef.Query xmlQuery = new MondrianDef.Query();
        xmlQuery.alias = physView.getAlias();
        for (PhysKey physKey : physView.getKeyList()) {
            MondrianDef.Key key = new MondrianDef.Key();
            key.name = physKey.name;
            List<MondrianDef.Column> list = new ArrayList<MondrianDef.Column>();
            for (PhysColumn physColumn : physKey.columnList) {
                list.add(new MondrianDef.Column(null, physColumn.name));
            }
            key.array = list.toArray(new MondrianDef.Column[list.size()]);
            xmlQuery.children.add(key);
        }
        MondrianDef.ExpressionView expressionView =
            new MondrianDef.ExpressionView();
        MondrianDef.SQL sql = new MondrianDef.SQL();
        sql.children = new NodeDef[]{new TextDef(physView.getSqlString())};
        sql.dialect = SqlQuery.getBestName(physView.physSchema.dialect);

        // note that this is a lossy conversion, we lose all the dialects other
        // than the active dialect.
        expressionView.expressions = new MondrianDef.SQL[]{sql};
        xmlQuery.children.add(expressionView);
        xmlTables.put(physView.getAlias(), xmlQuery);
        return xmlQuery;
    }

    private MondrianDef.Table toDef(
        RolapSchema.PhysTable physTable)
    {
        MondrianDef.Table xmlTable = new MondrianDef.Table();
        xmlTable.name = physTable.name;
        xmlTable.alias = physTable.alias;
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
            assert xmlCalcColumnDef.expression != null;
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

        for (PhysKey pKey : physInlineTable.getKeyList()) {
            final MondrianDef.Key key = new MondrianDef.Key();
            List<MondrianDef.Column> columns =
                new ArrayList<MondrianDef.Column>();
            for (PhysColumn pColumn : pKey.columnList) {
                MondrianDef.Column column = new MondrianDef.Column();
                column.name = pColumn.name;
                column.table = pColumn.relation.getAlias();
                columns.add(column);
            }
            key.array =
                columns.toArray(
                    new MondrianDef.Column[columns.size()]);
            key.name = pKey.name;
            xmlInlineTable.children.add(key);
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

    /**
     * Converts an expression to a calculated column.
     *
     * <p>If the expression belongs to more than one relation,
     * or if it belongs to a relation that is not a table,
     * posts an error and returns null.</p>
     *
     * <p>Returns an existing calculated column if possible.</p>
     *
     * @param expr Expression
     * @param legacyExpression XML expression that column is being calculated
     *                         from (just for context, if there is an error)
     * @param xmlExpressionview XML expression in mondrian-4 format
     * @param relation Relation that is context of expression
     *
     * @return Calculated column, or null
     */
    public RolapSchema.PhysCalcColumn toPhysColumn(
        RolapSchema.PhysExpr expr,
        NodeDef legacyExpression,
        MondrianDef.ExpressionView xmlExpressionview,
        RolapSchema.PhysRelation relation)
    {
        final Set<RolapSchema.PhysRelation> relationSet =
            new HashSet<RolapSchema.PhysRelation>();
        RolapSchemaLoader.PhysSchemaBuilder.collectRelations(
            expr, relation, relationSet);
        if (relationSet.size() != 1) {
            getHandler().error(
                "Expression must belong to one and only one relation",
                legacyExpression,
                null);
            return null;
        }
        RolapSchema.PhysRelation relation1 = relationSet.iterator().next();
        if (!(relation1 instanceof RolapSchema.PhysTable)) {
            // For now, there is a limitation that expressions can only be based
            // on a table.
            //
            // We might be able to fix this by automatically creating a view,
            // or by allowing other relation types (inline tables, views) to
            // have calculated columns.
            getHandler().error(
                "Cannot define an expression in a relation that it is not a "
                + "table; relation '" + relation1.getAlias() + "' is a "
                + relation1.getClass(),
                legacyExpression,
                null);
            return null;
        }
        final RolapSchema.PhysTable physTable =
            (RolapSchema.PhysTable) relation1;
        for (Map.Entry<String, RolapSchema.PhysColumn> entry
            : physTable.columnsByName.entrySet())
        {
            if (entry.getValue() instanceof RolapSchema.PhysCalcColumn) {
                RolapSchema.PhysCalcColumn physCalcColumn =
                    (RolapSchema.PhysCalcColumn) entry.getValue();
                if (physCalcColumn.getList().equals(
                        Collections.singletonList(expr)))
                {
                    return physCalcColumn;
                }
            }
        }
        RolapSchema.PhysCalcColumn physCalcColumn =
            new RolapSchema.PhysCalcColumn(
                loader,
                legacyExpression,
                physTable,
                "calc$" + physTable.columnsByName.size(),
                expr.getDatatype(),
                expr.getInternalType(),
                Collections.singletonList(expr));
        physTable.addColumn(physCalcColumn);
        calcColumnExprMap.put(
            physCalcColumn,
            xmlExpressionview);
        return physCalcColumn;
    }
}

// End PhysSchemaConverter.java
