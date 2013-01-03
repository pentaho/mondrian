/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.olap.*;
import mondrian.olap.Property;
import mondrian.olap.fun.FunInfo;
import mondrian.rolap.*;
import mondrian.xmla.RowsetDefinition;
import mondrian.xmla.XmlaHandler;

import org.olap4j.*;
import org.olap4j.Cell;
import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Level;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.Schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Provides access to internals of mondrian's olap4j driver that are not part
 * of the olap4j API.
 *
 * @author jhyde
 * @since Nov 12, 2010
 */
class MondrianOlap4jExtra implements XmlaHandler.XmlaExtra {
    static final MondrianOlap4jExtra INSTANCE = new MondrianOlap4jExtra();

    public ResultSet executeDrillthrough(
        OlapStatement olapStatement,
        String mdx,
        boolean advanced,
        String tabFields,
        int[] rowCountSlot) throws SQLException
    {
        return ((MondrianOlap4jStatement) olapStatement).executeQuery2(
            mdx,
            advanced,
            tabFields,
            rowCountSlot);
    }

    public void setPreferList(OlapConnection connection) {
        ((MondrianOlap4jConnection) connection).setPreferList(true);
    }

    public Date getSchemaLoadDate(Schema schema) {
        return ((MondrianOlap4jSchema) schema).schema.getSchemaLoadDate();
    }

    public int getLevelCardinality(Level level) throws OlapException {
        if (level instanceof MondrianOlap4jLevel) {
            // Improved implementation if the provider is mondrian.
            final MondrianOlap4jLevel olap4jLevel = (MondrianOlap4jLevel) level;
            final mondrian.olap.SchemaReader schemaReader =
                olap4jLevel.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
                    .olap4jConnection.getMondrianConnection().getSchemaReader()
                    .withLocus();
            return schemaReader.getLevelCardinality(
                olap4jLevel.level, true, true);
        } else {
            return level.getCardinality();
        }
    }

    public void getSchemaFunctionList(
        List<FunctionDefinition> funDefs,
        Schema schema,
        Util.Functor1<Boolean, String> functionFilter)
    {
        final FunTable funTable =
            ((MondrianOlap4jSchema) schema).schema.getFunTable();
        StringBuilder buf = new StringBuilder(50);
        for (FunInfo fi : funTable.getFunInfoList()) {
            switch (fi.getSyntax()) {
            case Empty:
            case Internal:
            case Parentheses:
                continue;
            }
            final Boolean passes = functionFilter.apply(fi.getName());
            if (passes == null || !passes) {
                continue;
            }

            int[][] paramCategories = fi.getParameterCategories();
            int[] returnCategories = fi.getReturnCategories();

            // Convert Windows newlines in 'description' to UNIX format.
            String description = fi.getDescription();
            if (description != null) {
                description = Util.replace(
                    fi.getDescription(),
                    "\r",
                    "");
            }
            if ((paramCategories == null)
                || (paramCategories.length == 0))
            {
                funDefs.add(
                    new FunctionDefinition(
                        fi.getName(),
                        description,
                        "(none)",
                        1,
                        1,
                        // TODO WHAT VALUE should this have
                        "",
                        fi.getName()));
            } else {
                for (int i = 0; i < paramCategories.length; i++) {
                    int[] pc = paramCategories[i];
                    int returnCategory = returnCategories[i];

                    buf.setLength(0);
                    for (int j = 0; j < pc.length; j++) {
                        int v = pc[j];
                        if (j > 0) {
                            buf.append(", ");
                        }
                        buf.append(
                            Category.instance.getDescription(
                                v & Category.Mask));
                    }

                    RowsetDefinition.MdschemaFunctionsRowset.VarType varType =
                        RowsetDefinition.MdschemaFunctionsRowset.VarType
                            .forCategory(returnCategory);
                    funDefs.add(
                        new FunctionDefinition(
                            fi.getName(),
                            description,
                            buf.toString(),
                            //TODO: currently FunInfo can not tell us which
                            // functions are MDX and which are UDFs.
                            varType.ordinal(),
                            1,
                            // TODO: Name of the interface for UDF and Group
                            // name for the MDX functions.
                            // TODO WHAT VALUE should this have
                            "",
                            fi.getName()));
                }
            }
        }
    }

    public int getHierarchyCardinality(Hierarchy hierarchy)
        throws OlapException
    {
        final MondrianOlap4jHierarchy olap4jHierarchy =
            (MondrianOlap4jHierarchy) hierarchy;
        final mondrian.olap.SchemaReader schemaReader =
            olap4jHierarchy.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
                .olap4jConnection.getMondrianConnection().getSchemaReader()
                .withLocus();
        return RolapMemberBase.getHierarchyCardinality(
            schemaReader, olap4jHierarchy.hierarchy);
    }

    public int getHierarchyStructure(Hierarchy hierarchy) {
        final MondrianOlap4jHierarchy olap4jHierarchy =
            (MondrianOlap4jHierarchy) hierarchy;
        return ((HierarchyBase) olap4jHierarchy.hierarchy).isRagged() ? 1 : 0;
    }

    public boolean isHierarchyParentChild(Hierarchy hierarchy) {
        Level nonAllFirstLevel = hierarchy.getLevels().get(0);
        if (nonAllFirstLevel.getLevelType() == Level.Type.ALL) {
            nonAllFirstLevel = hierarchy.getLevels().get(1);
        }
        MondrianOlap4jLevel olap4jLevel =
            (MondrianOlap4jLevel) nonAllFirstLevel;
        return ((RolapLevel) olap4jLevel.level).isParentChild();
    }

    public int getMeasureAggregator(Member member) {
        MondrianOlap4jMeasure olap4jMeasure =
            (MondrianOlap4jMeasure) member;
        Object aggProp =
            olap4jMeasure.member.getPropertyValue(
                Property.AGGREGATION_TYPE.name);
        if (aggProp == null) {
            return
                RowsetDefinition.MdschemaMeasuresRowset
                    .MDMEASURE_AGGR_CALCULATED;
        }
        RolapAggregator agg = (RolapAggregator) aggProp;
        if (agg == RolapAggregator.Sum) {
            return RowsetDefinition.MdschemaMeasuresRowset.MDMEASURE_AGGR_SUM;
        } else if (agg == RolapAggregator.Count) {
            return RowsetDefinition.MdschemaMeasuresRowset.MDMEASURE_AGGR_COUNT;
        } else if (agg == RolapAggregator.Min) {
            return RowsetDefinition.MdschemaMeasuresRowset.MDMEASURE_AGGR_MIN;
        } else if (agg == RolapAggregator.Max) {
            return RowsetDefinition.MdschemaMeasuresRowset.MDMEASURE_AGGR_MAX;
        } else if (agg == RolapAggregator.Avg) {
            return RowsetDefinition.MdschemaMeasuresRowset.MDMEASURE_AGGR_AVG;
        } else {
            //TODO: what are VAR and STD
            return RowsetDefinition.MdschemaMeasuresRowset
                .MDMEASURE_AGGR_UNKNOWN;
        }
    }

    public void checkMemberOrdinal(Member member) throws OlapException {
        if (member.getOrdinal() == -1) {
            MondrianOlap4jMember olap4jMember =
                (MondrianOlap4jMember) member;
            final mondrian.olap.SchemaReader schemaReader =
                olap4jMember.olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
                    .olap4jConnection.getMondrianConnection().getSchemaReader()
                    .withLocus();
            RolapMemberBase.setOrdinals(schemaReader, olap4jMember.member);
        }
    }

    public boolean shouldReturnCellProperty(
        CellSet cellSet,
        org.olap4j.metadata.Property cellProperty,
        boolean evenEmpty)
    {
        MondrianOlap4jCellSet olap4jCellSet = (MondrianOlap4jCellSet) cellSet;
        Query query = olap4jCellSet.query;
        return
            (evenEmpty
             && query.isCellPropertyEmpty())
            || query.hasCellProperty(cellProperty.getName());
    }

    public List<String> getSchemaRoleNames(Schema schema) {
        MondrianOlap4jSchema olap4jSchema = (MondrianOlap4jSchema) schema;
        // TODO: this returns ALL roles, no the current user's roles
        return new ArrayList<String>(
            ((RolapSchema) olap4jSchema.schema).roleNames());
    }

    public String getSchemaId(Schema schema) {
        return ((MondrianOlap4jSchema)schema).schema.getId();
    }

    public String getCubeType(Cube cube) {
        return
            (cube instanceof MondrianOlap4jCube)
            && ((RolapCube) ((MondrianOlap4jCube) cube).cube).isVirtual()
                ? RowsetDefinition.MdschemaCubesRowset.MD_CUBTYPE_VIRTUAL_CUBE
                : RowsetDefinition.MdschemaCubesRowset.MD_CUBTYPE_CUBE;
    }

    public boolean isLevelUnique(Level level) {
        MondrianOlap4jLevel olap4jLevel = (MondrianOlap4jLevel) level;
        return (olap4jLevel.level instanceof RolapLevel)
            && ((RolapLevel) olap4jLevel.level).isUnique();
    }

    public List<org.olap4j.metadata.Property> getLevelProperties(Level level) {
        MondrianOlap4jLevel olap4jLevel = (MondrianOlap4jLevel) level;
        return olap4jLevel.getProperties(false);
    }

    public boolean isPropertyInternal(org.olap4j.metadata.Property property) {
        MondrianOlap4jProperty olap4jProperty =
            (MondrianOlap4jProperty) property;
        return olap4jProperty.property.isInternal();
    }

    public List<Map<String, Object>> getDataSources(OlapConnection connection)
        throws OlapException
    {
        MondrianOlap4jConnection olap4jConnection =
            (MondrianOlap4jConnection) connection;
        MondrianServer server =
            MondrianServer.forConnection(
                olap4jConnection.getMondrianConnection());
        return server.getDatabases(olap4jConnection.getMondrianConnection());
    }

    public Map<String, Object> getAnnotationMap(MetadataElement element)
        throws SQLException
    {
        if (element instanceof OlapWrapper) {
            OlapWrapper wrapper = (OlapWrapper) element;
            if (wrapper.isWrapperFor(Annotated.class)) {
                final Annotated annotated = wrapper.unwrap(Annotated.class);
                final Map<String, Object> map = new HashMap<String, Object>();
                for (Map.Entry<String, Annotation> entry
                    : annotated.getAnnotationMap().entrySet())
                {
                    map.put(entry.getKey(), entry.getValue().getValue());
                }
                return map;
            }
        }
        return Collections.emptyMap();
    }

    public boolean canDrillThrough(Cell cell) {
        return ((MondrianOlap4jCell)cell).cell.canDrillThrough();
    }

    public int getDrillThroughCount(Cell cell) {
        return ((MondrianOlap4jCell)cell).cell.getDrillThroughCount();
    }

    public void flushSchemaCache(OlapConnection conn) throws OlapException {
        try {
            conn.unwrap(RolapConnection.class)
                .getCacheControl(null).flushSchemaCache();
        } catch (SQLException e) {
            throw new OlapException(e);
        }
    }

    public Object getMemberKey(Member m) throws OlapException {
        try {
            return ((MondrianOlap4jMember)m)
                .unwrap(RolapMemberBase.class).getKey();
        } catch (SQLException e) {
            throw new OlapException(e);
        }
    }

    public Object getOrderKey(Member m) throws OlapException {
        try {
            return ((MondrianOlap4jMember)m)
                .unwrap(RolapMemberBase.class).getOrderKey();
        } catch (SQLException e) {
            throw new OlapException(e);
        }
    }
}

// End MondrianOlap4jExtra.java
