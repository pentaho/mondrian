/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap;

import mondrian.olap.*;

/**
 * A <code>HierarchyUsage</code> is the usage of a hierarchy in the context
 * of a cube. Private hierarchies can only be used in their own
 * cube. Public hierarchies can be used in several cubes. The problem comes
 * when several cubes which the same public hierarchy are brought together
 * in one virtual cube. There are now several usages of the same public
 * hierarchy. Which one to use? It depends upon what measure we are
 * currently using. We should use the hierarchy usage for the fact table
 * which underlies the measure. That is what determines the foreign key to
 * join on.
 *
 * A <code>HierarchyUsage</code> is identified by
 * <code>(hierarchy.sharedHierarchy, factTable)</code> if the hierarchy is
 * shared, or <code>(hierarchy, factTable)</code> if it is private.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
abstract class HierarchyUsage
{
    /**
     * Fact table (or relation) which this usage is joining to. This
     * identifies the usage, and determines which join conditions need to be
     * used.
     */
    protected MondrianDef.Relation fact;
    /**
     * The foreign key by which this {@link Hierarchy} is joined to
     * the {@link #fact} table.
     */
    final String foreignKey;
    /**
     * Dimension table which contains the primary key for the hierarchy.
     * (Usually the table of the lowest level of the hierarchy.)
     */
    MondrianDef.Relation joinTable;
    /**
     * The expression (usually a {@link MondrianDef.Column}) by which the
     * hierarchy which is joined to the fact table.
     */
    MondrianDef.Expression joinExp;

    HierarchyUsage(MondrianDef.Relation fact, String foreignKey) {
        this.fact = fact;
        this.foreignKey = foreignKey;
    }

    void init(RolapCube cube, RolapHierarchy hierarchy,
             MondrianDef.DimensionUsage dimensionUsage) {
        // Three ways that a hierarchy can be joined to the fact table.
        if (dimensionUsage != null && dimensionUsage.level != null) {
            // 1. Specify an explicit 'level' attribute in a <DimensionUsage>.
            RolapLevel joinLevel = (RolapLevel)
                    Util.lookupHierarchyLevel(hierarchy, dimensionUsage.level);
            if (joinLevel == null) {
                throw MondrianResource.instance()
                        .newDimensionUsageHasUnknownLevel(
                                hierarchy.getUniqueName(),
                                cube.getUniqueName(),
                                dimensionUsage.level);
            }
            joinTable = findJoinTable(hierarchy, joinLevel.keyExp.getTableAlias());
            joinExp = joinLevel.keyExp;
        } else if (hierarchy.xmlHierarchy != null &&
                hierarchy.xmlHierarchy.primaryKey != null) {
            // 2. Specify a "primaryKey" attribute of in <Hierarchy>. You must
            //    also specify the "primaryKeyTable" attribute if the hierarchy
            //    is a join (hence has more than one table).
            joinTable = findJoinTable(hierarchy,
                hierarchy.xmlHierarchy.primaryKeyTable);
            joinExp = new MondrianDef.Column(joinTable.getAlias(),
                    hierarchy.xmlHierarchy.primaryKey);
        } else {
            // 3. If neither of the above, the join is assumed to be to key of
            //    the last level.
            final Level[] levels = hierarchy.getLevels();
            RolapLevel joinLevel = (RolapLevel) levels[levels.length - 1];
            joinTable = findJoinTable(hierarchy,
                joinLevel.keyExp.getTableAlias());
            joinExp = joinLevel.keyExp;
        }
        Util.assertTrue(Util.equals(foreignKey, hierarchy.foreignKey));

        // Unless this hierarchy is drawing from the fact table, we need
        // a join expresion and a foreign key.
        final boolean inFactTable = joinTable.equals(cube.getFact());
        if (!inFactTable) {
            if (joinExp == null) {
                throw MondrianResource.instance()
                        .newMustSpecifyPrimaryKeyForHierarchy(
                                hierarchy.getUniqueName(),
                                cube.getUniqueName());
            }
            if (foreignKey == null) {
                throw MondrianResource.instance()
                        .newMustSpecifyForeignKeyForHierarchy(
                                hierarchy.getUniqueName(),
                                cube.getUniqueName());
            }
        }
    }

    /**
     * Chooses the table with which to join a hierarchy to the fact table.
     *
     * @param hierarchy Hierarchy to be joined
     * @param tableName Alias of the table; may be omitted if the hierarchy
     *   has only one table
     * @return A table, never null
     */
    private MondrianDef.Relation findJoinTable(
        RolapHierarchy hierarchy,
        String tableName)
    {
        final MondrianDef.Relation table;
        if (tableName == null) {
            table = hierarchy.getUniqueTable();
            if (table == null) {
                throw MondrianResource.instance()
                    .newMustSpecifyPrimaryKeyTableForHierarchy(
                        hierarchy.getUniqueName());
            }
        } else {
            table = hierarchy.getRelation().find(tableName);
            if (table == null) {
                // todo: i18n msg
                throw Util.newError(
                    "no table '" + tableName +
                    "' found in hierarchy " + hierarchy.getUniqueName());
            }
        }
        return table;
    }

}

// End HierarchyUsage.java
