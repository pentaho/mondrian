/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
//
// jhyde, 21 March, 2002
*/

package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;

import org.apache.log4j.Logger;

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
 */
public class HierarchyUsage {
    private static final Logger LOGGER = Logger.getLogger(HierarchyUsage.class);

    enum Kind {
        UNKNOWN,
        SHARED,
        VIRTUAL,
        PRIVATE
    }

    /**
     * Fact table (or relation) which this usage is joining to. This
     * identifies the usage, and determines which join conditions need to be
     * used.
     */
    protected final MondrianDef.Relation fact;

    /**
     * This matches the hierarchy - may not be unique.
     * NOT NULL.
     */
    private final String hierarchyName;

    /**
     * not NULL for DimensionUsage
     * not NULL for Dimension
     */
    private final String name;

    /**
     * This is the name used to look up the hierachy usage. When the dimension
     * has only a single hierachy, then the fullName is simply the
     * CubeDimension name; there is no need to use the default dimension name.
     * But, when the dimension has more than one hierachy, then the fullName
     * is the CubeDimension dotted with the dimension hierachy name.
     *
     * <p>NOTE: jhyde, 2009/2/2: The only use of this field today is for
     * {@link RolapCube#getUsageByName}, which is used only for tracing.
     */
    private final String fullName;

    /**
     * The foreign key by which this {@link Hierarchy} is joined to
     * the {@link #fact} table.
     */
    private final String foreignKey;

    /**
     * not NULL for DimensionUsage
     * NULL for Dimension
     */
    private final String source;

    /**
     * May be null, this is the field that is used to disambiguate column
     * names in aggregate tables
     */
    private final String usagePrefix;

    // NOT USED
    private final String level;
    //final String type;
    //final String caption;

    /**
     * Dimension table which contains the primary key for the hierarchy.
     * (Usually the table of the lowest level of the hierarchy.)
     */
    private MondrianDef.Relation joinTable;

    /**
     * The expression (usually a {@link mondrian.olap.MondrianDef.Column}) by
     * which the hierarchy which is joined to the fact table.
     */
    private MondrianDef.Expression joinExp;

    private final Kind kind;

    /**
     * Creates a HierarchyUsage.
     *
     * @param cube Cube
     * @param hierarchy Hierarchy
     * @param cubeDim XML definition of a dimension which belongs to a cube
     */
    HierarchyUsage(
        RolapCube cube,
        RolapHierarchy hierarchy,
        MondrianDef.CubeDimension cubeDim)
    {
        assert cubeDim != null : "precondition: cubeDim != null";

        this.fact = cube.fact;

        // Attributes common to all Hierarchy kinds
        // name
        // foreignKey
        this.name = cubeDim.name;
        this.foreignKey = cubeDim.foreignKey;

        if (cubeDim instanceof MondrianDef.DimensionUsage) {
            this.kind = Kind.SHARED;


            // Shared Hierarchy attributes
            // source
            // level
            MondrianDef.DimensionUsage du =
                (MondrianDef.DimensionUsage) cubeDim;

            this.hierarchyName = deriveHierarchyName(hierarchy);
            int index = this.hierarchyName.indexOf('.');
            if (index == -1) {
                this.fullName = this.name;
                this.source = du.source;
            } else {
                String hname = this.hierarchyName.substring(
                    index + 1, this.hierarchyName.length());

                StringBuilder buf = new StringBuilder(32);
                buf.append(this.name);
                buf.append('.');
                buf.append(hname);
                this.fullName = buf.toString();

                buf.setLength(0);
                buf.append(du.source);
                buf.append('.');
                buf.append(hname);
                this.source = buf.toString();
            }

            this.level = du.level;
            this.usagePrefix = du.usagePrefix;

            init(cube, hierarchy, du);

        } else if (cubeDim instanceof MondrianDef.Dimension) {
            this.kind = Kind.PRIVATE;

            // Private Hierarchy attributes
            // type
            // caption
            MondrianDef.Dimension d = (MondrianDef.Dimension) cubeDim;

            this.hierarchyName = deriveHierarchyName(hierarchy);
            this.fullName = this.name;

            this.source = null;
            this.usagePrefix = d.usagePrefix;
            this.level = null;

            init(cube, hierarchy, null);

        } else if (cubeDim instanceof MondrianDef.VirtualCubeDimension) {
            this.kind = Kind.VIRTUAL;

            // Virtual Hierarchy attributes
            MondrianDef.VirtualCubeDimension vd =
                (MondrianDef.VirtualCubeDimension) cubeDim;

            this.hierarchyName = cubeDim.name;
            this.fullName = this.name;

            this.source = null;
            this.usagePrefix = null;
            this.level = null;

            init(cube, hierarchy, null);

        } else {
            getLogger().warn(
                "HierarchyUsage<init>: Unknown cubeDim="
                    + cubeDim.getClass().getName());

            this.kind = Kind.UNKNOWN;

            this.hierarchyName = cubeDim.name;
            this.fullName = this.name;

            this.source = null;
            this.usagePrefix = null;
            this.level = null;

            init(cube, hierarchy, null);
        }
        if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                toString()
                + ", cubeDim="
                + cubeDim.getClass().getName());
        }
    }

    private String deriveHierarchyName(RolapHierarchy hierarchy) {
        final String name = hierarchy.getName();
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            return name;
        } else {
            final String dimensionName = hierarchy.getDimension().getName();
            if (name == null
                || name.equals("")
                || name.equals(dimensionName))
            {
                return name;
            } else {
                return dimensionName + '.' + name;
            }
        }
    }

    protected Logger getLogger() {
        return LOGGER;
    }

    public String getHierarchyName() {
        return this.hierarchyName;
    }
    public String getFullName() {
        return this.fullName;
    }
    public String getName() {
        return this.name;
    }
    public String getForeignKey() {
        return this.foreignKey;
    }
    public String getSource() {
        return this.source;
    }
    public String getLevelName() {
        return this.level;
    }
    public String getUsagePrefix() {
        return this.usagePrefix;
    }

    public MondrianDef.Relation getJoinTable() {
        return this.joinTable;
    }

    public MondrianDef.Expression getJoinExp() {
        return this.joinExp;
    }

    public Kind getKind() {
        return this.kind;
    }
    public boolean isShared() {
        return this.kind == Kind.SHARED;
    }
    public boolean isVirtual() {
        return this.kind == Kind.VIRTUAL;
    }
    public boolean isPrivate() {
        return this.kind == Kind.PRIVATE;
    }

    public boolean equals(Object o) {
        if (o instanceof HierarchyUsage) {
            HierarchyUsage other = (HierarchyUsage) o;
            return (this.kind == other.kind)
                && Util.equals(this.fact, other.fact)
                && this.hierarchyName.equals(other.hierarchyName)
                && Util.equalName(this.name, other.name)
                && Util.equalName(this.source, other.source)
                && Util.equalName(this.foreignKey, other.foreignKey);
        } else {
            return false;
        }
    }

    public int hashCode() {
        int h = fact.hashCode();
        h = Util.hash(h, hierarchyName);
        h = Util.hash(h, name);
        h = Util.hash(h, source);
        h = Util.hash(h, foreignKey);
        return h;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(100);
        buf.append("HierarchyUsage: ");
        buf.append("kind=");
        buf.append(this.kind.name());
        buf.append(", hierarchyName=");
        buf.append(this.hierarchyName);
        buf.append(", fullName=");
        buf.append(this.fullName);
        buf.append(", foreignKey=");
        buf.append(this.foreignKey);
        buf.append(", source=");
        buf.append(this.source);
        buf.append(", level=");
        buf.append(this.level);
        buf.append(", name=");
        buf.append(this.name);

        return buf.toString();
    }

    void init(
        RolapCube cube,
        RolapHierarchy hierarchy,
        MondrianDef.DimensionUsage cubeDim)
    {
        // Three ways that a hierarchy can be joined to the fact table.
        if (cubeDim != null && cubeDim.level != null) {
            // 1. Specify an explicit 'level' attribute in a <DimensionUsage>.
            RolapLevel joinLevel = (RolapLevel)
                    Util.lookupHierarchyLevel(hierarchy, cubeDim.level);
            if (joinLevel == null) {
                throw MondrianResource.instance()
                    .DimensionUsageHasUnknownLevel.ex(
                        hierarchy.getUniqueName(),
                        cube.getName(),
                        cubeDim.level);
            }
            this.joinTable =
                findJoinTable(hierarchy, joinLevel.getKeyExp().getTableAlias());
            this.joinExp = joinLevel.getKeyExp();
        } else if (hierarchy.getXmlHierarchy() != null
            && hierarchy.getXmlHierarchy().primaryKey != null)
        {
            // 2. Specify a "primaryKey" attribute of in <Hierarchy>. You must
            //    also specify the "primaryKeyTable" attribute if the hierarchy
            //    is a join (hence has more than one table).
            this.joinTable =
                findJoinTable(
                    hierarchy,
                    hierarchy.getXmlHierarchy().primaryKeyTable);
            this.joinExp =
                new MondrianDef.Column(
                    this.joinTable.getAlias(),
                    hierarchy.getXmlHierarchy().primaryKey);
        } else {
            // 3. If neither of the above, the join is assumed to be to key of
            //    the last level.
            final Level[] levels = hierarchy.getLevels();
            RolapLevel joinLevel = (RolapLevel) levels[levels.length - 1];
            this.joinTable =
                findJoinTable(
                    hierarchy,
                    joinLevel.getKeyExp().getTableAlias());
            this.joinExp = joinLevel.getKeyExp();
        }

        // Unless this hierarchy is drawing from the fact table, we need
        // a join expresion and a foreign key.
        final boolean inFactTable = this.joinTable.equals(cube.getFact());
        if (!inFactTable) {
            if (this.joinExp == null) {
                throw MondrianResource.instance()
                    .MustSpecifyPrimaryKeyForHierarchy.ex(
                        hierarchy.getUniqueName(),
                        cube.getName());
            }
            if (foreignKey == null) {
                throw MondrianResource.instance()
                    .MustSpecifyForeignKeyForHierarchy.ex(
                        hierarchy.getUniqueName(),
                        cube.getName());
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
                    .MustSpecifyPrimaryKeyTableForHierarchy.ex(
                        hierarchy.getUniqueName());
            }
        } else {
            table = hierarchy.getRelation().find(tableName);
            if (table == null) {
                // todo: i18n msg
                throw Util.newError(
                    "no table '" + tableName
                    + "' found in hierarchy " + hierarchy.getUniqueName());
            }
        }
        return table;
    }

}

// End HierarchyUsage.java
