/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.Member.MemberType;

import java.util.*;

/**
 * Hierarchy that is associated with a specific Cube.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeHierarchy extends RolapHierarchy {

    final RolapCubeDimension cubeDimension;
    private final RolapHierarchy rolapHierarchy;
    private final int ordinal;

    /**
     * The raw member reader. For a member reader which incorporates access
     * control and deals with hidden members (if the hierarchy is ragged), use
     * {@link RolapSchemaLoader#createMemberReader}).
     */
    MemberReader memberReader;

    RolapMember defaultMember;

    /**
     * Creates a RolapCubeHierarchy.
     *
     * @param schemaLoader Schema loader
     * @param cubeDimension Dimension
     * @param rolapHierarchy Wrapped hierarchy
     * @param subName Name of hierarchy within dimension
     * @param uniqueName Unique name of hierarchy
     * @param ordinal Ordinal of hierarchy within cube
     */
    public RolapCubeHierarchy(
        RolapSchemaLoader schemaLoader,
        RolapCubeDimension cubeDimension,
        RolapHierarchy rolapHierarchy,
        String subName,
        String uniqueName,
        int ordinal,
        Larder larder)
    {
        super(
            cubeDimension,
            subName,
            uniqueName,
            rolapHierarchy.isVisible(),
            rolapHierarchy.hasAll(),
            rolapHierarchy.closureFor,
            rolapHierarchy.attribute,
            larder);
        this.ordinal = ordinal;
        this.rolapHierarchy = rolapHierarchy;
        this.cubeDimension = cubeDimension;
    }

    void initCubeHierarchy(
        RolapSchemaLoader schemaLoader,
        String allMemberName,
        String allMemberCaption)
    {
        for (RolapLevel level : rolapHierarchy.getLevelList()) {
            final Map<String, List<Larders.Resource>> resourceMap;
            final BitSet bitSet =
                schemaLoader.resourceHierarchyTags.get(
                    getCube() + "." + uniqueName);
            if (bitSet != null
                && (bitSet.get(level.getDepth())
                    || level.isAll()))
            {
                // We can't be sure whether there is a resource for the 'all'
                // member because we don't know its name when we are parsing
                // the resource file, so always give the 'all' level a resource
                // map.
                resourceMap = schemaLoader.resourceMap;
            } else {
                resourceMap = null;
            }
            final RolapClosure closure;
            if (level.closure != null) {
                RolapDimension dimension =
                    level.closure.closedPeerLevel.getHierarchy().getDimension();

                RolapCubeDimension cubeDimension =
                    new RolapCubeDimension(
                        getCube(),
                        dimension,
                        getDimension().getName() + "$Closure",
                        -1,
                        Larders.EMPTY);
                schemaLoader.initCubeDimension(
                    cubeDimension, null, getCube().hierarchyList);

                RolapCubeLevel closedPeerCubeLevel =
                    cubeDimension
                        .getHierarchyList().get(0)
                        .getLevelList().get(1);

                closure =
                    new RolapClosure(
                        closedPeerCubeLevel, level.closure.distanceColumn);
            } else {
                closure = null;
            }
            levelList.add(
                new RolapCubeLevel(level, this, resourceMap, closure));
        }

        if (hasAll) {
            allLevel = getLevelList().get(0);
        } else {
            // create an all level if one doesn't normally
            // exist in the hierarchy
            allLevel =
                new RolapCubeLevel(
                    rolapHierarchy.allLevel,
                    this,
                    schemaLoader.resourceMap,
                    null);
            allLevel.initLevel(schemaLoader);
        }

        // Create an all member.
        final Larders.LarderBuilder builder = new Larders.LarderBuilder();
        builder.name(allMemberName);
        if (allMemberCaption != null
            && !allMemberCaption.equals(allMemberName))
        {
            builder.caption(allMemberCaption);
        }
        this.allMember =
            new RolapMemberBase(
                null,
                (RolapCubeLevel) allLevel,
                Util.COMPARABLE_EMPTY_LIST,
                Member.MemberType.ALL,
                Util.makeFqName(allLevel.getHierarchy(), allMemberName),
                builder.build());
        this.allMember.setOrdinal(0);

        this.nullLevel =
            new RolapCubeLevel(rolapHierarchy.nullLevel, this, null, null);
        this.nullMember = new RolapNullMember((RolapCubeLevel) nullLevel);
    }

    // override with stricter return type
    @Override
    public RolapCubeDimension getDimension() {
        return (RolapCubeDimension) dimension;
    }

    public RolapHierarchy getRolapHierarchy() {
        return rolapHierarchy;
    }

    /**
     * Returns the ordinal of this hierarchy in its cube.
     *
     * @return Ordinal of this hierarchy in its cube
     */
    public final int getOrdinalInCube() {
        return ordinal;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RolapCubeHierarchy)) {
            return false;
        }

        RolapCubeHierarchy that = (RolapCubeHierarchy)o;
        return cubeDimension.equals(that.cubeDimension)
            && getUniqueName().equals(that.getUniqueName());
    }

    protected int computeHashCode() {
        return Util.hash(super.computeHashCode(), this.cubeDimension.cube);
    }

    public RolapMember createMember(
        Member _parent,
        Level _level,
        String name,
        Formula formula)
    {
        return createMember(_parent, _level, name, formula, name);
    }

    public RolapMember createMember(
        Member _parent,
        Level _level,
        String name,
        Formula formula,
        Comparable orderKey)
    {
        final RolapCubeLevel level = (RolapCubeLevel) _level;
        final RolapMember parent = (RolapMember) _parent;
        if (formula == null) {
            RolapMemberBase rolapMemberBase = new RolapMemberBase(
                parent, level, name, MemberType.REGULAR,
                RolapMemberBase.deriveUniqueName(
                    parent, level, name, false),
                Larders.ofName(name));
            rolapMemberBase.setOrderKey(orderKey);
            return rolapMemberBase;
        } else if (level.isMeasure()) {
            return new RolapCalculatedMeasure(
                parent, level, name, formula);
        } else {
            return new RolapCalculatedMember(
                parent, level, name, formula);
        }
    }

    public final RolapMember getDefaultMember() {
        assert defaultMember != null;
        return defaultMember;
    }

    /**
     * Sets default member of this Hierarchy.
     *
     * @param member Default member
     */
    public void setDefaultMember(RolapMember member) {
        if (member != null) {
            this.defaultMember = member;
        }
    }

    void setMemberReader(MemberReader memberReader) {
        this.memberReader = memberReader;
    }

    final MemberReader getMemberReader() {
        return memberReader;
    }

    public final RolapCube getCube() {
        return cubeDimension.cube;
    }

    public List<? extends RolapCubeLevel> getLevelList() {
        return Util.cast(levelList);
    }
}

// End RolapCubeHierarchy.java
