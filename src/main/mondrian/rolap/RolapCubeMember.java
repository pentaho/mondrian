/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// wgorman, 19 October 2007
*/

package mondrian.rolap;

import java.util.*;

import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

/**
 * RolapCubeMember wraps RolapMembers and binds them to a specific cube.
 * RolapCubeMember wraps or overrides RolapMember methods that directly
 * reference the wrapped Member.  Methods that only contain calls to other
 * methods do not need wrapped.
 *
 * @author Will Gorman (wgorman@pentaho.org)
 * @version $Id$
 */
public class RolapCubeMember extends RolapMember {

    protected final String rolapAllMemberCubeName;

    protected final RolapMember rolapMember;

    protected final RolapCubeLevel rolapLevel;

    protected final RolapCube rolapCube;

    public RolapCubeMember(
        RolapCubeMember parent,
        RolapMember member,
        RolapCubeLevel level,
        RolapCube cube)
    {
        super();

        this.parentMember = parent;
        this.rolapMember = member;
        this.rolapLevel = level;
        this.rolapCube = cube;
        if (parent != null) {
            this.parentUniqueName = parent.getUniqueName();
        }
        if (member.isAll()) {
            // this is a special case ...
            // replace hierarchy name portion of all member with new name
            if (member.getLevel().getHierarchy().getName().equals(
                    level.getHierarchy().getName()))
            {
                rolapAllMemberCubeName = member.getName();
            } else {
                // special case if we're dealing with a closure
                String replacement =
                    level.getHierarchy().getName().replaceAll("\\$", "\\\\\\$");

                // convert string to regular expression
                String memberLevelName =
                    member.getLevel().getHierarchy().getName().replaceAll(
                        "\\.", "\\\\.");

                rolapAllMemberCubeName = member.getName().replaceAll(
                        memberLevelName,
                       replacement);
            }
            setUniqueName(rolapAllMemberCubeName);
        } else {
            rolapAllMemberCubeName = null;
            Object name = rolapMember.getPropertyValue(Property.NAME.name);
            if (name != null
                && !(rolapMember.getKey() != null
                     && name.equals(rolapMember.getKey())))
            {
                // Save memory by only saving the name as a property if it's
                // different from the key.
                setUniqueName(name);
            } else if (rolapMember.getKey() != null) {
                setUniqueName(rolapMember.getKey());
            }
        }
    }

    public int getDepth() {
        return rolapMember.getDepth();
    }

    public boolean isNull() {
        return rolapMember.isNull();
    }

    public boolean isMeasure() {
        return rolapMember.isMeasure();
    }

    public boolean isAll() {
        return rolapMember.isAll();
    }

    public RolapMember getRolapMember() {
        return rolapMember;
    }

    public Map<String, Annotation> getAnnotationMap() {
        return rolapMember.getAnnotationMap();
    }

    /**
     * Returns the cube this cube member belongs to.
     *
     * <p>This method is not in the {@link Member} interface, because regular
     * members may be shared, and therefore do not belong to a specific cube.
     *
     * @return Cube this cube member belongs to
     */
    public RolapCube getCube() {
        return rolapCube;
    }

    public Member getDataMember() {
        RolapMember member = (RolapMember) rolapMember.getDataMember();
        if (member != null) {
            RolapCubeMember cubeDataMember =
                new RolapCubeMember(
                    getParentMember(), member,
                    getLevel(), this.rolapCube);
            return cubeDataMember;
        } else {
            return null;
        }
    }

    public int compareTo(Object o) {
        // light wrapper around rolap member compareTo
        RolapCubeMember other = (RolapCubeMember) o;
        return rolapMember.compareTo(other.rolapMember);
    }

    public boolean equals(Object o) {
        return (o == this)
               || ((o instanceof RolapCubeMember)
                   && equals((RolapCubeMember) o));
    }

    public boolean equals(OlapElement o) {
        return o.getClass() == RolapCubeMember.class
            && equals((RolapCubeMember) o);
    }

    private boolean equals(RolapCubeMember that) {
        assert that != null; // public method should have checked
        // Do not use equalsIgnoreCase; unique names should be identical, and
        // hashCode assumes this.
        return this.rolapLevel.equals(that.rolapLevel)
                && this.rolapMember.equals(that.rolapMember);
    }

    public Object getKey() {
        return rolapMember.getKey();
    }

    // override with stricter return type
    public RolapCubeHierarchy getHierarchy() {
        return (RolapCubeHierarchy) super.getHierarchy();
    }

    /**
     * {@inheritDoc}
     *
     * <p>This method is central to how RolapCubeMember works. It allows
     * a member from the cache to be used within different usages of the same
     * shared dimension. The cache member is the same, but the RolapCubeMembers
     * wrapping the cache member report that they belong to different levels,
     * and hence different hierarchies, dimensions, and cubes.
     */
    // this is cube dependent
    public RolapCubeLevel getLevel() {
        return rolapLevel;
    }

    public String getName() {
        if (rolapMember.isAll()) {
            return rolapAllMemberCubeName;
        }
        return rolapMember.getName();
    }

    public Comparable getOrderKey() {
        return rolapMember.getOrderKey();
    }

    void setOrderKey(Comparable orderKey) {
        // this should never be called
        throw new UnsupportedOperationException();
    }

    public int getOrdinal() {
        return rolapMember.getOrdinal();
    }

    void setOrdinal(int ordinal) {
        rolapMember.setOrdinal(ordinal);
    }

    public synchronized void setProperty(String name, Object value) {
        rolapMember.setProperty(name, value);
    }

    public Object getPropertyValue(String propertyName, boolean matchCase) {
        // we need to wrap these children as rolap cube members
        Property property = Property.lookup(propertyName, matchCase);
        if (property != null) {
            Member parentMember;
            switch (property.ordinal) {
            case Property.CONTRIBUTING_CHILDREN_ORDINAL:
                List<RolapMember> list = new ArrayList<RolapMember>();
                List<RolapMember> origList =
                    (List) rolapMember.getPropertyValue(
                        propertyName, matchCase);
                for (RolapMember member : origList) {
                    list.add(
                        new RolapCubeMember(
                            this, member, this.getLevel(), this.rolapCube));
                }
                return list;

            case Property.DIMENSION_UNIQUE_NAME_ORDINAL:
                return getHierarchy().getDimension().getUniqueName();

            case Property.HIERARCHY_UNIQUE_NAME_ORDINAL:
                return getHierarchy().getUniqueName();

            case Property.LEVEL_UNIQUE_NAME_ORDINAL:
                return getLevel().getUniqueName();

            case Property.MEMBER_UNIQUE_NAME_ORDINAL:
                return getUniqueName();

            case Property.MEMBER_NAME_ORDINAL:
                return getName();

            case Property.MEMBER_CAPTION_ORDINAL:
                return getCaption();

            case Property.PARENT_UNIQUE_NAME_ORDINAL:
                parentMember = getParentMember();
                return parentMember == null ? null : parentMember
                        .getUniqueName();
            case Property.CHILDREN_CARDINALITY_ORDINAL:
                // because rolapcalculated member overrides this property,
                // we need to make sure it gets called
                if (rolapMember instanceof RolapCalculatedMember) {
                    return
                        rolapMember.getPropertyValue(propertyName, matchCase);
                } else {
                    return super.getPropertyValue(propertyName, matchCase);
                }

            case Property.MEMBER_KEY_ORDINAL:
            case Property.KEY_ORDINAL:
                return this == this.getHierarchy().getAllMember() ? 0
                    : getKey();
            }
        }
        // fall through to rolap member
        return rolapMember.getPropertyValue(propertyName, matchCase);
    }

    public int getSolveOrder() {
        return rolapMember.getSolveOrder();
    }

    protected Object getPropertyFromMap(
        String propertyName,
        boolean matchCase)
    {
        return rolapMember.getPropertyFromMap(propertyName, matchCase);
    }

    public final MemberType getMemberType() {
        return rolapMember.getMemberType();
    }

    public RolapCubeMember getParentMember() {
        return (RolapCubeMember) super.getParentMember();
    }

    public String getCaption() {
        return rolapMember.getCaption();
    }

    public boolean isCalculated() {
        return rolapMember.isCalculated();
    }

    public boolean isCalculatedInQuery() {
        return rolapMember.isCalculatedInQuery();
    }

    // this method is overridden to make sure that any HierarchyExpr returns
    // the cube hierarchy vs. shared hierarchy.  this is the case for
    // SqlMemberSource.RolapParentChildMemberNoClosure
    public Exp getExpression() {
        Exp exp = rolapMember.getExpression();
        if (exp instanceof ResolvedFunCall) {
            // convert any args to RolapCubeHierarchies
            ResolvedFunCall fcall = (ResolvedFunCall)exp;
            for (int i = 0; i < fcall.getArgCount(); i++) {
                if (fcall.getArg(i) instanceof HierarchyExpr) {
                    HierarchyExpr expr = (HierarchyExpr)fcall.getArg(i);
                    if (expr.getHierarchy().equals(
                        rolapMember.getHierarchy()))
                    {
                        fcall.getArgs()[i] =
                            new HierarchyExpr(this.getHierarchy());
                    }
                }
            }
        }
        return exp;
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader,
        Id.Segment childName,
        MatchType matchType)
    {
        return
            schemaReader.lookupMemberChildByName(this, childName, matchType);
    }

}

// End RolapCubeMember.java
