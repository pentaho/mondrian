/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.mdx.HierarchyExpr;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;
import mondrian.olap.fun.VisualTotalsFunDef.VisualTotalMember;
import mondrian.util.Bug;

/**
 * RolapCubeMember wraps RolapMembers and binds them to a specific cube.
 * RolapCubeMember wraps or overrides RolapMember methods that directly
 * reference the wrapped Member.  Methods that only contain calls to other
 * methods do not need wrapped.
 *
 * @author Will Gorman, 19 October 2007
 */
public class RolapCubeMember
    extends DelegatingRolapMember
    implements RolapMemberInCube
{
    protected final RolapCubeLevel cubeLevel;
    protected final RolapCubeMember parentCubeMember;

    /**
     * Creates a RolapCubeMember.
     *
     * @param parent Parent member
     * @param member Member of underlying (non-cube) hierarchy
     * @param cubeLevel Level
     */
    public RolapCubeMember(
        RolapCubeMember parent, RolapMember member, RolapCubeLevel cubeLevel)
    {
        super(member);
        this.parentCubeMember = parent;
        this.cubeLevel = cubeLevel;
        assert !member.isAll() || getClass() != RolapCubeMember.class;
    }

    @Override
    public String getUniqueName() {
        // We are making a hard design decision to compute uniqueName every
        // time it is requested rather than storing it. RolapCubeMember is thin
        // wrapper, so cheap to construct that we don't need to cache instances.
        //
        // Storing uniqueName would make creation of RolapCubeMember more
        // expensive and use significantly more memory, so we don't do that.
        // That meakes each call to getUniqueName more expensive, so we try to
        // minimize the number of calls to this method.
        return cubeLevel.getHierarchy().convertMemberName(
            member.getUniqueName());
    }

    /**
     * Returns the underlying member. This is a member of a shared dimension and
     * does not belong to a cube.
     *
     * @return Underlying member
     */
    public final RolapMember getRolapMember() {
        return member;
    }

    // final is important for performance
    public final RolapCube getCube() {
        return cubeLevel.getCube();
    }

    public final RolapCubeMember getDataMember() {
        RolapMember member = (RolapMember) super.getDataMember();
        if (member == null) {
            return null;
        }
        return new RolapCubeMember(parentCubeMember, member, cubeLevel);
    }

    public int compareTo(Object o) {
        // light wrapper around rolap member compareTo
        RolapCubeMember other = null;
        if (o instanceof VisualTotalMember) {
            // REVIEW: Maybe VisualTotalMember should extend/implement
            // RolapCubeMember. Then we can remove special-cases such as this.
            other = (RolapCubeMember) ((VisualTotalMember) o).getMember();
        } else {
            other = (RolapCubeMember) o;
        }
        return member.compareTo(other.member);
    }

    public String toString() {
        return getUniqueName();
    }

    public int hashCode() {
        return member.hashCode();
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof RolapCubeMember) {
            return equals((RolapCubeMember) o);
        }
        if (o instanceof Member) {
            assert !Bug.BugSegregateRolapCubeMemberFixed;
            return getUniqueName().equals(((Member) o).getUniqueName());
        }
        return false;
    }

    public boolean equals(OlapElement o) {
        return o.getClass() == RolapCubeMember.class
            && equals((RolapCubeMember) o);
    }

    private boolean equals(RolapCubeMember that) {
        assert that != null; // public method should have checked
        // Assume that RolapCubeLevel is canonical. (Besides, its equals method
        // is very slow.)
        return this.cubeLevel == that.cubeLevel
               && this.member.equals(that.member);
    }

    // override with stricter return type; final important for performance
    public final RolapCubeHierarchy getHierarchy() {
        return cubeLevel.getHierarchy();
    }

    // override with stricter return type; final important for performance
    public final RolapCubeDimension getDimension() {
        return cubeLevel.getDimension();
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
    // override with stricter return type; final important for performance
    public final RolapCubeLevel getLevel() {
        return cubeLevel;
    }

    public void setProperty(String name, Object value) {
        synchronized (this) {
            super.setProperty(name, value);
        }
    }

    public Object getPropertyValue(String propertyName, boolean matchCase) {
        // we need to wrap these children as rolap cube members
        Property property = Property.lookup(propertyName, matchCase);
        if (property != null) {
            switch (property.ordinal) {
            case Property.DIMENSION_UNIQUE_NAME_ORDINAL:
                return getDimension().getUniqueName();

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
                return parentCubeMember == null
                    ? null
                    : parentCubeMember.getUniqueName();

            case Property.MEMBER_KEY_ORDINAL:
            case Property.KEY_ORDINAL:
                return this == this.getHierarchy().getAllMember() ? 0
                    : getKey();
            }
        }
        // fall through to rolap member
        return member.getPropertyValue(propertyName, matchCase);
    }

    public final RolapCubeMember getParentMember() {
        return parentCubeMember;
    }

    // this method is overridden to make sure that any HierarchyExpr returns
    // the cube hierarchy vs. shared hierarchy.  this is the case for
    // SqlMemberSource.RolapParentChildMemberNoClosure
    public Exp getExpression() {
        Exp exp = member.getExpression();
        if (exp instanceof ResolvedFunCall) {
            // convert any args to RolapCubeHierarchies
            ResolvedFunCall fcall = (ResolvedFunCall)exp;
            for (int i = 0; i < fcall.getArgCount(); i++) {
                if (fcall.getArg(i) instanceof HierarchyExpr) {
                    HierarchyExpr expr = (HierarchyExpr)fcall.getArg(i);
                    if (expr.getHierarchy().equals(
                            member.getHierarchy()))
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
