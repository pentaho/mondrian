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
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import mondrian.resource.MondrianResource;

import java.util.List;
import java.util.ArrayList;

/**
 * <code>MemberBase</code> is a partial implementation of {@link Member}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public abstract class MemberBase
    extends OlapElementBase
    implements Member
{

    protected Member parentMember;
    protected final Level level;
    protected String uniqueName;

    /**
     * Combines member type and other properties, such as whether the member
     * is the 'all' or 'null' member of its hierarchy and whether it is a
     * measure or is calculated, into an integer field.
     *
     * <p>The fields are:<ul>
     * <li>bits 0, 1, 2 ({@link #FLAG_TYPE_MASK}) are member type;
     * <li>bit 3 ({@link #FLAG_HIDDEN}) is set if the member is hidden;
     * <li>bit 4 ({@link #FLAG_ALL}) is set if this is the all member of its
     *     hierarchy;
     * <li>bit 5 ({@link #FLAG_NULL}) is set if this is the null member of its
     *     hierarchy;
     * <li>bit 6 ({@link #FLAG_CALCULATED}) is set if this is a calculated
     *     member.
     * <li>bit 7 ({@link #FLAG_MEASURE}) is set if this is a measure.
     * </ul>
     *
     * NOTE: jhyde, 2007/8/10. It is necessary to cache whether the member is
     * 'all', 'calculated' or 'null' in the member's state, because these
     * properties are used so often. If we used a virtual method call - say we
     * made each subclass implement 'boolean isNull()' - it would be slower.
     * We use one flags field rather than 4 boolean fields to save space.
     */
    protected final int flags;

    private static final int FLAG_TYPE_MASK = 0x07;
    private static final int FLAG_HIDDEN = 0x08;
    private static final int FLAG_ALL = 0x10;
    private static final int FLAG_NULL = 0x20;
    private static final int FLAG_CALCULATED = 0x40;
    private static final int FLAG_MEASURE = 0x80;

    protected String parentUniqueName;

    /**
     * Cached values of {@link mondrian.olap.Member.MemberType} enumeration.
     * Without caching, get excessive calls to {@link Object#clone}.
     */
    private static final MemberType[] MEMBER_TYPE_VALUES = MemberType.values();

    protected MemberBase(
        Member parentMember,
        Level level,
        MemberType memberType)
    {
        this.parentMember = parentMember;
        this.level = level;
        this.parentUniqueName = (parentMember == null)
            ? null
            : parentMember.getUniqueName();
        this.flags = memberType.ordinal()
            | (memberType == MemberType.ALL ? FLAG_ALL : 0)
            | (memberType == MemberType.NULL ? FLAG_NULL : 0)
            | (computeCalculated(memberType) ? FLAG_CALCULATED : 0)
            | (level.getHierarchy().getDimension().isMeasures()
               ? FLAG_MEASURE
               : 0);
    }

    protected MemberBase() {
        this.level = null;
        this.flags = 0;
        this.parentUniqueName = null;
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxMemberName.str(getUniqueName());
    }

    public abstract String getName();

    public String getUniqueName() {
        return uniqueName;
    }

    public String getCaption() {
        // if there is a member formatter for the members level,
        //  we will call this interface to provide the display string
        MemberFormatter mf = getLevel().getMemberFormatter();
        if (mf != null) {
            return mf.formatMember(this);
        }
        final String caption = super.getCaption();
        return (caption != null)
            ? caption
            : getName();
    }

    public String getParentUniqueName() {
        return parentUniqueName;
    }

    public Dimension getDimension() {
        return getLevel().getDimension();
    }

    public Hierarchy getHierarchy() {
        return getLevel().getHierarchy();
    }

    public Level getLevel() {
        return level;
    }

    public MemberType getMemberType() {
        return MEMBER_TYPE_VALUES[flags & FLAG_TYPE_MASK];
    }

    public String getDescription() {
        return (String) getPropertyValue(Property.DESCRIPTION.name);
    }

    public boolean isMeasure() {
        return (flags & FLAG_MEASURE) != 0;
    }

    public boolean isAll() {
        return (flags & FLAG_ALL) != 0;
    }

    public boolean isNull() {
        return (flags & FLAG_NULL) != 0;
    }

    public boolean isCalculated() {
        return (flags & FLAG_CALCULATED) != 0;
    }

    public boolean isEvaluated() {
        return isCalculated();
    }

    public OlapElement lookupChild(
        SchemaReader schemaReader,
        Id.Segment childName,
        MatchType matchType)
    {
        return schemaReader.lookupMemberChildByName(
            this, childName, matchType);
    }

    // implement Member
    public Member getParentMember() {
        // use the cache if possible (getAdoMember can be very expensive)
        if (parentUniqueName == null) {
            return null; // we are root member, which has no parent
        } else if (parentMember != null) {
            return parentMember;
        } else {
            boolean failIfNotFound = true;
            final Hierarchy hierarchy = getHierarchy();
            final SchemaReader schemaReader =
                hierarchy.getDimension().getSchema().getSchemaReader();
            List<Id.Segment> parentUniqueNameParts =
                Util.parseIdentifier(parentUniqueName);
            parentMember =
                schemaReader.getMemberByUniqueName(
                    parentUniqueNameParts, failIfNotFound);
            return parentMember;
        }
    }

    // implement Member
    public boolean isChildOrEqualTo(Member member) {
        return (member != null) && isChildOrEqualTo(member.getUniqueName());
    }

   /**
    * Returns whether this <code>Member</code>'s unique name is equal to, a
    * child of, or a descendent of a member whose unique name is
    * <code>uniqueName</code>.
    */
    public boolean isChildOrEqualTo(String uniqueName) {
        if (uniqueName == null) {
            return false;
        }

        // The mapping member uniqueName --> parent uniqueName is more
        // efficient than using getAdoMember().
        String thisUniqueName = getUniqueName();
        if (thisUniqueName.equals(uniqueName)) {
            //found a match
            return true;
        }
        String parentUniqueName = getParentUniqueName();
        return (parentUniqueName == null)
            // have reached root
            ? false
            // try candidate's parentMember
            : ((MemberBase) getParentMember()).isChildOrEqualTo(uniqueName);
    }

    /**
     * Computes the value to be returned by {@link #isCalculated()}, so it can
     * be cached in a variable.
     *
     * @param memberType Member type
     * @return Whether this member is calculated
     */
    protected boolean computeCalculated(final MemberType memberType) {
        // If the member is not created from the "with member ..." MDX, the
        // calculated will be null. But it may be still a calculated measure
        // stored in the cube.
        return isCalculatedInQuery() || memberType == MemberType.FORMULA;
    }

    public int getSolveOrder() {
        return -1;
    }

    /**
     * Returns the expression by which this member is calculated. The expression
     * is not null if and only if the member is not calculated.
     *
     * @post (return != null) == (isCalculated())
     */
    public Exp getExpression() {
        return null;
    }

    // implement Member
    public List<Member> getAncestorMembers() {
        List<Member> list = new ArrayList<Member>();
        Member parentMember = getParentMember();
        while (parentMember != null) {
            list.add(parentMember);
            parentMember = parentMember.getParentMember();
        }
        return list;
    }

    /**
     * Returns the ordinal of this member within its hierarchy.
     * The default implementation returns -1.
     */
    public int getOrdinal() {
        return -1;
    }

    /**
     * Returns the order key of this member among its siblings.
     * The default implementation returns null.
     */
    public Comparable getOrderKey() {
        return null;
    }

    public boolean isHidden() {
        return false;
    }

    public Member getDataMember() {
        return null;
    }

    public String getPropertyFormattedValue(String propertyName) {
        return getPropertyValue(propertyName).toString();
    }

    public boolean isParentChildLeaf() {
        return false;
    }
}

// End MemberBase.java
