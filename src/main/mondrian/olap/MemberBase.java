/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
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
    implements Member {

    protected Member parentMember;
    protected final Level level;
    protected String uniqueName;

    /** 
     * Combines member type and whether is hidden. 
     *
     * <p>The lowest 3 bits are member type;
     * bit 4 is set if the member is hidden.
     */
    protected final int flags;
    protected final String parentUniqueName;

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
        this.flags = memberType.ordinal();
    }

    public String getQualifiedName() {
        return MondrianResource.instance().MdxMemberName.str(uniqueName);
    }

    public abstract String getName();

    public final String getUniqueName() {
        return uniqueName;
    }

    public final String getCaption() {
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

    public final String getParentUniqueName() {
        return parentUniqueName;
    }

    public Dimension getDimension() {
        return level.getDimension();
    }

    public final Hierarchy getHierarchy() {
        return level.getHierarchy();
    }

    public final Level getLevel() {
        return level;
    }

    public final MemberType getMemberType() {
        return MemberType.values()[flags & 7];
    }

    public String getDescription() {
        return null;
    }

    public final boolean isMeasure() {
        return level.getHierarchy().getDimension().isMeasures();
    }

    public final boolean isAll() {
        return level.isAll();
    }

    public boolean isNull() {
        return false;
    }

    public OlapElement lookupChild(SchemaReader schemaReader, String s) {
        return lookupChild(schemaReader, s, MatchType.EXACT);
    }
    
    public OlapElement lookupChild(
        SchemaReader schemaReader, String s, MatchType matchType)
    {
        return schemaReader.lookupMemberChildByName(this, s, matchType);
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
            String[] parentUniqueNameParts = Util.explode(parentUniqueName);
            parentMember = schemaReader.getMemberByUniqueName(
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

    // implement Member
    public boolean isCalculated() {
        return isCalculatedInQuery() || getMemberType() == MemberType.FORMULA;
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
    public Member[] getAncestorMembers() {
        List<Member> list = new ArrayList<Member>();
        Member parentMember = getParentMember();
        while (parentMember != null) {
            list.add(parentMember);
            parentMember = parentMember.getParentMember();
        }
        return list.toArray(new Member[list.size()]);
    }

    /**
     * Returns the ordinal of this member within its hierarchy.
     * The default implementation returns -1.
     */
    public int getOrdinal() {
        return -1;
    }

    public boolean isHidden() {
        return false;
    }

    public Member getDataMember() {
        return null;
    }

    public String getPropertyFormattedValue(String propertyName){
        return getPropertyValue(propertyName).toString();
    }
}

// End MemberBase.java
