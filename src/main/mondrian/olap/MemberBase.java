/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2004 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import java.util.ArrayList;

/**
 * <code>MemberBase</code> is a partial implementation of {@link Member}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class MemberBase
	extends OlapElementBase
	implements Member
{
	protected MemberBase parentMember;
	protected LevelBase level;
	protected String uniqueName;
    /** Combines member type and whether is hidden, according to the following
     * relation: <code>flags == (isHidden ? 8 : 0) | memberType</code>. */
	protected int flags;
	protected String parentUniqueName;

	// implement Exp, OlapElement, Member
	public String getQualifiedName() {
		return Util.getRes().getMdxMemberName(uniqueName);
	}

    public final int getType() {
		return Category.Member;
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

		final String caption = (String) getPropertyValue(Property.PROPERTY_CAPTION);
		if (caption != null) {
			return caption;
		}
		return getName();
	}

    public final String getParentUniqueName() {
		return parentUniqueName;
	}

    public boolean usesDimension(Dimension dimension) {
		return level.hierarchy.dimension == dimension;
	}

    public final Hierarchy getHierarchy() {
		return level.hierarchy;
	}

    public final Level getLevel() {
		return level;
	}

    public final int getMemberType() {
		return flags & 7;
	}

    public String getDescription() {
		return null;
	}

    public final boolean isMeasure() {
		return level.hierarchy.dimension.isMeasures();
	}

    public final boolean isAll() {
		return level.isAll();
	}

    public boolean isNull() {
		return false;
	}

	public OlapElement lookupChild(SchemaReader schemaReader, String s) {
		return Util.lookupMemberChildByName(schemaReader, this, s);
	}

	// implement Member
	public Member getParentMember()
	{
		// use the cache if possible (getAdoMember can be very expensive)
		if (parentUniqueName == null) {
			return null; // we are root member, which has no parent
		} else if (parentMember != null) {
			return parentMember;
		} else {
			boolean failIfNotFound = true;
			final Hierarchy hierarchy = getHierarchy();
			final SchemaReader schemaReader = hierarchy.getDimension().getSchema().getSchemaReader();
			String[] parentUniqueNameParts = Util.explode(parentUniqueName);
			parentMember = (MemberBase) schemaReader.getMemberByUniqueName(
					parentUniqueNameParts, failIfNotFound);
			return parentMember;
		}
	}

	// implement Member
	public boolean isChildOrEqualTo(Member member)
	{
		if (member == null) {
			return false;
		} else {
			return isChildOrEqualTo(member.getUniqueName());
		}
	}

   /**
	* Return whether this <code>Member</code>'s unique name is equal to, a
	* child of, or a descendent of a member whose unique name is
	* <code>uniqueName</code>.
	**/
	public boolean isChildOrEqualTo(String uniqueName)
	{
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
		if (parentUniqueName == null) {
			return false; // have reached root
		} else {
			// try candidate's parentMember
			return ((MemberBase) getParentMember()).isChildOrEqualTo(uniqueName);
		}
	}

	// implement Member
	public boolean isCalculated()
	{
		if (isCalculatedInQuery()) {
			return true;
		}
		// If the member is not created from the "with member ..." MDX, the
		// calculated will be null. But it may be still a calculated measure
		// stored in the cube.
		return getMemberType() == FORMULA_MEMBER_TYPE;
	}

	public Exp resolve(Resolver resolver) {
		return this;
	}

	// implement Member
	public Member[] getAncestorMembers()
	{
		ArrayList list = new ArrayList();
		MemberBase parentMember = (MemberBase) getParentMember();
		while (parentMember != null) {
			list.add(parentMember);
			parentMember = (MemberBase) parentMember.getParentMember();
		}
        return (Member[]) list.toArray(new Member[list.size()]);
	}

	public void accept(Visitor visitor)
	{
		visitor.visit(this);
	}

	public void childrenAccept(Visitor visitor)
	{
		// don't generally traverse to children -- we could implement, if
		// necessary
	}

	/**
	 * Default implementation returns -1.
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
