/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import java.util.Vector;

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
	protected int memberType;
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
		return memberType;
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
	public final boolean isNull() {
		return memberType == NULL_MEMBER_TYPE;
	}
	public OlapElement lookupChild(SchemaReader schemaReader, String s) {
		return Util.lookupMemberChildByName(schemaReader, this, s);
	}

	// implement Member
	public Member getParentMember()
	{
		// use the cache if possible (getAdoMember can be v. expensive)
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
		if (uniqueName == null)
			return false;

		// The mapping member uniqueName --> parent uniqueName is more
		// efficient than using getAdoMember().
		String thisUniqueName = getUniqueName();
		if (thisUniqueName.equals(uniqueName)){
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
		String type = memberTypes[getMemberType()];
		return type.equals("formula");
	}

//  	/**
//  	 * Return direct children for every mdxMember in given array.
//  	 **/
//  	protected Member[] getChildren(Member[] parentMembers)
//  	{
//  		return getCube().getMemberChildren(parentMembers);
//  	}

	public Exp resolve(Resolver resolver) {
		return this;
	}

	// implement Member
	public Member[] getAncestorMembers()
	{
		Vector v = new Vector();
		MemberBase parentMember = (MemberBase) getParentMember();
		while (parentMember != null) {
			v.addElement(parentMember);
			parentMember = (MemberBase) parentMember.getParentMember();
		}
		Member[] mdxAncestorMembers = new Member[v.size()];
		v.copyInto(mdxAncestorMembers);
		return mdxAncestorMembers;
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
	 * Defaulty implementation returns -1.
	 */
	public int getOrdinal() {
		return -1;
  }

}

// End MemberBase.java
