/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import java.util.Vector;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

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
	protected String name;
	protected String caption;
	protected String uniqueName;
	protected int memberType;
	protected String parentUniqueName;

	/**
	 * If a level has <code>LARGE_LEVEL_THRESHOLD</code> or more members, it
	 * is considered a <em>large level</em>.  This means that the PivotTable
	 * service will not bring all of the members back to the client, but
	 * instead bring them back in chunks.
	 */
	public static final int LARGE_LEVEL_THRESHOLD = 1000;

	// implement Exp, OlapElement, Member
	public String getQualifiedName() {
		return Util.getRes().getMdxMemberName(uniqueName);
	}
	public final int getType() {
		return CatMember;
	}
	public final String getName() {
		return name;
	}
	public final String getUniqueName() {
		return uniqueName;
	}
	public final String getCaption() {
		return caption;
	}
	public final String getParentUniqueName() {
		return parentUniqueName;
	}
	public final Hierarchy getHierarchy() {
		return level.hierarchy;
	}
	public final Cube getCube() {
		return level.hierarchy.dimension.cube;
	}
	public final OlapElement getParent() {
		return level;
	}
	public final Level getLevel() {
		return level;
	}
	public final int getDepth() {
		return level.depth;
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
	public OlapElement lookupChild(NameResolver st, String s) {
		return lookupChildMember(s);
	}

	// implement Member
	public Member lookupChildMember(String s)
	{
		// calculated members may not have children
		if (isCalculated()) {
			throw Util.getRes().newMdxCalcMemberCanNotHaveChildren(
				getUniqueName());
		}
		Member[] children = getCube().getMemberChildren(
			new Member[] {this});
		String childName = getUniqueName() + ".[" + s + "]";
		for (int i = 0; i < children.length; i++){
			if (childName.equals(children[i].getUniqueName()) ||
				childName.equals(
					removeCarriageReturn(children[i].getUniqueName()))) {
				return children[i];
			}
		}
		return null;
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
			return parentMember = (MemberBase)
				getCube().lookupMemberByUniqueName(
					parentUniqueName, failIfNotFound);
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
			if (parentMember == null) {
				parentMember = (MemberBase)
					getCube().lookupMemberByUniqueName(
						parentUniqueName, false);
			}
			return parentMember.isChildOrEqualTo(uniqueName);
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

	public Exp resolve(Query q)
	{
//		return q.lookupMemberByUniqueName(getUniqueName(), true);
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

	String removeCarriageReturn(String s)
	{
		String retString = "";
		for (int i = 0; i < s.length(); i++){
			if (s.charAt(i) == '\r')
				continue;
			retString += s.charAt(i);
		}
		return retString;
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
	public Exp getFormatStringExp() {
		return Literal.emptyString;
	}
}

// End MemberBase.java
