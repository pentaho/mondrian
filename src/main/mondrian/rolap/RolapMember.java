/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapMember extends MemberBase
{
	int ordinal;
	Object key;
	/**
	 * Maps property name to property value.
	 *
	 * <p> We expect there to be a lot of members, but few of them will
	 * have properties. So to reduce memory usage, when empty, this is set to
	 * an immutable empty set.
	 */
	private Map mapPropertyNameToValue = emptyMap;
	private static final Map emptyMap = Collections.unmodifiableMap(new HashMap(0));

	RolapMember(
		RolapMember parentMember, RolapLevel level, Object key, String name)
	{
		this.parentMember = parentMember;
		this.parentUniqueName = parentMember == null ? null:
			parentMember.getUniqueName();
		this.level = level;
		this.name = name;
		this.caption = name;
		this.key = key;
		this.memberType = 1; // adMemberRegular
		this.uniqueName = (parentMember == null)
			? Util.makeFqName(getHierarchy(), name)
			: Util.makeFqName(parentMember, name);
	}

	RolapMember(
		RolapMember parentMember, RolapLevel level, Object value)
	{
		this(parentMember, level, value, value.toString());
	}

	public int compareHierarchically(Member o)
	{
		RolapMember other = (RolapMember) o;
		if (this.parentMember == other.parentMember) {
			// including case where both parents are null
			return 0;
		} else {
			return this.ordinal - other.ordinal;
		}
	}

	public Member[] getMemberChildren() {
		return getHierarchy().getChildMembers(this);
	}

	public Member getLeadMember(int n)
	{
		return ((RolapHierarchy) getHierarchy()).memberReader.getLeadMember(
			this, n);
	}
	public boolean isCalculatedInQuery()
	{
		return false;
	}
	public void setName(String name)
	{
		throw new Error("unsupported");
	}
	/** The name of the property which holds the parsed format string. Internal. **/
	public static final String PROPERTY_FORMAT_EXP = "$format_exp";
	public void setProperty(String name, Object value) {
		if (mapPropertyNameToValue.isEmpty()) {
			// the empty map is shared and immutable; create our own
			mapPropertyNameToValue = new HashMap();
		}
		mapPropertyNameToValue.put(name, value);
	}
	public Object getPropertyValue(String name) {
		return mapPropertyNameToValue.get(name);
	}
	public Property[] getProperties() {
		return level.getInheritedProperties();
	}
	// implement Exp
	public Object evaluateScalar(Evaluator evaluator)
	{
		Member old = evaluator.setContext(this);
		Object value = evaluator.evaluateCurrent();
		evaluator.setContext(old);
		return value;
	}

	String quoteKeyForSql()
	{
		if ((((RolapLevel) level).flags & RolapLevel.NUMERIC) != 0) {
			return key.toString();
		} else {
			return RolapUtil.singleQuoteForSql(key.toString());
		}
	}

	int getSolveOrder() {
		return -1;
	}
};



// End RolapMember.java
