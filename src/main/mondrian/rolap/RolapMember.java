/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2004 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

import java.util.*;

/**
 * A <code>RolapMember</code> is a member of a {@link RolapHierarchy}. There are
 * sub-classes for {@link RolapStoredMeasure}, {@link RolapCalculatedMember}.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapMember extends MemberBase
{
	/** Ordinal of the member within the hierarchy. Some member readers do not
	 * use this property; in which case, they should leave it as its default,
	 * -1. */
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

    /**
     * Creates a RolapMember
     *
     * @param parentMember Parent member
     * @param level Level this member belongs to
     * @param key Key to this member in the underlying RDBMS
     * @param name Name of this member
     * @param flags Flags describing this member (see {@link #flags}
     */
    RolapMember(RolapMember parentMember, RolapLevel level, Object key,
            String name, int flags) {
		this.parentMember = parentMember;
		this.parentUniqueName = parentMember == null ? null:
			parentMember.getUniqueName();
		this.level = level;
		this.key = key;
		this.ordinal = -1;
		this.flags = flags;
		if (name != null &&
				!(key != null && name.equals(key.toString()))) {
			// Save memory by only saving the name as a property if it's different from
			// the key.
			setProperty(Property.PROPERTY_NAME, name);
		} else {
			setUniqueName();
		}
	}

	private void setUniqueName() {
		this.uniqueName = (parentMember == null)
			? Util.makeFqName(getHierarchy(), getName())
			: Util.makeFqName(parentMember, getName());
	}

	RolapMember(RolapMember parentMember, RolapLevel level, Object value) {
		this(parentMember, level, value, null, REGULAR_MEMBER_TYPE);
	}

	public boolean isCalculatedInQuery() {
		return false;
	}

	public String getName() {
		final String name = (String) getPropertyValue(Property.PROPERTY_NAME);
		if (name != null) {
			return name;
		}
		return key.toString();
	}

	public void setName(String name) {
		throw new Error("unsupported");
	}
	/**
	 * Sets a property of this member to a given value.
	 * <p>WARNING: Setting system properties such as "$name" may have nasty
	 * side-effects.
	 */
	public synchronized void setProperty(String name, Object value) {
		if (mapPropertyNameToValue.isEmpty()) {
			// the empty map is shared and immutable; create our own
			mapPropertyNameToValue = new HashMap();
		}
		mapPropertyNameToValue.put(name, value);
		if (name.equals(Property.PROPERTY_NAME)) {
			setUniqueName();
		}
	}

	public Object getPropertyValue(String name) {
		if (name.equals(Property.PROPERTY_CONTRIBUTING_CHILDREN)) {
			List list = new ArrayList();
			((RolapHierarchy) getHierarchy()).memberReader.getMemberChildren(this, list);
			return list;
		} else if (name.equals(Property.PROPERTY_MEMBER_UNIQUE_NAME)) {
            return getUniqueName();
        } else if (name.equals(Property.PROPERTY_MEMBER_CAPTION)) {
            return getCaption();
        } else if (name.equals(Property.PROPERTY_LEVEL_UNIQUE_NAME)) {
            return getLevel().getUniqueName();
        } else if (name.equals(Property.PROPERTY_LEVEL_NUMBER)) {
            return new Integer(getLevel().getDepth());
        }
		synchronized (this) {
			return mapPropertyNameToValue.get(name);
		}
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

	/**
	 * Returns whether this member is calculated using an expression.
	 * (<code>member.{@link #isCalculated}()</code> is equivalent to
	 * <code>member.{@link #getExpression}() != null</code>.)
	 */
	public boolean isCalculated() {
		return false;
	}

	/**
	 * Returns the expression by which this member is calculated. The expression
	 * is not null if and only if the member is not calculated.
	 *
	 * @post (return != null) == (isCalculated())
	 */
	Exp getExpression() {
		return null;
	}

	/**
	 * Returns the ordinal of the Rolap Member
	 */
	public int getOrdinal() {
		return ordinal;
	}

	/**
	 * implement the Comparable interface
	 */
	public int compareTo(Object o) {
		RolapMember other = (RolapMember)o;

		if (this.key != null && other.key == null)
			return 1; // not null is greater than null

		if (this.key == null && other.key != null)
			return -1; // null is less than not null

		// compare by unique name, if both keys are null
		if (this.key == null && other.key == null)
			return this.getUniqueName().compareTo(other.getUniqueName());

		// as both keys are not null, compare by key
		//  String, Double, Integer should be possible
		//  any key object should be "Comparable"
		// anyway - keys should be of the same class
		if (this.key.getClass().equals(other.key.getClass()))
			return ((Comparable)this.key).compareTo(other.key);

		// we should never compare objects with different key classes
		throw new java.lang.ClassCastException(
			"Comparing " + this.key.getClass().getName() +
			" against " + other.key.getClass().getName());

	}

    public boolean isHidden() {
        final RolapLevel rolapLevel = (RolapLevel) level;
        switch (rolapLevel.hideMemberCondition.ordinal_) {
        case RolapLevel.HideMemberCondition.NeverORDINAL:
            return false;
        case RolapLevel.HideMemberCondition.IfBlankNameORDINAL: {
            final String name = getName();
            return name == null || name.equals("");
        }
        case RolapLevel.HideMemberCondition.IfParentsNameORDINAL: {
            final Member parentMember = getParentMember();
            if (parentMember == null) {
                return false;
            }
            final String parentName = parentMember.getName();
            final String name = getName();
            return (parentName == null ? "" : parentName).equals(
                    name == null ? "" : name);
        }
        default:
            throw rolapLevel.hideMemberCondition.unexpected();
        }
    }
}


// End RolapMember.java
