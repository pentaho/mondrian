/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import mondrian.olap.Exp;
import mondrian.olap.FunCall;
import mondrian.olap.Literal;
import mondrian.olap.Util;

import javax.olap.OLAPException;
import javax.olap.metadata.Member;
import javax.olap.query.dimensionfilters.DimensionInsertOffset;
import javax.olap.query.edgefilters.EdgeInsertOffset;
import javax.olap.query.enumerations.DimensionInsertOffsetType;
import javax.olap.query.enumerations.DimensionInsertOffsetTypeEnum;
import javax.olap.query.enumerations.SetActionType;
import javax.olap.query.enumerations.SetActionTypeEnum;
import javax.olap.query.querycoremodel.DimensionFilter;
import javax.olap.query.querycoremodel.MemberInsertOffset;

/**
 * A <code>MondrianDimensionFilter</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class MondrianDimensionFilter extends MondrianDimensionStep
		implements DimensionFilter {
	private SetActionType setAction;
	private MondrianInsertOffset dimensionInsertOffset;

	MondrianDimensionFilter(MondrianDimensionStepManager manager) {
		super(manager);
	}

	protected Exp combine(Exp previousExp, Exp exp) {
		if (setAction == SetActionTypeEnum.APPEND) {
			return new FunCall("Union", new Exp[] {previousExp, exp});
		} else if (setAction == SetActionTypeEnum.DIFFERENCE) {
			return new FunCall("Minus", new Exp[] {previousExp, exp});
		} else if (setAction == SetActionTypeEnum.INITIAL) {
//			Util.assertTrue(previousExp == null);
			return exp;
		} else if (setAction == SetActionTypeEnum.INSERT) {
			// todo: Implement "Insert(<set>,<set>,<number>|<member>)" and
			return new FunCall("Insert", new Exp[] {
				previousExp, exp, dimensionInsertOffset.convert()});
		} else if (setAction == SetActionTypeEnum.INTERSECTION) {
			return new FunCall("Intersect", new Exp[] {previousExp, exp});
		} else if (setAction == SetActionTypeEnum.PREPEND) {
			return new FunCall("Union", new Exp[] {exp, previousExp});
		} else {
			throw Util.newInternal("Unknown SetAction " + setAction);
		}
	}

	// object model methods

	public SetActionType getSetAction() throws OLAPException {
		return setAction;
	}

	public void setSetAction(SetActionType input) throws OLAPException {
		this.setAction = input;
	}

	public void setDimensionInsertOffset(DimensionInsertOffset input) throws OLAPException {
		this.dimensionInsertOffset = (MondrianInsertOffset) input;
	}

	public DimensionInsertOffset getDimensionInsertOffset() throws OLAPException {
		throw new UnsupportedOperationException();
	}

	public DimensionInsertOffset createDimensionInsertOffset(DimensionInsertOffsetType type) throws OLAPException {
		if (type == DimensionInsertOffsetTypeEnum.INTEGERINSERTOFFSET) {
			return new MondrianIntegerInsertOffset(this);
		} else if (type == DimensionInsertOffsetTypeEnum.MEMBERINSERTOFFSET) {
			return new MondrianMemberInsertOffset(this);
		} else {
			throw Util.newInternal("Unknown DimensionInsertOffsetType " + type);
		}
	}
}

/**
 * <code>MondrianInsertOffset</code> implements {@link EdgeInsertOffset} and
 * provides a basis for implementing {@link DimensionInsertOffset} and
 * {@link MemberInsertOffset}.
 */
abstract class MondrianInsertOffset extends QueryObjectSupport implements
		EdgeInsertOffset {
	private MondrianDimensionFilter dimensionFilter;

	MondrianInsertOffset(MondrianDimensionFilter dimensionFilter) {
		super(false);
		this.dimensionFilter = dimensionFilter;
	}

	public DimensionFilter getDimensionFilter() throws OLAPException {
		return dimensionFilter;
	}

	/** Converts this offset into an expression which can be used in a call to
	 * the "Insert(<set>,<set>,<member>|<number>)" function. */
	abstract Exp convert();
}

/**
 * <code>MondrianIntegerInsertOffset</code> implements
 * {@link DimensionInsertOffset}
 */
class MondrianIntegerInsertOffset extends MondrianInsertOffset
		implements DimensionInsertOffset {
	Integer value;

	MondrianIntegerInsertOffset(MondrianDimensionFilter dimensionFilter) {
		super(dimensionFilter);
	}

	Exp convert() {
		return Literal.create(value);
	}

	public Integer getValue() throws OLAPException {
		return value;
	}

	public void setValue(Integer input) throws OLAPException {
		this.value = input;
	}
}

/**
 * <code>MondrianMemberInsertOffset</code> implements
 * {@link MemberInsertOffset}
 */
class MondrianMemberInsertOffset extends MondrianInsertOffset
		implements MemberInsertOffset {
	Member member;

	MondrianMemberInsertOffset(MondrianDimensionFilter dimensionFilter) {
		super(dimensionFilter);
	}

	Exp convert() {
		return ((MondrianJolapMember) member).member;
	}

	public Member getMember() throws OLAPException {
		return member;
	}

	public void setMember(Member input) throws OLAPException {
		this.member = input;
	}
}

// End MondrianDimensionFilter.java