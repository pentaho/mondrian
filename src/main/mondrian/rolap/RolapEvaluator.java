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
import mondrian.util.Format;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapEvaluator implements Evaluator
{
	RolapCube cube;
	RolapMember[] currentMembers;
	Evaluator parent;
	CellReader cellReader;
	private static final RolapMember[] emptyMembers = {}; 

	RolapEvaluator(RolapCube cube)
	{
		this.cube = cube;
		RolapDimension[] dimensions = (RolapDimension[]) cube.getDimensions();
		currentMembers = new RolapMember[dimensions.length];
		for (int i = 0; i < dimensions.length; i++) {
			currentMembers[i] = (RolapMember)
				dimensions[i].getHierarchy().getDefaultMember();
		}
		parent = null;
		this.cellReader = cellReader;
	}
	private RolapEvaluator(
		RolapCube cube, RolapMember[] currentMembers, RolapEvaluator parent)
	{
		this.cube = cube;
		this.currentMembers = currentMembers;
		this.parent = parent;
		this.cellReader = parent.cellReader;
	}
	// implement Evaluator
	public Cube getCube()
	{
		return cube;
	}
	public Evaluator push(Member[] members)
	{
		RolapMember[] cloneCurrentMembers = new RolapMember[
			this.currentMembers.length];
		for (int i = 0; i < this.currentMembers.length; i++) {
			cloneCurrentMembers[i] = this.currentMembers[i];
		}
		for (int i = 0; i < members.length; i++) {
			RolapMember member = (RolapMember) members[i];
			int ordinal = member.getDimension().getOrdinal();
			cloneCurrentMembers[ordinal] = member;
		}
		return new RolapEvaluator(cube, cloneCurrentMembers, this);
	}
	public Evaluator pop()
	{
		return parent;
	}
	public Object xx(Literal literal) {
		return literal.s;
	}
	public Object xx(Parameter parameter) {
		return parameter.getValue();
	}
	public Object xx(FunCall funCall) {
		FunDef funDef = funCall.getFunDef();
		return funDef.evaluate(this, funCall.args);
	}
	public Object xx(Id id) {
		throw new Error("unsupported");
	}
	public Object xx(OlapElement mdxElement) {
		return mdxElement;
	}
	public Member setContext(Member member)
	{
		RolapMember m = (RolapMember) member;
		int ordinal = m.getDimension().getOrdinal();
		RolapMember previous = currentMembers[ordinal];
		currentMembers[ordinal] = m;
		return previous;
	}
	public void setContext(Member[] members)
	{
		for (int i = 0; i < members.length; i++) {
			setContext(members[i]);
		}
	}
	public Member getContext(Dimension dimension)
	{
		return currentMembers[dimension.getOrdinal()];
	}
	public Object evaluateCurrent()
	{
		int minSolve = Integer.MAX_VALUE;
		RolapCalculatedMember minSolveMember = null;
		for (int i = 0, count = currentMembers.length; i < count; i++) {
			if (currentMembers[i] instanceof RolapCalculatedMember) {
				int solve = currentMembers[i].getSolveOrder();
				if (solve < minSolve) {
					minSolve = solve;
					minSolveMember = (RolapCalculatedMember) currentMembers[i];
				}
			}
		}
		if (minSolve < Integer.MAX_VALUE) {
			// There is at least one calculated member. Expand the first one
			// with the lowest solve order.  todo: Handle cycles.
			Evaluator evaluator = push(emptyMembers);
			return minSolveMember.exp.evaluateScalar(evaluator);
		}
		return cellReader.get(this);
	}
	/**
	 * Retrieves the value of property <code>name</code>. If more than one
	 * member in the current context defines that property, the one with the
	 * highest solve order has precedence.
	 */
	Object getProperty(String name)
	{
		Object o = null;
		int maxSolve = Integer.MIN_VALUE;
		for (int i = 0; i < currentMembers.length; i++) {
			RolapMember member = currentMembers[i];
			Object p = member.getProperty(name);
			if (p != null) {
				int solve = member.getSolveOrder();
				if (solve > maxSolve) {
					o = p;
					maxSolve = solve;
				}
			}
		}
		return o;
	}
	private String getFormatString()
	{
		Exp formatExp = (Exp) getProperty(RolapMember.PROPERTY_FORMAT_EXP);
		Object o = formatExp.evaluate(this);
		return o.toString();
	}
	private Format getFormat()
	{
		String formatString = getFormatString();
		return Format.get(
				formatString, getCube().getConnection().getLocale());
	}

	/**
	 * Converts a value of this member into a string according to this member's
	 * format specification.
	 **/
	String format(Evaluator evaluator, Object o)
	{
		return getFormat().format(o);
	}

	public String format(Object o) {
		Format format = getFormat();
		return format.format(o);
	}

	class MeasureCellReader implements CellReader
	{
		public Object get(Evaluator evaluator)
		{
			RolapEvaluator rolapEvaluator = (RolapEvaluator) evaluator;
			RolapStoredMeasure measure = (RolapStoredMeasure)
				rolapEvaluator.currentMembers[0];
			CellReader cellReader = measure.getCellReader();
			return cellReader.get(evaluator);
		}
	}
};

// End RolapEvaluator.java
