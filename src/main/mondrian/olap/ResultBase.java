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

package mondrian.olap;
import java.io.PrintWriter;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public abstract class ResultBase implements Result {
	protected Query query;
	protected Axis[] axes;
	protected Axis slicerAxis;

	public Query getQuery() {
		return query;
	}

	// implement Result
	public Axis[] getAxes()
	{
		return axes;
	}
	// implement Result
	public Axis getSlicerAxis()
	{
		return slicerAxis;
	}
	// implement Result
	public void print(PrintWriter pw)
	{
		for (int i = -1; i < axes.length; i++) {
			pw.println("Axis #" + (i + 1) + ":");
			printAxis(pw, i < 0 ? slicerAxis : axes[i]);
		}
		// Usually there are 3 axes: {slicer, columns, rows}. Position is a
		// {column, row} pair. We call printRows with axis=2. When it recurses
		// to axis=-1, it prints.
		int[] pos = new int[axes.length];
		printRows(pw, axes.length - 1, pos);
	}
	private void printRows(PrintWriter pw, int axis, int[] pos)
	{
		Axis _axis = axis < 0 ? slicerAxis : axes[axis];
		for (int i = 0, count = _axis.positions.length; i < count; i++) {
			if (axis < 0) {
				if (i > 0) {
					pw.print(", ");
				}
				printCell(pw, pos);
			} else {
				pos[axis] = i;
				if (axis == 0) {
					int row = axis + 1 < pos.length ? pos[axis + 1] : 0;
					pw.print("Row #" + row + ": ");
				}
				printRows(pw, axis - 1, pos);
				if (axis == 0) {
					pw.println();
				}
			}
		}
	}
	private void printAxis(PrintWriter pw, Axis axis)
	{
		for (int i = 0; i < axis.positions.length; i++) {
			Position position = axis.positions[i];
			pw.print("{");
			for (int j = 0; j < position.members.length; j++) {
				Member member = position.members[j];
				if (j > 0) {
					pw.print(", ");
				}
				pw.print(member.getUniqueName());
			}
			pw.println("}");
		}
	}
	private void printCell(PrintWriter pw, int[] pos)
	{
		Cell cell = getCell(pos);
		pw.print(cell.getFormattedValue());
	}

	public Member getMember(int[] pos, Dimension dimension)
	{
		for (int i = -1; i < axes.length; i++) {
			Axis axis = slicerAxis;
			int index = 0;
			if (i >= 0) {
				axis = axes[i];
				index = pos[i];
			}
			Position position = axis.positions[index];
			for (int j = 0; j < position.members.length; j++) {
				Member member = position.members[j];
				if (member.getDimension() == dimension) {
					return member;
				}
			}
		}
		return dimension.getHierarchy().getDefaultMember();
	}

	// implement Result
	public void close() {}
}


// End ResultBase.java
