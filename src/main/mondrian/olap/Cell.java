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

/**
 * A <code>Cell</code> is an item in the grid of a {@link Result}.  It is
 * returned by {@link Result#getCell}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public interface Cell {
	/**
	 * Returns the cell's raw value. This is useful for sending to further data
	 * processing, such as plotting a chart.
	 *
	 * <p> The value is never null. It may have various types:<ul>
	 *   <li>if the cell is null, the value is an instance of
	 *       {@link Util.NullCellValue};</li>
	 *   <li>if the cell contains an error, the value is an instance of
	 *       {@link Throwable};</li>
	 *   <li>otherwise, the type of this value depends upon the type of
	 *       measure: possible types include {@link java.math.BigDecimal},
	 *       {@link Double}, {@link Integer} and {@link String}.</li>
	 * </ul>
	 *
	 * @post return != null
	 * @post (return instanceof Throwable) == isError()
	 * @post (return instanceof Util.NullCellValue) == isNull()
	 */
	Object getValue();
	/**
	 * Returns the cell's value formatted according to the current format
	 * string, and locale-specific settings such as currency symbol. The
	 * current format string may itself be derived via an expression. For more
	 * information about format strings, see {@link mondrian.util.Format}.
	 */
	String getFormattedValue();
	/**
	 * Returns whether the cell's value is null.
	 */
	boolean isNull();
	/**
	 * Returns whether the cell's calculation returned an error.
	 */
	boolean isError();
}

// End Cell.java
