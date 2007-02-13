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
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.Evaluator;
import mondrian.olap.Util;

/**
 * A <code>CellReader</code> finds the cell value for the current context
 * held by <code>evaluator</code>.
 *
 * <p>It returns:<ul>
 * <li><code>null</code> if the source is unable to evaluate the cell (for
 *   example, <code>AggregatingCellReader</code> does not have the cell
 *   in its cache). This value should only be returned if the caller is
 *   expecting it.</li>
 * <li>{@link Util#nullValue} if the cell evaluates to null</li>
 * <li>{@link mondrian.olap.Util.ErrorCellValue} if the cell evaluates to an
 *   error</li>
 * <li>an Object representing a value (often a {@link Double} or a {@link
 *   java.math.BigDecimal}), otherwise</li>
 * </ul>
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
interface CellReader {
    /**
     * Returns the value of the cell which has the context described by the
     * evaluator.
     *
     * <p>If no aggregation contains the required cell, returns null.
     *
     * <p>If the value is null, returns {@link Util#nullValue}.
     */
    Object get(Evaluator evaluator);

    /**
     * Returns the number of times this cell reader has told a lie because the
     * required cell value is not in the cache.
     */
    int getMissCount();
}

// End CellReader.java
