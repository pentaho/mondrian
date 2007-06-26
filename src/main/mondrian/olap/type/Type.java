/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;
import mondrian.olap.Level;

/**
 * Type of an MDX expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
 */
public interface Type {
    /**
     * Returns whether this type contains a given dimension.<p/>
     *
     * For example:
     * <ul>
     * <li><code>DimensionType([Gender])</code> uses only the
     *     <code>[Gender]</code> dimension.</li>
     * <li><code>TupleType(MemberType([Gender]), MemberType([Store]))</code>
     *     uses <code>[Gender]</code>  and <code>[Store]</code>
     *     dimensions.</li>
     * </ul><p/>
     *
     * The <code>definitely</code> parameter comes into play when the
     * dimensional information is incomplete. For example, when applied to
     * <code>TupleType(MemberType(null), MemberType([Store]))</code>,
     * <code>usesDimension([Gender], false)</code> returns true because it
     * is possible that the expression returns a member of the
     * <code>[Gender]</code> dimension; but
     * <code>usesDimension([Gender], true)</code> returns true because it
     * is possible that the expression returns a member of the
     * <code>[Gender]</code> dimension.
     *
     * @param dimension Dimension
     * @param definitely If true, returns true only if this type definitely
     *    uses the dimension
     */
    boolean usesDimension(Dimension dimension, boolean definitely);

    /**
     * Returns the dimension of this type, or null if not known.
     */
    Dimension getDimension();

    /**
     * Returns the hierarchy of this type. If not applicable, throws.
     */
    Hierarchy getHierarchy();

    /**
     * Returns the level of this type, or null if not known.
     */
    Level getLevel();

}

// End Type.java
