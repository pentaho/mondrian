/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho
// All Rights Reserved.
*/

package mondrian.olap.type;

import mondrian.olap.*;

/**
 * Type of an MDX expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
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
     *
     * @return whether this Type uses the given Dimension
     */
    boolean usesDimension(Dimension dimension, boolean definitely);

    /**
     * Returns whether this type contains a given hierarchy.<p/>
     *
     * For example:
     * <ul>
     * <li><code>HierarchyType([Customer].[Gender])</code> uses only the
     *     <code>[Customer].[Gender]</code> hierarchy.</li>
     * <li><code>TupleType(MemberType([Customer].[Gender]),
     *           MemberType([Store].[Store]))</code>
     *     uses <code>[Gender]</code>  and <code>[Store]</code>
     *     dimensions.</li>
     * </ul><p/>
     *
     * The <code>definitely</code> parameter comes into play when the
     * dimensional information is incomplete. For example, when applied to
     * <code>TupleType(MemberType([Customer]), MemberType([Store]))</code>,
     * <code>usesDimension([Customer].[Gender], false)</code> returns true
     * because the expression returns a member of one hierarchy of the
     * <code>[Customer]</code> dimension and that might be a member of the
     * <code>[Customer].[Gender]</code> hierarchy; but
     * <code>usesDimension([Customer].[Gender], true)</code> returns false
     * because might return a member of a different hierarchy, such as
     * <code>[Customer].[State]</code>.
     *
     * @param hierarchy Hierarchy
     * @param definitely If true, returns true only if this type definitely
     *    uses the hierarchy
     *
     * @return whether this Type uses the given Hierarchy
     */
    boolean usesHierarchy(Hierarchy hierarchy, boolean definitely);

    /**
     * Returns the Dimension of this Type, or null if not known.
     * If not applicable, throws.
     *
     * @return the Dimension of this Type, or null if not known.
     */
    Dimension getDimension();

    /**
     * Returns the Hierarchy of this Type, or null if not known.
     * If not applicable, throws.
     *
     * @return the Hierarchy of this type, or null if not known
     */
    Hierarchy getHierarchy();

    /**
     * Returns the Level of this Type, or null if not known.
     * If not applicable, throws.
     *
     * @return the Level of this Type
     */
    Level getLevel();

    /**
     * Returns a Type which is more general than this and the given Type.
     * The type returned is broad enough to hold any value of either type,
     * but no broader. If there is no such type, returns null.
     *
     * <p>Some examples:<ul>
     * <li>The common type for StringType and NumericType is ScalarType.
     * <li>The common type for NumericType and DecimalType(4, 2) is
     *     NumericType.
     * <li>DimensionType and NumericType have no common type.
     * </ul></p>
     *
     * <p>If <code>conversionCount</code> is not null, implicit conversions
     * such as HierarchyType to DimensionType are considered; the parameter
     * is incremented by the number of conversions performed.
     *
     * <p>Some examples:<ul>
     * <li>The common type for HierarchyType(hierarchy=Time.Weekly)
     *     and LevelType(dimension=Time), if conversions are allowed, is
     *     HierarchyType(dimension=Time).
     * </ul>
     *
     * <p>One use of common types is to determine the types of the arguments
     * to the <code>Iif</code> function. For example, the call
     *
     * <blockquote><code>Iif(1 &gt; 2, [Measures].[Unit Sales],
     * 5)</code></blockquote>
     *
     * has type ScalarType, because DecimalType(-1, 0) is a subtype of
     * ScalarType, and MeasureType can be converted implicitly to ScalarType.
     *
     * @param type Type
     *
     * @param conversionCount Number of conversions; output parameter that is
     * incremented each time a conversion is performed; if null, conversions
     * are not considered
     *
     * @return More general type
     */
    Type computeCommonType(Type type, int[] conversionCount);

    /**
     * Returns whether a value is valid for a type.
     *
     * @param value Value
     * @return Whether value is valid for this type
     */
    boolean isInstance(Object value);

    /**
     * Returns the number of fields in a tuple type, or a set of tuples.
     * For most other types, in particular member type, returns 1.
     *
     * @return Arity of type
     */
    int getArity();
}

// End Type.java
