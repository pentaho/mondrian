/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.math.BigDecimal;

/**
 * Implementation of {@link UtilCompatible} which runs in
 * JDK 1.4.
 *
 * <p>The code uses JDK 1.5 constructs such as generics and for-each loops,
 * but retroweaver can convert these. It does not use
 * <code>java.util.EnumSet</code>, which is important, because retroweaver has
 * trouble with this.
 *
 * @author jhyde
 * @version $Id$
 * @since Feb 5, 2007
 */
public class UtilCompatibleJdk14 implements UtilCompatible {
    public <E extends Enum<E>> Set<E> enumSetOf(E first, E... rest) {
        HashSet<E> set = new HashSet<E>();
        set.add(first);
        for (E e : rest) {
            set.add(e);
        }
        return set;
    }

    public <E extends Enum<E>> Set<E> enumSetNoneOf(Class<E> elementType) {
        return new HashSet<E>();
    }

    public <E extends Enum<E>> Set<E> enumSetAllOf(Class<E> elementType) {
        return new HashSet<E>(Arrays.asList(elementType.getEnumConstants()));
    }

    /** 
     * This generates a BigDecimal that can have a precision that does
     * not reflect the precision of the input double.
     * 
     * @param d input double
     * @return BigDecimal
     */
    public BigDecimal makeBigDecimalFromDouble(double d) {
        return new BigDecimal(d);
    }
}

// End UtilCompatibleJdk14.java
