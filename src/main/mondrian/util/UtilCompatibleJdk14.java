/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.math.BigDecimal;
import java.lang.reflect.Method;

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
        set.addAll(Arrays.asList(rest));
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

    public String quotePattern(String s) {
        int slashEIndex = s.indexOf("\\E");
        if (slashEIndex == -1) {
            return "\\Q" + s + "\\E";
        }
        StringBuilder sb = new StringBuilder(s.length() * 2);
        sb.append("\\Q");
        int current = 0;
        while ((slashEIndex = s.indexOf("\\E", current)) != -1) {
            sb.append(s.substring(current, slashEIndex));
            current = slashEIndex + 2;
            sb.append("\\E\\\\E\\Q");
        }
        sb.append(s.substring(current, s.length()));
        sb.append("\\E");
        return sb.toString();
    }

    public <T> T getAnnotation(
        Method method, String annotationClassName, T defaultValue)
    {
        return defaultValue;
    }
}

// End UtilCompatibleJdk14.java
