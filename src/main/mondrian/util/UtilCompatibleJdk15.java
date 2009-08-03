/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.Set;
import java.util.EnumSet;
import java.util.regex.Pattern;
import java.math.BigDecimal;
// Only in Java5 and above
import java.math.MathContext;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.annotation.Annotation;

/**
 * Implementation of {@link UtilCompatible} which runs in
 * JDK 1.5.
 *
 * <p>Prior to JDK 1.5, this class should never be loaded. Applications should
 * instantiate this class via {@link Class#forName(String)} or better, use
 * methods in {@link mondrian.olap.Util}, and not instantiate it at all.
 *
 * @author jhyde
 * @version $Id$
 * @since Feb 5, 2007
 */
public class UtilCompatibleJdk15 implements UtilCompatible {
    public <E extends Enum<E>> Set<E> enumSetOf(E first, E... rest) {
        return EnumSet.of(first, rest);
    }

    public <E extends Enum<E>> Set<E> enumSetNoneOf(Class<E> elementType) {
        return EnumSet.noneOf(elementType);
    }

    public <E extends Enum<E>> Set<E> enumSetAllOf(Class<E> elementType) {
        return EnumSet.allOf(elementType);
    }

    /**
     * This generates a BigDecimal with a precision reflecting
     * the precision of the input double.
     *
     * @param d input double
     * @return BigDecimal
     */
    public BigDecimal makeBigDecimalFromDouble(double d) {
        return new BigDecimal(d, MathContext.DECIMAL64);
    }

    public String quotePattern(String s) {
        return Pattern.quote(s);
    }

    @SuppressWarnings("unchecked")
    public <T> T getAnnotation(
        Method method, String annotationClassName, T defaultValue)
    {
        try {
            Class<? extends Annotation> annotationClass =
                (Class<? extends Annotation>)
                    Class.forName(annotationClassName);
            if (method.isAnnotationPresent(annotationClass)) {
                final Annotation annotation =
                    method.getAnnotation(annotationClass);
                final Method method1 =
                    annotation.getClass().getMethod("value");
                return (T) method1.invoke(annotation);
            }
        } catch (IllegalAccessException e) {
            return defaultValue;
        } catch (InvocationTargetException e) {
            return defaultValue;
        } catch (NoSuchMethodException e) {
            return defaultValue;
        } catch (ClassNotFoundException e) {
            return defaultValue;
        }
        return defaultValue;
    }
}

// End UtilCompatibleJdk15.java
