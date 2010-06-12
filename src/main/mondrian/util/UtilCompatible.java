/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.math.BigDecimal;
import java.lang.reflect.Method;

/**
 * Interface containing methods which are implemented differently in different
 * versions of the JDK.
 *
 * <p>The methods should not be called directly, only via the corresponding
 * static methods in {@link mondrian.olap.Util}, namely:<ul>
 * <li>{@link mondrian.olap.Util#makeBigDecimalFromDouble(double)}</li>
 * <li>{@link mondrian.olap.Util#quotePattern(String)}</li>
 * </ul></p>
 *
 * <p>This interface could in principle be extended to allow native
 * implementations of methods, or to serve as a factory for entire classes
 * which have different implementations in different environments.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Feb 5, 2007
 */
public interface UtilCompatible {
    BigDecimal makeBigDecimalFromDouble(double d);

    String quotePattern(String s);

    <T> T getAnnotation(
        Method method, String annotationClassName, T defaultValue);
}

// End UtilCompatible.java
