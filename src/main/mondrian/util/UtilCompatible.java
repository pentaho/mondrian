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
import java.math.BigDecimal;

/**
 * Interface containing methods which are implemented differently in different
 * versions of the JDK.
 *
 * <p>The methods should not be called directly, only via the corresponding
 * static methods in {@link mondrian.olap.Util}, namely:<ul>
 * <li>{@link mondrian.olap.Util#enumSetOf(Enum, Enum[])}</li>
 * <li>{@link mondrian.olap.Util#enumSetNoneOf(Class)}</li>
 * <li>{@link mondrian.olap.Util#enumSetAllOf(Class)}</li>
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
    <E extends Enum<E>> Set<E> enumSetOf(E first, E... rest);
    <E extends Enum<E>> Set<E> enumSetNoneOf(Class<E> elementType);
    <E extends Enum<E>> Set<E> enumSetAllOf(Class<E> elementType);

    BigDecimal makeBigDecimalFromDouble(double d);

    String quotePattern(String s);
}

// End UtilCompatible.java
