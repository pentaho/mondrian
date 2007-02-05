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
import java.util.EnumSet;

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
}

// End UtilCompatibleJdk15.java
