/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.util.Pair;

import java.util.*;

/**
 * Where we keep the good stuff: values of localized properties (caption and
 * description) and annotations.
 *
 * <p>There are various implementations, most of which have the goal of being
 * very compact. For instance, many elements have no annotations, caption or
 * description, and therefore use the same empty larder object.</p>
 *
 * @see  Larders
 */
public interface Larder extends Annotated {
    /** Returns the localized properties on this element. */
    Map<Pair<Locale, LocalizedProperty>, String> translations();

    /** Returns the value of a given (locale, property) pair. */
    String get(LocalizedProperty prop, Locale locale);

    /** Returns the value of a property. */
    Object get(Property property);
}

// End Larder.java
