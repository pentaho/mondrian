/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2009-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap;

import java.util.Map;

/**
 * An element that has annotations.
 *
 * @version $Id$
 * @author jhyde
 */
public interface Annotated {
    /**
     * Returns a list of annotations.
     *
     * <p>The map may be empty, never null.
     *
     * @return Map from annotation name to annotations.
     */
    Map<String, Annotation> getAnnotationMap();
}

// End Annotated.java
