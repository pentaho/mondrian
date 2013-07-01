/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2009-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.util.Map;

/**
 * An element that has annotations.
 *
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
