/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
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
