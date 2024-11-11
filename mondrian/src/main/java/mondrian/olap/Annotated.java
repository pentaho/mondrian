/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
