/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.rolap;

import mondrian.olap.Member;

/**
 * Interface implemented by all measures (both stored and calculated).
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public interface RolapMeasure extends Member {
    /**
     * Returns the object that formats cells of this measure, or null to use
     * default formatting.
     *
     * @return formatter
     */
    RolapResult.ValueFormatter getFormatter();
}

// End RolapMeasure.java
