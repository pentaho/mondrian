/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.olap;

import java.util.List;

/**
 * A <code>Axis</code> is a component of a {@link Result}.
 * It contains a list of {@link Position}s.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public interface Axis {
    List<Position> getPositions();
}
// End Axis.java
