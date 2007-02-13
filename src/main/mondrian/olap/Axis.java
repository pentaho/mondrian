/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import java.util.List;

/**
 * A <code>Axis</code> is a component of a {@link Result}.
 * It contains a list of {@link Position}s.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public interface Axis {
    List<Position> getPositions();
}
// End Axis.java
