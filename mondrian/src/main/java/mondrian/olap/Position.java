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

import java.util.List;

/**
 * A <code>Position</code> is an item on an {@link Axis}.  It contains
 * one or more {@link Member}s.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public interface Position extends List<Member> {
}

// End Position.java
