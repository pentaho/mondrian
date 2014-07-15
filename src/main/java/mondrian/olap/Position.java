/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2007 Pentaho and others
// All Rights Reserved.
//
// jhyde, 6 August, 2001
*/
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
