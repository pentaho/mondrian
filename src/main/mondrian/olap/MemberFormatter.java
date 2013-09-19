/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap;

/**
 * An SPI to redefine the caption displayed for members.
 *
 * @deprecated Use {@link mondrian.spi.MemberFormatter}. This interface
 * exists for temporary backwards compatibility and will be removed
 * in mondrian-4.0.
 *
 * @author hhaas
 * @since 6 October, 2004
 */
public interface MemberFormatter extends mondrian.spi.MemberFormatter {
}

// End MemberFormatter.java
