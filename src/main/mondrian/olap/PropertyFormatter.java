/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * SPI to redefine a member property display string.
 *
 * @deprecated Use {@link mondrian.spi.PropertyFormatter}. This interface
 * exists for temporary backwards compatibility and will be removed
 * in mondrian-4.0.
 *
 * @version $Id$
 */
public interface PropertyFormatter extends mondrian.spi.PropertyFormatter {
}

// End PropertyFormatter.java
