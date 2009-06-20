/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap;

/**
 * Callback to redefine a member property display string.
 */
public interface PropertyFormatter {
    String formatProperty(
        Member member, String propertyName, Object propertyValue);
}

// End PropertyFormatter.java
