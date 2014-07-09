/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2000-2005 Julian Hyde
// Copyright (C) 2005-2005 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * Namer contains the methods to retrieve localized attributes
 */
public interface Namer {
    public String getLocalResource(String uName, String defaultValue);
}


// End Namer.java
