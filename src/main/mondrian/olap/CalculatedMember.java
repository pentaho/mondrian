/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

/**
 * A <code>CalculatedMember</code> is a member based upon a
 * {@link Formula}.
 **/
public interface CalculatedMember extends Member {
    Formula getFormula();
}

// End CalculatedMember.java
