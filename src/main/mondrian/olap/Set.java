/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 April, 2000
*/

package mondrian.olap;

/**
 * a <code>Set</code> represents an MDX set.  Unlike other MDX objects, this
 * is not persistent: it can only be defined using a 'with set' clause of an
 * MDX query.
 **/
public interface Set extends OlapElement {
    void setName(String newName);
}


// End Set.java
