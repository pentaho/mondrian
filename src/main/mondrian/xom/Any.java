/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 31 October, 2001
*/

package mondrian.xom;

/**
 * An element which has 'Any' content.
 *
 * @author jhyde
 * @since 31 October, 2001
 * @version $Id$
 **/
public interface Any {

    NodeDef[] getChildren();
    void setChildren(NodeDef[] children);
}


// End Any.java
