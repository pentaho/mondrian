/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 19 September, 2002
*/
package mondrian.resource;



/**
 * A <code>ResourceInstance</code> is an occurrence of a {@link
 * ResourceDefinition} with a set of arguments. It can later be formatted to a
 * specific locale.
 */
public interface ResourceInstance {
	public String toString();
}

// End ResourceInstance.java
