/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * An <code>OlapElement</code> is a catalog object (dimension, hierarchy, level,
 * member).  It is also a node in a parse tree.
 **/
public interface OlapElement extends Exp
{
	String getUniqueName();
	String getName();
	String getDescription();
	void unparse(PrintWriter pw);
	void accept(Visitor visitor);
	void childrenAccept(Visitor visitor);
	/** Looks up a child element, returning null if it does not exist. */
	OlapElement lookupChild(SchemaReader schemaReader, String s);
	/** Returns the name of this element qualified by its class, for example
	 * "hierarchy 'Customers'". **/
	String getQualifiedName();
	String getCaption();
}

// End OlapElement.java
