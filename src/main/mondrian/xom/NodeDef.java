/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 11 October, 2001
*/

package mondrian.xom;
import java.io.PrintWriter;

/**
 * <code>NodeDef</code> represents a node in a parse tree. It is a base class
 * for {@link ElementDef}, {@link TextDef}, etc.
 *
 * @author jhyde
 * @since 11 October, 2001
 * @version $Id$
 **/
public interface NodeDef {

	/**
	 * Returns the name of this node's tag.
	 **/
	String getName();

	/**
	 * Returns the type of this element (see {@link DOMWrapper#getType}).
	 **/
	int getType();

	/**
	 * Returns the text inside this node.
	 **/
	String getText();

	/**
	 * Returns the children of this node.
	 **/
	NodeDef[] getChildren();

	/**
	 * Outputs this element definition in XML to any XMLOutput.
	 * @param out the XMLOutput class to display the XML
	 **/
	void displayXML(XMLOutput out, int indent);

	/**
	 * Outputs this node to any PrintWriter,
	 * in a formatted fashion with automatic indenting.
	 * @param out the PrintWriter to which to write this NodeDef.
	 * @param indent the indentation level for the printout.
	 */
	void display(PrintWriter out, int indent);

	/**
	 * Retrieves the {@link DOMWrapper} which was used to create this
	 * node. Only works if this nodes's {@link MetaDef.Element#keepDef} was
	 * set; otherwise, returns <code>null</code>.
	 **/
	DOMWrapper getWrapper();
}


// End NodeDef.java
