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
 * todo:
 *
 * @author jhyde
 * @since 11 October, 2001
 * @version $Id$
 **/
public class CommentDef extends TextDef {
	
	public CommentDef()
	{
		super();
	}
	
	public CommentDef(String s)
	{
		super(s);
	}
	
	public CommentDef(DOMWrapper _def) throws XOMException
	{
		super(_def);
	}

	// override ElementDef
	public int getType()
	{
		return DOMWrapper.COMMENT;
	}

	// implement NodeDef
	public void display(PrintWriter pw, int indent)
	{
		pw.print("<!-- ");
		pw.print(s);
		pw.print(" -->");
	}

	// implement NodeDef
	public void displayXML(XMLOutput out, int indent)
	{
		out.beginNode();
		out.print("<!-- ");
		out.print(s);
		out.print(" -->");
	}
}


// End CommentDef.java
