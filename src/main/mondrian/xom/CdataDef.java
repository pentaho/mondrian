/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 October, 2001
*/

package mondrian.xom;


/**
 * A <code>CdataDef</code> represents a CDATA element.  It allows an
 * <code>&lt;Any&gt;</code> element to have mixed children.
 *
 * @author jhyde
 * @since 3 October, 2001
 * @version $Id$
 **/
public class CdataDef extends TextDef 
{
	public CdataDef()
	{
		super();
	}

	public CdataDef(String s)
	{
		super(s);
	}

	public CdataDef(DOMWrapper _def)
		throws mondrian.xom.XOMException
	{
		super(_def);
	}

	// implement NodeDef
	public int getType()
	{
		return DOMWrapper.CDATA;
	}

	// override NodeDef
	public void displayXML(XMLOutput out, int indent)
	{ 
		out.beginNode();
		out.cdata(s, true);
	}
}


// End CdataDef.java
