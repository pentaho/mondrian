/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 5 October, 2001
*/

package mondrian.xom;
import java.io.PrintWriter;

/**
 * A <code>TextDef</code> represents piece of textual data in an XML document.
 * Free text (such as <code>Some text</code>) is represented by an actual
 * <code>TextDef</code>; comments (such as <code>&lt-- a comment --&gt;</code>)
 * by derived class {@link CommentDef}; and CDATA sections (such as
 * <code>&lt;![CDATA[Some text]]&gt;</code>) by derived class {@link CdataDef}.
 *
 * @author jhyde
 * @since 5 October, 2001
 * @version $Id$
 **/
public class TextDef implements NodeDef {

	public String s;

	/**
	 * Whether to print the data as is -- never quote as a CDATA
	 * section. Useful if the fragment contains a valid XML string.
	 **/
	boolean asIs;

	public TextDef()
	{
	}

	public TextDef(String s)
	{
		this.s = s;
	}

	public TextDef(String s, boolean asIs)
	{
		this.s = s;
		this.asIs = asIs;
	}

	public TextDef(mondrian.xom.DOMWrapper _def)
		throws mondrian.xom.XOMException
	{
		switch (_def.getType()) {
		case DOMWrapper.FREETEXT:
		case DOMWrapper.CDATA:
		case DOMWrapper.COMMENT:
			break;
		default:
			throw new XOMException(
				"cannot make CDATA/PCDATA element from a " + _def.getType());
		}
		this.s = _def.getText();
	}

	// override ElementDef
	public String getName()
	{
		return null;
	}

	// override ElementDef
	public String getText()
	{
		return s;
	}

	// implement NodeDef
	public NodeDef[] getChildren()
	{
		return XOMUtil.emptyNodeArray;
	}

	// implement NodeDef
	public DOMWrapper getWrapper()
	{
		return null;
	}

	// implement NodeDef
	public int getType()
	{
		return DOMWrapper.FREETEXT;
	}

	// implement NodeDef
	public void display(PrintWriter pw, int indent)
	{
		pw.print(s);
	}

	// override NodeDef
	public void displayXML(XMLOutput out, int indent)
	{ 
		if (out.getIgnorePcdata()) {
			return;
		}
		out.beginNode();
		if (asIs) {
			out.print(s);
		} else {
			boolean quote = true;
			out.cdata(s, quote);
		}
	}
}


// End TextDef.java
