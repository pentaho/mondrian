/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 16 July, 2001
*/

package mondrian.xom;

/**
 * DOMWrapper implements a Wrapper around the Element class from any DOM-style
 * XML parser.  The wrapper is used to isolate ElementParser, ElementDef, and
 * all ElementDef subclasses from the specifics of the underlying XML
 * parser.
 */
public interface DOMWrapper {

	/**
	 * UNKNOWN is used for DOM Element types unsupported by the
	 * wrapper.
	 */
	public static final int UNKNOWN = 0;

	/**
	 * FREETEXT is a type of DOM Element representing a piece of text (but not
	 * a CDATA section).  For example, <code>Some text</code>.  FREETEXT
	 * elements always have a tag name of NULL and have no children.  It
	 * maps to a {@link TextDef}.
	 **/
	public static final int FREETEXT = 1;

	/**
	 * ELEMENT is a type of DOM Element representing a named tag, possibly
	 * containing attributes, child elements, and text.  It maps to a {@link
	 * ElementDef} (or a generated class derived from it), or a {@link
	 * GenericDef}.
	 **/
	public static final int ELEMENT = 2;

	/**
	 * COMMENT is a type of DOM Element representing an XML comment.  It maps
	 * to a {@link CommentDef}.
	 **/
	public static final int COMMENT = 3;

	/**
	 * CDATA is a type of DOM Element representing a piece of text embedded in
	 * a CDATA section, for example,
	 * <code>&lt;&#33;[CDATA[Some text]]&gt;</code>.
	 * CDATA elements always have a tag name of NULL and have no children.  It
	 * maps to a {@link CdataDef}.
	 **/
	public static final int CDATA = 4;

	/**
	 * Returns the type of this element/node.  DOMWrapper supports only four
	 * possibilities: FREETEXT, ELEMENT, COMMENT, CDATA.
	 */
	public int getType();

	/**
	 * Returns the tag name of this element, or null for TEXT elements.
	 */
	public String getTagName();

	/**
	 * Returns the value of the attribute with the given attrName.  If the
	 * attribute is not defined, this method returns null.
	 */
	public String getAttribute(String attrName);

	/**
	 * Returns a list of attribute names.
	 **/
	public String[] getAttributeNames();

	/**
	 * Returns a flattened representation of the text inside thie element.
	 * For a TEXT element, this returns the text itself.  For an ELEMENT
	 * element, this returns all pieces of text within the element,
	 * with all markup removed.
	 */
	public String getText();

	/**
	 * Returns this node serialized as XML.
	 **/
	public String toXML();

	/**
	 * Returns all children of this element, including TEXT elements, as
	 * an array of DOMWrappers.
	 */
	public DOMWrapper[] getChildren();

	/**
	 * Returns all element children of this element as an array of
	 * DOMWrappers.
	 */
	public DOMWrapper[] getElementChildren();

}


// End DOMWrapper.java
