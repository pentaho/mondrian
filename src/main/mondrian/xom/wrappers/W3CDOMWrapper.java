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

package mondrian.xom.wrappers;

import mondrian.xom.DOMWrapper;
import mondrian.xom.XOMUtil;
import org.w3c.dom.CharacterData;
import org.w3c.dom.*;

/**
 * This implementation of DOMWrapper wraps any w3c DOM-compliant java
 * XML Parser.  
 */
public class W3CDOMWrapper implements DOMWrapper {

	private Node node;

	/**
	 * W3CDOMWrapper parses XML based on a Node.  The Node may be either an
	 * Element or some form of text node.
	 */
	public W3CDOMWrapper(Node node)
	{
		this.node = node;
	}

	/**
	 * Map the Node's type to DOMWrapper's simplified concept of type.
	 */
	public int getType()
	{
		int nodeType = node.getNodeType();
		switch (nodeType) {
		case Node.ELEMENT_NODE:
			return ELEMENT;
		case Node.COMMENT_NODE:
			return COMMENT;
		case Node.CDATA_SECTION_NODE:
			return CDATA;
		case Node.TEXT_NODE:
			return FREETEXT;
		default:
			return UNKNOWN;
		}
	}

	/**
	 * Retrieve the tag name directly.  Return null immediately if not an
	 * element.
	 **/
	public String getTagName()
	{
		if(getType() != ELEMENT)
			return null;
		return ((Element)node).getTagName();
	}

	/**
	 * Return the attribute.  Return null if the attribute isn't defined,
	 * or if not an element.  This behavior differs from the underlying DOM,
	 * which returns an empty string for undefined attributes.
	 */
	public String getAttribute(String attrName)
	{
		if(getType() != ELEMENT)
			return null;
		String attrVal = ((Element)node).getAttribute(attrName);
		if(attrVal == null || attrVal.length() == 0)
			return null;
		else
			return attrVal;
	}	

	// implement DOMWrapper
	public String[] getAttributeNames()
	{
		NamedNodeMap map = node.getAttributes();
		int count = map.getLength();
		String[] attributeNames = new String[count];
		for (int i = 0; i < count; i++) {
			attributeNames[i] = map.item(i).getLocalName();
		}
		return attributeNames;
	}

	/**
	 * Recursively unwrap and create the contained text.  If the node is a
	 * comment, return the comment text; but ignore comments inside elements.
	 **/
	public String getText()
	{
		if (node instanceof Comment) {
			return ((Comment)node).getData();
		} else {
			StringBuffer sbuf = new StringBuffer();
			appendNodeText(node, sbuf);
			return sbuf.toString();
		}
	}

	// implement DOMWrapper
	public String toXML()
	{
		boolean onlyElements = false;
		return XOMUtil.wrapperToXml(this, onlyElements);
	}

	/**
	 * Helper to collect all Text nodes into a buffer.
	 */
	private static void appendNodeText(Node node, StringBuffer sbuf)
	{
		if (node instanceof Comment) {
			// ignore it
		}
		else if (node instanceof CharacterData) {
			// Text
			sbuf.append(((CharacterData)node).getData());
		} else if (node instanceof Element) {
			NodeList nodeList = node.getChildNodes();
			for(int i=0; i<nodeList.getLength(); i++)
				appendNodeText(nodeList.item(i), sbuf);
		}
	}

	/**
	 * Retrieve all children, and build an array of W3CDOMWrappers around
	 * each child that is of TEXT or ELEMENT type to return.
	 */
	public DOMWrapper[] getChildren()
	{
		if(getType() != ELEMENT)
			return new DOMWrapper[0];

		NodeList nodeList = node.getChildNodes();

		// Count the elements that are TEXT or ELEMENTs.
		int count = 0;
		for(int i=0; i<nodeList.getLength(); i++) {
			Node nextNode = nodeList.item(i);
			if(nextNode instanceof Element || nextNode instanceof Text)
				count++;
		}

		// Create and populate the array
		DOMWrapper[] ret = new DOMWrapper[count];
		count = 0;
		for(int i=0; i<nodeList.getLength(); i++) {
			Node nextNode = nodeList.item(i);
			if(nextNode instanceof Element || nextNode instanceof Text)
				ret[count++] = new W3CDOMWrapper(nextNode);
		}

		// Done.
		return ret;
	}

	/**
	 * Retrieve all children, and build an array of W3CDOMWrappers around
	 * each ELEMENT child.
	 */
	public DOMWrapper[] getElementChildren()
	{
		if(getType() != ELEMENT)
			return new DOMWrapper[0];

		NodeList nodeList = node.getChildNodes();

		// Count the elements that are TEXT or ELEMENTs.
		int count = 0;
		for(int i=0; i<nodeList.getLength(); i++) {
			Node nextNode = nodeList.item(i);
			if(nextNode instanceof Element)
				count++;
		}

		// Create and populate the array
		DOMWrapper[] ret = new DOMWrapper[count];
		count = 0;
		for(int i=0; i<nodeList.getLength(); i++) {
			Node nextNode = nodeList.item(i);
			if(nextNode instanceof Element)
				ret[count++] = new W3CDOMWrapper(nextNode);
		}

		// Done.
		return ret;
	}

}


// End W3CDOMWrapper.java
