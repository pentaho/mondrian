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
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * A <code>GenericDef</code> is a {@link ElementDef} whose attributes and
 * children are stored in collections, not generated members.  It is convenient
 * for building XML documents, but is not strongly typed.
 *
 * @author jhyde
 * @since 3 October, 2001
 * @version $Id$
 **/
public class GenericDef extends ElementDef {
	private String tagName;
	private Vector children;
	private OrderedStringMap attributes;

	public GenericDef(String tagName)
	{
		this.tagName = tagName;
		this.children = new Vector();
		this.attributes = new OrderedStringMap();
	}

	// implement ElementDef
	public void display(PrintWriter out, int indent)
	{
		out.println(getName());
		for (int i = 0, count = attributes.size(); i < count; i++) {
			String key = attributes.keyAt(i);
			Object value = attributes.valueAt(i);
			displayAttribute(out, key, value.toString(), indent + 1);
		}
		for (int i = 0, count = children.size(); i < count; i++) {
			ElementDef child = (ElementDef) children.elementAt(i);
			displayElement(out, "?", child, indent + 1);
		}
	}

	// implement NodeDef
	public void displayXML(XMLOutput out, int indent)
	{
		out.beginBeginTag(tagName);
		for (int i = 0, count = attributes.size(); i < count; i++) {
			String key = attributes.keyAt(i);
			Object value = attributes.valueAt(i);
			out.attribute(key, value.toString());
		}
		out.endBeginTag(tagName);
		for (int i = 0, count = children.size(); i < count; i++) {
			NodeDef child = (NodeDef) children.elementAt(i);
			child.displayXML(out, indent + 1);
		}
		out.endTag(tagName);
	}

	// override ElementDef
	public int getType()
	{
		return DOMWrapper.ELEMENT;
	}

	/**
	 * Returns the tag name of this element, or null for TEXT elements.
	 */
	public String getName()
	{
		return tagName;
	}

	public void addChild(NodeDef element)
	{
		children.addElement(element);
	}

	public NodeDef[] getChildren()
	{
		NodeDef[] a = new NodeDef[children.size()];
		children.copyInto(a);
		return a;
	}

	public void setAttribute(String key, Object value)
	{
		attributes.put(key, value);
	}

	public Object getAttribute(String key)
	{
		return attributes.get(key);
	}

	private static class OrderedStringMap
	{
		Vector v;
		Hashtable h;

		OrderedStringMap()
		{
			v = new Vector();
			h = new Hashtable();
		}
		int size()
		{
			return v.size();
		}
		Object get(String key)
		{
			return h.get(key);
		}
		void put(String key, Object value)
		{
			if (h.put(key, value) == null) {
				// attribute does not have a value; append to vector
				v.addElement(new StringMapEntry(key,value));
			} else {
				// attribute already has a value; replace it
				for (int i = 0, count = v.size(); i < count; i++) {
					StringMapEntry entry = (StringMapEntry) v.elementAt(i);
					if (entry.key.equals(key)) {
						entry.value = value;
						return;
					}
				}
				throw new Error(
					"key " + key + " not found in OrderedStringMap");
			}
		}
		String keyAt(int i)
		{
			return ((StringMapEntry) v.elementAt(i)).key;
		}
		Object valueAt(int i)
		{
			return ((StringMapEntry) v.elementAt(i)).value;
		}
	};

	private static class StringMapEntry
	{
		String key;
		Object value;
		StringMapEntry(String key, Object value)
		{
			this.key = key;
			this.value = value;
		}
	}
}


// End GenericDef.java
