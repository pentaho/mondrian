/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 12 December, 2000
*/

package mondrian.xom;
import java.io.PrintWriter;
import java.util.Vector;

/**
 * XMLAttrVector is an class which assists in writing XML attributes to a
 * stream.
 */
public class XMLAttrVector {

    // Vector to hold all attributes and their values.
    private Vector attrs;

    /**
     * This private helper class holds an attribute-value pair.  It is used
     * as the element of the vector attrs.
     */
    private static class AttrVal
    {
        public AttrVal(String attr, String val)
        {
            this.attr = attr;
            this.val = val;
        }

        public String attr;
        public String val;
    }

    /**
     * Construct an empty XMLAttrVector.  Attribute/value pairs may be added
     * with the add() functions below.
     */
    public XMLAttrVector()
    {
        attrs = new Vector();
    }

    /**
     * Returns the number of attributes.
     **/
    public int size()
    {
        return attrs.size();
    }

    /**
     * Add a new attribute/value pair based on a String value.  Note that
     * attrVal may be null, in which case no attribute/value pair is added.
     * @param attrName the name of the attribute.
     * @param attrVal the String value of the attribute.
     * @return this (to allow chaining)
     */
    public XMLAttrVector add(String attrName, Object attrVal)
    {
        if(attrVal != null)
            attrs.addElement(new AttrVal(attrName, attrVal.toString()));
        return this;
    }

    /**
     * Add a new attribute/value pair based on an int value.
     * @param attrName the name of the attribute.
     * @param attrVal the int value of the attribute.
     * @return this (to allow chaining)
     */
    public XMLAttrVector add(String attrName, int attrVal)
    {
        attrs.addElement(new AttrVal(attrName, ""+attrVal));
        return this;
    }

    /**
     * Add a new attribute/value pair based on a double value.
     * @param attrName the name of the attribute.
     * @param attrVal the double value of the attribute.
     * @return this (to allow chaining)
     */
    public XMLAttrVector add(String attrName, double attrVal)
    {
        attrs.addElement(new AttrVal(attrName, ""+attrVal));
        return this;
    }

    /**
     * Add a new attribute/value pair based on a boolean value.
     * True is represented as "true", and false as "false".
     * @param attrName the name of the attribute.
     * @param attrVal the boolean value of the attribute.
     * @return this (to allow chaining)
     */
    public XMLAttrVector add(String attrName, boolean attrVal)
    {
        if(attrVal)
            attrs.addElement(new AttrVal(attrName, "true"));
        else
            attrs.addElement(new AttrVal(attrName, "false"));
        return this;
    }

    /**
     * Displays the entire attribute/value pair list, given a PrintWriter
     * to which to display and an indentation level.
     * This function is typically called from XMLOutput.
     * @param out PrintWriter to which to write output.
     * @param indent indentation level.
     */
    public void display(PrintWriter out, int indent)
    {
        // The indentation level is not used; all attribute/value pairs
        // are rendered on the same line.
        for(int i=0; i<attrs.size(); i++) {
            AttrVal av = (AttrVal)(attrs.elementAt(i));
            if (av.val != null) {
                out.print(" ");
                out.print(av.attr);
                out.print("=\"");
                out.print(StringEscaper.xmlNumericEscaper.escapeString(av.val));
                out.print("\"");
            }
        }
    }
}


// End XMLAttrVector.java
