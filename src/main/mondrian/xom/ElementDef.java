/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 6 November, 2000
*/

package mondrian.xom;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * ElementDef is the base class for all element definitions.  It specifies the
 * basic interface as well as provides useful services for all elements.
 **/
public abstract class ElementDef implements NodeDef, Serializable, Cloneable
{

    /**
     * getElementClass is a static helper function which finds the XMLDef class
     * corresponding to an Element.  The Element's tag must start with the
     * given prefix name, and
     * the remainder of the tag must correspond to a static inner class of
     * the given enclosing class.
     * @param wrapper the DOMWrapper whose class to look up.
     * @param enclosure a Class which encloses the Class to lookup.
     * @param prefix a prefix which must appear on the tag name.
     * @return the ElementDef Class corresponding to the element, or null if no
     * class could be found (possible if this is a String element.
     */
    public static Class getElementClass(DOMWrapper wrapper,
                                        Class enclosure,
                                        String prefix)
        throws XOMException
    {
        if(enclosure == null) {
            // don't try to find a class -- they will use GenericDef
            return null;
        }
        // wrapper must be of ELEMENT type.  If not, throw a XOMException.
        if(wrapper.getType() != DOMWrapper.ELEMENT)
            throw new XOMException("DOMWrapper must be of ELEMENT type.");

        // Retrieve the tag name.  It must start with the prefix.
        String tag = wrapper.getTagName();
        if(prefix == null)
            prefix = "";
        else if(!tag.startsWith(prefix))
            throw new XOMException("Element names must start "
                                      + "\"" + prefix + "\": "
                                      + tag + " is invalid.");

        // Remove the prefix and look for the name in the _elements field
        // of the enclosure class.  Note that the lookup is case-sensitive
        // even though XML tags are not.
        String className = tag.substring(prefix.length(), tag.length());
        className = XOMUtil.capitalize(className);
        Class elemClass = null;
        try {
            elemClass = Class.forName(enclosure.getName() + "$"
                                      + className);
        } catch (ClassNotFoundException ex) {
            return null;
        }
        return elemClass;
    }

    /**
     * constructElement is a helper function which builds the appropriate type
     * of ElementDef from an XML Element.  This version of the function takes
     * an Element and a Class object specifying the exact class to use in
     * constructing the element.
     * @param wrapper the DOM Element wrapper from which to build this class.
     * @param clazz the Class to use to construct this class.  It must have
     * a constructor which takes the Element type.
     * @return a fully constructed ElementDef of the type specified by
     * Class.
     * @throws XOMException if clazz has no constructor which takes Element,
     * or if construction fails.
     */
    public static NodeDef constructElement(DOMWrapper wrapper,
                                           Class elemClass)
        throws XOMException
    {
        // Find a constructor of this class which takes an "Element" object
        Constructor[] constructors = elemClass.getDeclaredConstructors();
        Constructor elemConstructor = null;
        for(int i=0; i<constructors.length; i++) {
            Class[] params = constructors[i].getParameterTypes();
            if(params.length == 1 && params[0] == DOMWrapper.class) {
                elemConstructor = constructors[i];
                break;
            }
        }
        if(elemConstructor == null)
            throw new XOMException("No constructor taking class DOMWrapper "
                                      + "could be found in class "
                                      + elemClass.getName());

        // Call the constructor to instantiate the object
        Object[] args = new Object[1];
        args[0] = wrapper;
        try {
            return (ElementDef)(elemConstructor.newInstance(args));
        } catch (InstantiationException ex) {
            throw new XOMException("Unable to instantiate object of class "
                                      + elemClass.getName() + ": "
                                      + ex.getMessage());
        } catch (InvocationTargetException ex) {
            // the Element constructor can only throw XOMException or
            // RuntimeException or Error, so cast to whichever type is appropriate
            // and throw here.
            Throwable target = ex.getTargetException();
            if(target instanceof XOMException)
                throw (XOMException)target;
            else if(target instanceof RuntimeException)
                throw (RuntimeException)target;
            else if(target instanceof Error)
                throw (Error)target;
            else
                throw new XOMException("Unexpected exception while "
                                          + "instantiating object: "
                                          + target.toString());
        } catch (IllegalAccessException ex) {
            throw new XOMException("Unable to instantiate object of class "
                                      + elemClass.getName() + ": "
                                      + ex.getMessage());
        }
    }

    /**
     * constructElement is a helper function which builds the appropriate type
     * of ElementDef from an XML Element.  This function should be used when
     * creating an ElementDef from a list of optional XML element types using
     * ElementParser.requiredOption.  Generally, it is better to call the
     * constructors of ElementDef subclasses directly if the exact type of
     * an element is known.
     * @param wrapper the DOM Element Wrapper from which to build this class.
     * @return an ElementDef whose exact type depends on the tag name of the
     * element definition.
     * @throws XOMException if no subclass of ElementDef can be found,
     * or if def is malformed.
     */
    public static NodeDef constructElement(
        DOMWrapper wrapper, Class enclosure, String prefix)
        throws XOMException
    {
        switch (wrapper.getType()) {
        case DOMWrapper.ELEMENT:
            Class elemClass = getElementClass(wrapper, enclosure, prefix);
            if (elemClass == null) {
                if (true) {
                    return new WrapperElementDef(wrapper, enclosure, prefix);
                } else {
                    throw new XOMException("No class corresponding to element "
                                           + wrapper.getTagName()
                                           + " could be found in enclosure "
                                           + enclosure.getName());
                }
            } else {
                return constructElement(wrapper, elemClass);
            }
        case DOMWrapper.COMMENT:
            return new CommentDef(wrapper.getText());
        case DOMWrapper.CDATA:
            return new CdataDef(wrapper.getText());
        case DOMWrapper.FREETEXT:
            return new TextDef(wrapper.getText());
        default:
            throw new XOMException("Unknown type: " + wrapper.getText());
        }
    }



    // implement NodeDef
    public void displayXML(XMLOutput out, int indent) {}

    public void displayXML(XMLOutput out)
    {
        displayXML(out, 0);
    }

    /**
     * The displayDiff function compares this element definition against another,
     * compiling a message containing all diffs.  It is used internally by
     * the equals(), diff(), and verifyEquals() functions.
     * @param other the ElementDef to which to compare this element.
     * @param out a PrintWriter to which to display any discovered differences,
     * or null if just doing an equality check (and no diff report is needed).
     * @param indent the current indentation level (used for nice display of diffs).
     * @return true if this and other match exactly, false if not.
     */
    public boolean displayDiff(ElementDef other, PrintWriter out, int indent)
    {
        return false;
    }

    // implement NodeDef
    public String getName()
    {
        return getClass().getName();
    }

    // implement NodeDef
    public int getType()
    {
        return DOMWrapper.ELEMENT;
    }

    // implement NodeDef
    public String getText()
    {
        return null;
    }

    /**
     * This function writes an indentation level to the given PrintWriter.
     * @param out the PrintWriter to which to write the indent.
     * @param indent the indentation level
     */
    protected static void displayIndent(PrintWriter out, int indent)
    {
        for(int i=0; i<indent; i++)
            out.print("   ");
    }

    /**
     * This convenience function displays a String value with the given
     * parameter name at the given indentation level.  It is meant to be
     * called by subclasses of ElementDef.
     * @param out the PrintWriter to which to write this String.
     * @param name the parameter name of this string.
     * @param value the value of the String parameter.
     * @param indent the indentation level.
     */
    protected static void displayString(PrintWriter out, String name,
                                        String value, int indent)
    {
        displayIndent(out, indent);
        if(value == null)
            out.println(name + ": null");
        else
            out.println(name + ": \"" + value + "\"");
    }

    /**
     * This convenience function displays an XML attribute value
     * with the given attribute name at the given indentation level.
     * It should be called by subclasses of ElementDef.
     * @param out the PrintWriter to which to write this String.
     * @param name the attribute name.
     * @param value the attribute value.
     * @param indent the indentation level.
     */
    protected static void displayAttribute(PrintWriter out, String name,
                                           Object value, int indent)
    {
        displayIndent(out, indent);
        if(value == null)
            out.println(name + " = null");
        else
            out.println(name + " = \"" + value.toString() + "\"");
    }

    /**
     * This convenience function displays any ElementDef with the given
     * parameter name at the given indentation level.
     * @param out the PrintWriter to which to write this ElementDef.
     * @param name the parameter name for this ElementDef.
     * @param value the parameter's value (as an ElementDef).
     * @param indent the indentation level.
     */
    protected static void displayElement(PrintWriter out, String name,
                                         ElementDef value, int indent)
    {
        displayIndent(out, indent);
        if(value == null)
            out.println(name + ": null");
        else {
            out.print(name + ": ");
            value.display(out, indent);
        }
    }

    /**
     * This convenience function displays any array of ElementDef values with
     * the given parameter name (assumed to represent an array) at the given
     * indentation level.  Each value of the array will be written on a
     * separate line with a new indentation.
     * @param out the PrintWriter to which to write this ElementDef.
     * @param name the parameter name for this ElementDef.
     * @param values the parameter's values (as an ElementDef[] array).
     * @param indent the indentation level.
     */
    protected static void displayElementArray(PrintWriter out, String name,
                                              NodeDef[] values, int indent)
    {
        displayIndent(out, indent);
        if(values == null)
            out.println(name + ": null array");
        else {
            out.println(name + ": array of " + values.length + " values");
            for(int i=0; i<values.length; i++) {
                displayIndent(out, indent);
                if(values[i] == null)
                    out.println(name + "[" + i + "]: null");
                else {
                    out.print(name + "[" + i + "]: ");
                    values[i].display(out, indent);
                }
            }
        }
    }

    /**
     * This convenience function displays any array of String values with
     * the given parameter name (assumed to represent an array) at the given
     * indentation level.  Each value of the array will be written on a
     * separate line with a new indentation.
     * @param out the PrintWriter to which to write this ElementDef.
     * @param name the parameter name for this ElementDef.
     * @param values the parameter's values (as a String[] array).
     * @param indent the indentation level.
     */
    protected static void displayStringArray(PrintWriter out, String name,
                                             String[] values, int indent)
    {
        displayIndent(out, indent);
        if(values == null)
            out.println(name + ": null array");
        else {
            out.println(name + ": array of " + values.length + " values");
            for(int i=0; i<values.length; i++) {
                displayIndent(out, indent);
                if(values[i] == null)
                    out.println(name + "[" + i + "]: null");
                else {
                    out.println(name + "[" + i + "]: " + values[i]);
                }
            }
        }
    }

    /**
     * This convenience function displays a String value in XML.
     * parameter name at the given indentation level.  It is meant to be
     * called by subclasses of ElementDef.
     * @param out XMLOutput class to which to generate XML.
     * @param tag the Tag name of this String object.
     * @param value the String value.
     */
    protected static void displayXMLString(XMLOutput out, String tag, String value)
    {
        if(value != null)
            out.stringTag(tag, value);
    }

    /**
     * This convenience function displays any ElementDef in XML.
     * @param out the XMLOutput class to which to generate XML.
     * @param value the ElementDef to display.
     */
    protected static void displayXMLElement(XMLOutput out,
                                            ElementDef value)
    {
        if(value != null)
            value.displayXML(out, 0);
    }

    /**
     * This convenience function displays an array of ElementDef values in XML.
     * @param out the XMLOutput class to which to generate XML.
     * @param value the ElementDef to display.
     */
    protected static void displayXMLElementArray(XMLOutput out,
                                                 NodeDef[] values)
    {
        if (values != null)
        {
            for(int i=0; i<values.length; i++)
                values[i].displayXML(out, 0);
        }
    }

    /**
     * This convenience function displays a String array in XML.
     * @param out the XMLOutput class to which to generate XML.
     * @param tag the tag name for the String elements.
     * @param values the actual string values.
     */
    protected static void displayXMLStringArray(XMLOutput out,
                                                String tag,
                                                String[] values)
    {
        for(int i=0; i<values.length; i++)
            out.stringTag(tag, values[i]);
    }

    /**
     * This convenience function displays differences in two versions of
     * the same string object.
     * @param name the object name.
     * @param value1 the first string.
     * @param value2 the second string.
     * @param out the PrintWriter to which to write differences.
     * @param indent the indentation level.
     * @return true if the strings match, false if not.
     */
    protected static boolean displayStringDiff(String name,
                                               String value1, String value2,
                                               PrintWriter out, int indent)
    {
        // True if both values are null.
        if(value1 == null && value2 == null)
            return true;

        // Deal with the cases where one value is set but the other is not.
        if(value2 == null) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("String " + name + ": mismatch: "
                            + value1.toString() + " vs null.");
            }
            return false;
        }
        if(value1 == null) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("String " + name + ": mismatch: "
                            + "null vs " + value2.toString() + ".");
            }
            return false;
        }

        // Finally, check the values themselves
        if(value1.equals(value2))
            return true;

        if(out != null) {
            displayIndent(out, indent);
            out.println("String " + name + ": mismatch: "
                        + value1.toString() + " vs "
                        + value2.toString() + ".");
        }
        return false;
    }

    /**
     * This convenience function displays differences in two versions of
     * the same XML attribute value.
     * @param name the attribute name.
     * @param value1 the first attribute value.
     * @param value2 the second attribute value.
     * @param out the PrintWriter to which to write differences.
     * @param indent the indentation level.
     * @return true if the values match, false if not.
     */
    protected static boolean displayAttributeDiff(String name,
                                                  Object value1, Object value2,
                                                  PrintWriter out, int indent)
    {
        // True if both values are null.
        if(value1 == null && value2 == null)
            return true;

        // Deal with the cases where one value is set but the other is not.
        if(value2 == null) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Attribute " + name + ": mismatch: "
                            + value1.toString() + " vs null.");
            }
            return false;
        }
        if(value1 == null) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Attribute " + name + ": mismatch: "
                            + "null vs " + value2.toString() + ".");
            }
            return false;
        }

        // Verify that types match
        if (value1.getClass() != value2.getClass()) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Attribute " + name + ": class mismatch: "
                            + value1.getClass().getName()
                            + " vs "
                            + value2.getClass().getName()
                            + ".");
            }
            return false;
        }

        // Finally, check the values themselves
        if(value1.equals(value2))
            return true;

        if(out != null) {
            displayIndent(out, indent);
            out.println("Attribute " + name + ": mismatch: "
                        + value1.toString() + " vs "
                        + value2.toString() + ".");
        }
        return false;
    }

    /**
     * This convenience function displays differences in the values of any
     * two ElementDefs, returning true if they match and false if not.
     * @param name the object name.
     * @param value1 the first value.
     * @param value2 the second value.
     * @param out the PrintWriter to which to write differences.
     * @param indent the indentation level.
     * @return true if the values match, false if not.
     */
    protected static boolean displayElementDiff(String name,
                                                NodeDef value1,
                                                NodeDef value2,
                                                PrintWriter out, int indent)
    {
        // True if both values are null.
        if(value1 == null && value2 == null)
            return true;

        // Deal with the cases where one value is set but the other is not.
        if(value2 == null) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Object " + name + ": mismatch: "
                            + "(...) vs null.");
            }
            return false;
        }
        if(value1 == null) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Object " + name + ": mismatch: "
                            + "null vs (...).");
            }
            return false;
        }

        // Verify that types match
        if (value1.getClass() != value2.getClass()) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Object " + name + ": class mismatch: "
                            + value1.getClass().getName()
                            + " vs "
                            + value2.getClass().getName()
                            + ".");
            }
            return false;
        }

        // Do a sub equality check
        return ((ElementDef) value1).displayDiff(
            (ElementDef) value2, out, indent);
    }

    /**
     * This convenience function diffs any array of ElementDef values with
     * the given array name.  All differences are written to the given
     * PrintWriter at the given indentation level.
     * @param name the array name.
     * @param values1 the first array.
     * @param values2 the second array.
     * @param out the PrintWriter to which to write differences.
     * @param indent the indentation level.
     * @return true if the both arrays match, false if there are any differences.
     */
    protected static boolean displayElementArrayDiff(String name,
                                                     NodeDef[] values1,
                                                     NodeDef[] values2,
                                                     PrintWriter out, int indent)
    {
        int length1 = 0;
        int length2 = 0;
        if (values1 != null)
            length1 = values1.length;
        if (values2 != null)
            length2 = values2.length;
        // Check array sizes
        //  a null array does not differ from an empty array
        if(length1 != length2) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Array " + name + ": size mismatch: "
                            + length1 + " vs "
                            + length2 + ".");
            }
            return false;
        }

        // Check each member of the array
        boolean diff = true;
        for(int i=0; i< length1; i++)
            diff = diff && displayElementDiff(name + "[" + i + "]",
                                              values1[i], values2[i],
                                              out, indent);
        return diff;
    }

    /**
     * This convenience function diffs any array of strings with
     * the given array name.  All differences are written to the given
     * PrintWriter at the given indentation level.
     * @param name the array name.
     * @param values1 the first array.
     * @param values2 the second array.
     * @param out the PrintWriter to which to write differences.
     * @param indent the indentation level.
     * @return true if the both arrays match, false if there are any differences.
     */
    protected static boolean displayStringArrayDiff(String name,
                                                    String[] values1,
                                                    String[] values2,
                                                    PrintWriter out, int indent)
    {
        // Check array sizes
        if(values1.length != values2.length) {
            if(out != null) {
                displayIndent(out, indent);
                out.println("Array " + name + ": size mismatch: "
                            + values1.length + " vs "
                            + values2.length + ".");
            }
            return false;
        }

        // Check each member of the array
        boolean diff = true;
        for(int i=0; i<values1.length; i++)
            diff = diff && displayStringDiff(name + "[" + i + "]",
                                             values1[i], values2[i],
                                             out, indent);
        return diff;
    }

    /**
     * The toString function automatically uses display() to produce a string
     * version of this ElementDef.  The indentation level is always zero.
     */
    public String toString()
    {
        StringWriter strOut = new StringWriter();
        PrintWriter prOut = new PrintWriter(strOut);
        display(prOut, 0);
        return strOut.toString();
    }

    /**
     * The toXML function automatically uses displayXML() to produce an XML
     * version of this ElementDef as a String.
     * @return an XML representation of this ElementDef, as a String.
     */
    public String toXML()
    {
        StringWriter writer = new StringWriter();
        XMLOutput out = new XMLOutput(writer);
        displayXML(out, 0);
        return writer.toString();
    }

    /**
     * The toCompactXML function automatically uses displayXML() to produce an XML
     * version of this ElementDef as a String.  The generated XML is
     * <i>compact</i>; free of unnecessary whitespace.  Compact XML is useful
     * when embedding XML in a CDATA section or transporting over the network.
     * @return an XML representation of this ElementDef, as a String.
     */
    public String toCompactXML()
    {
        StringWriter writer = new StringWriter();
        XMLOutput out = new XMLOutput(writer);
        out.setCompact(true);
        displayXML(out, 0);
        return writer.toString();
    }

    /**
     * The diff function compares this element against another, determining if
     * they are exactly equal.  If so, the function returns null.  If not,
     * it returns a String describing the differences.
     */
    public String diff(ElementDef other)
    {
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        boolean diff = displayDiff(other, out, 0);
        if(!diff)
            return writer.toString();
        else
            return null;
    }

    /**
     * Determines if this ElementDef is equal to other (deeply), returning true
     * if the two are equal.
     * @return true if this equals other, false if not.
     * @throws ClassCastException if other is not an ElementDef.
     */
    public boolean equals(Object other)
    {
            try {
        return displayDiff((ElementDef)other, null, 0);
            } catch (ClassCastException ex) {
                return false;
            }
    }

        /**
         * Returns a unique hash of this instance.
         * @return hash of the toXML() return value
         */
    public int hashCode()
    {
            return this.toXML().hashCode();
    }

    /**
     * Verifies that this ElementDef is equal to other, throwing a
     * XOMException with a lengthy explanation if equality
     * fails.
     * @param other the ElementDef to compare to this one.
     */
    public void verifyEqual(ElementDef other)
        throws XOMException
    {
        StringWriter writer = new StringWriter();
        PrintWriter out = new PrintWriter(writer);
        out.println();
        boolean diff = displayDiff(other, out, 1);
        out.println();
        if(!diff)
            throw new XOMException("Element definition mismatch: "
                                      + writer.toString());
    }

    /**
     * Clone an ElementDef.  Because all ElementDefs are serializable, we can
     * clone through a memory buffer.
     */
    protected Object clone()
        throws CloneNotSupportedException
    {
        try {
            return deepCopy();
        } catch (XOMException ex) {
            throw new CloneNotSupportedException(
                "Unable to clone " + getClass().getName() + ": "
                + ex.toString());
        }
    }

    /**
     * Public version of clone(); returns a deep copy of this ElementDef.
     */
    public ElementDef deepCopy()
        throws XOMException
    {
        try {
            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(byteBuffer);
            objOut.writeObject(this);
            objOut.flush();
            ByteArrayInputStream byteIn = new
                ByteArrayInputStream(byteBuffer.toByteArray());
            ObjectInputStream objIn = new ObjectInputStream(byteIn);
            ElementDef def = (ElementDef)(objIn.readObject());
            return def;
        } catch (IOException ex) {
            throw new XOMException(ex, "Failed to serialize-copy ElementDef");
        } catch (ClassNotFoundException ex) {
            throw new XOMException(ex, "Failed to serialize-copy ElementDef");
        }
    }

    // implement NodeDef
    public DOMWrapper getWrapper()
    {
        try {
            Field field = getClass().getField("_def");
            return (DOMWrapper) field.get(this);
        } catch (NoSuchFieldException ex) {
            return null;
        } catch (IllegalAccessException ex) {
            throw new Error(ex.toString() + " in getWrapper");
        }
    }

    // implement NodeDef
    public NodeDef[] getChildren()
    {
        // Still to be implemented.  Use reflection, based upon a static list
        // of XOM fields which each class provides?
        throw new Error();
    }

    public void addChild(NodeDef child) throws XOMException
    {
        XOMUtil.addChild(this, child);
    }

    public void addChildren(NodeDef[] children) throws XOMException
    {
        XOMUtil.addChildren(this, children);
    }

    protected static NodeDef[] getMixedChildren_new(
        DOMWrapper _def, Class clazz, String prefix) throws XOMException
    {
        DOMWrapper[] _elts = _def.getChildren();
        int count = 0;
        for (int i = 0; i < _elts.length; i++) {
            switch (_elts[i].getType()) {
            case DOMWrapper.ELEMENT:
            case DOMWrapper.CDATA:
            case DOMWrapper.COMMENT:
                count++;
                break;
            case DOMWrapper.FREETEXT:
            default:
                break;
            }
        }
        NodeDef[] children = new NodeDef[count];
        count = 0;
        for (int i = 0; i < _elts.length; i++) {
            switch (_elts[i].getType()) {
            case DOMWrapper.ELEMENT:
            case DOMWrapper.CDATA:
            case DOMWrapper.COMMENT:
                children[count++] = constructElement(_elts[i], clazz, prefix);
                break;
            case DOMWrapper.FREETEXT:
            default:
                break;
            }
        }
        return children;
    }

    protected static NodeDef[] getMixedChildren(
        DOMWrapper _def, Class clazz, String prefix) throws XOMException
    {
        DOMWrapper[] _elts = _def.getChildren();
        NodeDef[] children = new NodeDef[_elts.length];
        for (int i = 0; i < _elts.length; i++) {
            children[i] = constructElement(_elts[i], clazz, prefix);
        }
        return children;
    }

    protected static ElementDef[] getElementChildren(
        DOMWrapper _def, Class clazz, String prefix) throws XOMException
    {
        DOMWrapper[] _elts = _def.getElementChildren();
        ElementDef[] children = new ElementDef[_elts.length];
        for (int i = 0; i < children.length; i++) {
            children[i] = (ElementDef) constructElement(
                _elts[i], clazz, prefix);
        }
        return children;
    }


}


// End ElementDef.java
