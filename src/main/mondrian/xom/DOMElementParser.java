/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 6 November, 2000
*/

package mondrian.xom;
import java.util.*;
import java.lang.reflect.*;

/**
 * DOMElementParser is a utility wrapper around DOMWrapper.
 * implements a parseable stream of child DOMWrappers and also provides
 * validation on an XML document beyond the DTD.
 */
public class DOMElementParser {

	private DOMWrapper wrapper;
	private DOMWrapper[] children;
	private int currentIndex;
	private DOMWrapper currentChild;

	private int optionIndex;
	private String prefix;
	private Class enclosure;

	/**
	 * Constructs an new ElementParser based on an Element of the XML parse
	 * tree wrapped in a DOMWrapper, and a prefix (to be applied to all element 
	 * tags except the root), and the name of the enclosing class.
	 * @param wrapper a DOMWrapper representing the section of the XML parse tree
	 * to traverse.
	 */
	public DOMElementParser(DOMWrapper wrapper, String prefix, Class enclosure)
		throws XOMException
	{
		this.wrapper = wrapper;
		children = wrapper.getElementChildren();
		currentIndex = 0;
		currentChild = null;
		getNextElement();

		this.prefix = prefix;
		if(prefix == null)
			this.prefix = "";
		this.enclosure = enclosure;
	}	

	/**
	 * Private helper function to retrieve the next child element in sequence.
	 * @return the next element, or null if the enumerator has no more
	 * elements to return.
	 */
	private void getNextElement()
	{
		if(currentIndex >= children.length)
			currentChild = null;
		else
			currentChild = children[currentIndex++];
	}

	/**
	 * Private helper function to verify that the next element matches a
	 * specific name.
	 * @param name name of the element to match.  Names are not case-sensitive.
	 * @throws XOMException if there is no current element or the names do
	 * not match.
	 */
	private void requiredName(String name)
		throws XOMException
	{
		String augName = prefix + name;
		if(currentChild == null)
			throw new XOMException("Expected <"+ augName + "> but found "
									  + "nothing.");
		else if(!augName.equalsIgnoreCase(currentChild.getTagName()))
			throw new XOMException("Expected <" + augName + "> but found <"
									  + currentChild.getTagName() + ">");
	}	

	/**
	 * Private helper function to determine if the next element has the
	 * specified name.
	 * @return true if the next element's name matches <i>name</i>.  Matching
	 * is not case-sensitive.  Returns false if there is no next element or
	 * if the names don't match.
	 */
	private boolean optionalName(String name)
	{
		String augName = prefix + name;
		if(currentChild == null)
			return false;
		else if(augName.equalsIgnoreCase(currentChild.getTagName()))
			return true;
		else
			return false;
	}

	/**
	 * Returns the enclosure class associated with clazz, or falls back on
	 * the fixed enclosure if none can be found.
	 */
	private Class getEnclosureClass(Class clazz)
	{
		// Instead of using a fixed enclosure, derive it from the given Class.
		// If we can't figure it out, just use the given enclosure instead.
		Class thisEnclosure = enclosure;
		String className = clazz.getName();
		int dollarPos = className.indexOf('$');
		if(dollarPos >= 0) {
			String encName = className.substring(0, dollarPos);
			try {
				thisEnclosure = Class.forName(encName);
			} catch (ClassNotFoundException ex) {
				throw new AssertFailure("Enclosure class " + encName
								 + " not found.");
			}
		}
		return thisEnclosure;
	}
	
	/**
	 * Private helper function to determine if the next element's corresponding
	 * definition class is a subclass of the given class.  This may be used
	 * to detect if a name matches a class.
	 * @param clazz the class to match the next element against.
	 * @return true if the next element's name matches the given class, false
	 * otherwise.
	 * @throws XOMException if the next name is invalid (either doesn't
	 * start with DM or has no associated definition class).
	 */
	private boolean nameMatchesClass(Class clazz)
		throws XOMException
	{
		// Get the next name.  It must start with the set prefix, and it must
		// match a definition in the enclosure class.
		Class thisEnclosure = getEnclosureClass(clazz);
		Class nextClass = ElementDef.getElementClass(currentChild,
													 thisEnclosure, 
													 prefix);

		// Determine if nextClass is a subclass of clazz.  Return true if so.
		return nextClass != null &&
				clazz.isAssignableFrom(nextClass);
	}

	/**
	 * This function retrieves a required String element from this parser,
	 * advancing the parser after the read.
	 * @param elementName the name of the element to retrieve.
	 * @return the String value stored inside the element to retrieve.
	 * @throws XOMException if there is no element with the given name.
	 */
	public String requiredString(String elementName)
		throws XOMException
	{
		requiredName(elementName);
		String retval = currentChild.getText().trim();
		getNextElement();
		return retval;
	}

	/**
	 * This function retrieves an optional String element from this parser,
	 * advancing the parser if the element is found.
	 * If no element of the correct name is found, this function returns null.
	 * @param elementName the name of the element to retrieve.
	 * @return the String value stored inside the element to retrieve.
	 */
	public String optionalString(String elementName)
		throws XOMException
	{
		if(optionalName(elementName)) {
			String retval = currentChild.getText().trim();
			getNextElement();
			return retval;
		}
		else
			return null;
	}
	
	/**
	 * This function retrieves a required Element from this parser,
	 * advancing the parser after the read.
	 * @param elementName the name of the element to retrieve.
	 * @return the DOMWrapper to retrieve.
	 * @throws XOMException if there is no element with the given name.
	 */
	public DOMWrapper requiredElement(String elementName)
		throws XOMException
	{
		requiredName(elementName);
		DOMWrapper prevWrapper = currentChild;
		getNextElement();
		return prevWrapper;
	}

	/**
	 * This function is used to return a CDATA section as text.  It does
	 * no parsing.
	 * @return the contents of the CDATA element as text.
	 */
	public String getText()
	{
		return wrapper.getText().trim();
	}	

	/**
	 * This function retrieves an optional Element from this parser,
	 * advancing the parser if the element is found.
	 * If no element of the correct name is found, this function returns null.
	 * @param elementName the name of the element to retrieve.
	 * @return the DOMWrapper to retreive, or null if none found.
	 */
	public DOMWrapper optionalElement(String elementName)
		throws XOMException
	{
		if(optionalName(elementName)) {
			DOMWrapper prevChild = currentChild;
			getNextElement();
			return prevChild;
		}
		else
			return null;
	}

	/**
	 * This private helper function formats a list of element names into
	 * a readable string for error messages.
	 */
	private String formatOption(String[] elementNames)
	{
		StringBuffer sbuf = new StringBuffer();
		for(int i=0; i<elementNames.length; i++) {
			sbuf.append("<DM" + prefix);
			sbuf.append(elementNames[i]);
			sbuf.append(">");
			if(i < elementNames.length-1)
				sbuf.append(" or ");
		}		
		return sbuf.toString();
	}	

	/**
	 * This function retrieves a required element which may have one of a
	 * number of names.  The parser is advanced after the read.
	 * @param elementNames an array of allowed names.  Names are compared in
	 * a case-insensitive fashion.
	 * @return the first element with one of the given names.
	 * @throws XOMException if there are no more elements to read or if
	 * the next element's name is not in the elementNames list.
	 */
	public DOMWrapper requiredOption(String[] elementNames)
		throws XOMException
	{
		if(currentChild == null) {
			throw new XOMException("Expecting "
									  + formatOption(elementNames)
									  + " but found nothing.");
		} 
		else {
			for(int i=0; i<elementNames.length; i++) {
				String augName = "DM" + elementNames[i];
				if(augName.equalsIgnoreCase(
					currentChild.getTagName().toString())) {
					DOMWrapper prevWrapper = currentChild;
					getNextElement();
					optionIndex = i;
					return prevWrapper;
				}
			}
			
			// If we got here, no names match.
			throw new XOMException("Expecting "
									  + formatOption(elementNames)
									  + " but found <"
									  + currentChild.getTagName()
									  + ">.");
		}			
	}

	/**
	 * This function retrieves a required Element of a specific class
	 * from this parser, advancing the parser after the read.
	 * The class must be derived from ElementDef.
	 */
	public NodeDef requiredClass(Class classTemplate)
		throws XOMException
	{
		// The name must match the class.
		if(!nameMatchesClass(classTemplate))
			throw new XOMException("element <" + currentChild.getTagName()
									  + "> does not match expected class "
									  + classTemplate.getName());

		// Get the class corresponding to the current tag
		Class currentClass = ElementDef.getElementClass(currentChild,
														enclosure, prefix);
		
		// Get the element
		DOMWrapper prevWrapper = currentChild;
		getNextElement();

		// Construct an ElementDef of the correct class from the element
		return ElementDef.constructElement(prevWrapper, currentClass);
	}

	/**
	 * Returns the option index of the element returned through the last
	 * requiredOption call.
	 */
	public int lastOptionIndex()
	{
		return optionIndex;
	}	

	/**
	 * This function retrieves a required Attribute by name from the
	 * current Element.
	 * @param attrName the name of the attribute.
	 * @return the String value of the attribute.
	 * @throws XOMException if no attribute of this name is set.
	 */
	public String requiredAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			throw new XOMException("Required attribute "
									  + attrName + " is not set.");
		return attr.toString();
	}

	/**
	 * This static version of requiredAttribute uses any element definition
	 * as a basis for the attribute.  It is used by Plugin definitions to
	 * return attributes before the parser is created.
	 * @param def the Element in which to find the attribute.
	 * @param attrName the name of the attribute to retrieve.
	 * @param defaultVal the default value of the attribute to retrieve.
	 * @throws XOMException if no attribute of this name is set.
	 */
	public static String requiredDefAttribute(DOMWrapper wrapper,
											  String attrName,
											  String defaultVal)
		throws XOMException
	{		
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null) {
			if(defaultVal == null)
				throw new XOMException("Required attribute "
										  + attrName + " is not set.");
			else
				return defaultVal;
		}
		return attr.toString();
	}		

	/**
	 * This function retrieves an optional Attribute by name from the
	 * current Element.
	 * @param attrName the name of the attribute.
	 * @return the String value of the attribute, or null if the
	 * attribute is not set.
	 */
	public String optionalAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			return null;
		return attr.toString();
	}

	/**
	 * This function retrieves an optional Attribute by name from the
	 * current Element, converting it to an Integer.
	 * @param attrName the name of the attribute.
	 * @return the Integer value of the attribute, or null if the
	 * attribute is not set.
	 * @throws XOMException if the value is set to an illegal
	 * integer value.
	 */
	public Integer optionalIntegerAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			return null;
		try {
			return new Integer(attr.toString());
		} catch (NumberFormatException ex) {
			throw new XOMException("Illegal integer value \""
									  + attr.toString() + "\" for attribute "
									  + attrName + ": " + ex.getMessage());
		}		
	}		

   /**
	 * This function retrieves an optional Attribute by name from the
	 * current Element, converting it to a Double.
	 * @param attrName the name of the attribute.
	 * @return the Double value of the attribute, or null if the
	 * attribute is not set.
	 * @throws XOMException if the value is set to an illegal
	 * double value.
	 */
	public Double optionalDoubleAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			return null;
		try {
			return new Double(attr.toString());
		} catch (NumberFormatException ex) {
			throw new XOMException("Illegal double value \""
									  + attr.toString() + "\" for attribute "
									  + attrName + ": " + ex.getMessage());
		}		
	}

	/**
	 * This function retrieves an required Attribute by name from the
	 * current Element, converting it to an Integer.
	 * @param attrName the name of the attribute.
	 * @return the Integer value of the attribute.
	 * @throws XOMException if the value is not set, or is set to 
	 * an illegal integer value.
	 */
	public Integer requiredIntegerAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			throw new XOMException("Required integer attribute "
									  + attrName + " is not set.");
		try {
			return new Integer(attr.toString());
		} catch (NumberFormatException ex) {
			throw new XOMException("Illegal integer value \""
									  + attr.toString() + "\" for attribute "
									  + attrName + ": " + ex.getMessage());
		}		
	}

	/**
	 * This function retrieves an optional Attribute by name from the
	 * current Element, converting it to an Boolean.  The string value
	 * "true" (in any case) is considered TRUE.  Any other value is
	 * considered false.
	 * @param attrName the name of the attribute.
	 * @return the Boolean value of the attribute, or null if the
	 * attribute is not set.
	 * @throws XOMException if the value is set to an illegal
	 * integer value.
	 */
	public Boolean optionalBooleanAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			return null;
		return new Boolean(attr.toString());
	}		

	/**
	 * This function retrieves an required Attribute by name from the
	 * current Element, converting it to a Boolean.  The string value
	 * "true" (in any case) is considered TRUE.  Any other value is
	 * considered false.
	 * @param attrName the name of the attribute.
	 * @return the Boolean value of the attribute.
	 */
	public Boolean requiredBooleanAttribute(String attrName)
		throws XOMException
	{
		Object attr = wrapper.getAttribute(attrName);
		if(attr == null)
			throw new XOMException("Required boolean attribute "
									  + attrName + " is not set.");
		return new Boolean(attr.toString());
	}

	/**
	 * This function retrieves a collection of elements with the given name,
	 * returning them as an array.
	 * @param elemName the element name.
	 * @param min the minimum number of elements required in the array.  Set
	 * this parameter to 0 to indicate no minimum.
	 * @param max the maximum number of elements allowed in the array.  Set
	 * this parameter to 0 to indicate no maximum.
	 * @return an Element array containing the discovered elements.
	 * @throws XOMException if there are fewer than min or more than max
	 * elements with the name <i>elemName</i>.
	 */
	public DOMWrapper[] optionalArray(String elemName, int min, int max)
		throws XOMException
	{
		// First, read the appropriate elements into a vector.
		Vector vec = new Vector();
		String augName = "DM" + elemName;
		while(currentChild != null &&
			  augName.equalsIgnoreCase(currentChild.getTagName())) {
			vec.addElement(currentChild);
			getNextElement();
		}

		// Now, check for size violations
		if(min > 0 && vec.size() < min)
			throw new XOMException("Expecting at least " + min + " <"
									  + elemName + "> but found " + vec.size());
		if(max > 0 && vec.size() > max)
			throw new XOMException("Expecting at most " + max + " <"
									  + elemName + "> but found " +
									  vec.size());
		
		// Finally, convert to an array and return.
		DOMWrapper[] retval = new DOMWrapper[vec.size()];
		for(int i=0; i<retval.length; i++)
			retval[i] = (DOMWrapper)(vec.elementAt(i));
		return retval;
	}
	
	/**
	 * This function retrieves a collection of elements which are subclasses of
	 * the given class, returning them as an array.  The array will contain
	 * ElementDef objects automatically constructed to be of the correct class.
	 * @param elemClass the element class.
	 * @param min the minimum number of elements required in the array.  Set
	 * this parameter to 0 to indicate no minimum.
	 * @param max the maximum number of elements allowed in the array.  Set
	 * this parameter to 0 to indicate no maximum.
	 * @return an ElementDef array containing the discovered elements.
	 * @throws XOMException if there are fewer than min or more than max
	 * elements with the name <i>elemName</i>.
	 */
	public NodeDef[] classArray(Class elemClass, int min, int max)
		throws XOMException
	{
		// Instead of using a fixed enclosure, derive it from the given Class.
		// If we can't figure it out, just use the given enclosure instead.
		Class thisEnclosure = getEnclosureClass(elemClass);

		// First, read the appropriate elements into a vector.
		Vector vec = new Vector();
		while(currentChild != null &&
			  nameMatchesClass(elemClass)) {
			vec.addElement(currentChild);
			getNextElement();
		}

		// Now, check for size violations
		if(min > 0 && vec.size() < min)
			throw new XOMException("Expecting at least " + min 
									  + elemClass.getName()
									  + "> but found " + vec.size());
		if(max > 0 && vec.size() > max)
			throw new XOMException("Expecting at most " + max 
									  + elemClass.getName()
									  + "> but found " +
									  vec.size());
		
		// Finally, convert to an array and return.
		NodeDef[] retval = new NodeDef[vec.size()];
		for(int i=0; i<retval.length; i++) {
			retval[i] =
				ElementDef.constructElement((DOMWrapper)(vec.elementAt(i)),
											thisEnclosure, prefix);
		}		
		return retval;
	}
	
	/**
	 * This function retrieves an Element from this parser, advancing the
	 * parser if the element is found.  The Element's corresponding
	 * ElementDef class is looked up and its constructor is called
	 * automatically.  If the requested Element is not found the function
	 * returns null <i>unless</i> required is set to true.  In this case,
	 * a XOMException is thrown.
	 * @param elementClass the Class of the element to retrieve.
	 * @param required true to throw an exception if the element is not
	 * found, false to simply return null.
	 * @return the element, as an ElementDef, or null if it is not found
	 * and required is false.
	 * @throws XOMException if required is true and the element could not
	 * be found.
	 */ 
	public NodeDef getElement(Class elementClass,
							  boolean required)
		throws XOMException
	{
		// If current element is null, return null immediately
		if(currentChild == null)
			return null;

		// Check if the name matches the class
		if(!nameMatchesClass(elementClass)) {
			if(required)
				throw new XOMException("element <" + currentChild.getTagName()
										  + "> is not of expected type "
										  + elementClass.getName());
			else
				return null;
		}		

		

		// Get the class corresponding to the current tag.  This will be
		// equal to elementClass if the current content was declared using
		// an Element, but not if the current content was declared using
		// a Class.
		Class thisEnclosure = getEnclosureClass(elementClass);
		Class currentClass = ElementDef.getElementClass(currentChild,
														thisEnclosure, prefix);

		// Get the element
		DOMWrapper prevChild = currentChild;
		getNextElement();

		// Construct an ElementDef of the correct class from the element
		return ElementDef.constructElement(prevChild, currentClass);		
	}

	/**
	 * This function retrieves a collection of elements which are subclasses of
	 * the given class, returning them as an array.  The array will contain
	 * ElementDef objects automatically constructed to be of the correct class.
	 * @param elemClass the element class.
	 * @param min the minimum number of elements required in the array.  Set
	 * this parameter to 0 to indicate no minimum.
	 * @param max the maximum number of elements allowed in the array.  Set
	 * this parameter to 0 to indicate no maximum.
	 * @return an ElementDef array containing the discovered elements.
	 * @throws XOMException if there are fewer than min or more than max
	 * elements with the name <i>elemName</i>.
	 */
	public NodeDef[] getArray(Class elemClass, int min, int max)
		throws XOMException
	{
		return classArray(elemClass, min, max);
	}

	/**
	 * This function retrieves a String element from this parser,
	 * advancing the parser if the element is found.
	 * If no element of the correct name is found, this function returns null,
	 * unless required is true, in which case a XOMException is thrown.
	 * @param elementName the name of the element to retrieve.
	 * @param required true to throw an exception if the element is not
	 * found, false to simply return null.
	 * @return the String value stored inside the element to retrieve, or
	 * null if no element with the given elementName could be found.
	 */
	public String getString(String elementName, boolean required)
		throws XOMException
	{
		boolean found;
		if(required) {
			requiredName(elementName);
			found = true;
		}
		else
			found = optionalName(elementName);
		if(found) {
			String retval = currentChild.getText().trim();
			getNextElement();
			return retval;
		} 
		else
			return null;
	}

	/**
	 * This function returns a collection of String elements of the given
	 * name, returning them as an array.
	 * @param elemName the element name.
	 * @param min the minimum number of elements required in the array.  Set
	 * this parameter to 0 to indicate no minimum.
	 * @param max the maximum number of elements allowed in the array.  Set
	 * this parameter to 0 to indicate no maximum.
	 * @return a String array containing the discovered elements.
	 * @throws XOMException if there are fewer than min or more than max
	 * elements with the name <i>elemName</i>.
	 */
	public String[] getStringArray(String elemName, int min, int max)
		throws XOMException
	{
		// First, read the appropriate elements into a vector.
		Vector vec = new Vector();
		String augName = prefix + elemName;
		while(currentChild != null &&
			  augName.equalsIgnoreCase(currentChild.getTagName().toString())) {
			vec.addElement(currentChild);
			getNextElement();
		}

		// Now, check for size violations
		if(min > 0 && vec.size() < min)
			throw new XOMException("Expecting at least " + min + " <"
									  + elemName + "> but found " + vec.size());
		if(max > 0 && vec.size() > max)
			throw new XOMException("Expecting at most " + max + " <"
									  + elemName + "> but found " +
									  vec.size());
		
		// Finally, convert to an array, retrieve the text from each
		// element, and return.
		String[] retval = new String[vec.size()];
		for(int i=0; i<retval.length; i++)
			retval[i] = ((DOMWrapper)(vec.elementAt(i))).getText().trim();
		return retval;
	}		

	// Determine if a String is present anywhere in a given array.
	private boolean stringInArray(String str, String[] array)
	{
		for(int i=0; i<array.length; i++)
			if(str.equals(array[i]))
				return true;
		return false;
	}

	// Convert an array of Strings into a single String for display.
	private String arrayToString(String[] array)
	{
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("{");
		for(int i=0; i<array.length; i++) {
			sbuf.append(array[i]);
			if(i<array.length-1)
				sbuf.append(", ");
		}
		sbuf.append("}");
		return sbuf.toString();
	}	

	/**
	 * Get a Class object representing a plugin class, identified either
	 * directly by a Java package and Java class name, or indirectly
	 * by a Java package and Java class which defines a method called
	 * getXMLDefClass() to return the appropriate class.
	 * @param packageName the name of the Java package containing the
	 * plugin class.
	 * @param className the name of the plugin definition class.
	 * @throws XOMException if the plugin class cannot be located
	 * or if the designated class is not suitable as a plugin class.
	 */
	public static Class getPluginClass(String packageName,
									   String className)
		throws XOMException
	{
		Class managerClass = null;
		try {
			managerClass = Class.forName(packageName + "." + className);
		} catch (ClassNotFoundException ex) {
			throw new XOMException("Unable to locate plugin class "
									  + packageName + "."
									  + className + ": "
									  + ex.getMessage());
		}		

		return getPluginClass(managerClass);
	}

	/**
	 * Get a Class object representing a plugin class, given a manager
	 * class that implements the static method getXMLDefClass().
	 * @param managerClass any Class that implements getXMLDefClass.
	 * @return the plugin Class.
	 */
	public static Class getPluginClass(Class managerClass)
		throws XOMException
	{		
		// Look for a static method called getXMLDefClass which returns
		// type Class.  If we find this method, call it to produce the
		// actual plugin class.  Otherwise, throw an exception; the
		// class we selected is inappropriate.
		Method[] methods = managerClass.getMethods();
		for(int i=0; i<methods.length; i++) {
			// Must be static, take no args, and return Class.
			if(methods[i].getParameterTypes().length != 0)
				continue;
			if(!(methods[i].getReturnType() == Class.class))
				continue;
			if(!(Modifier.isStatic(methods[i].getModifiers())))
				continue;
			
			// Invoke the method here.
			try {
				Object[] args = new Object[0];
				return (Class)(methods[i].invoke(null, args));
			} catch (InvocationTargetException ex) {
				throw new XOMException("Exception while retrieving "
										  + "plugin class: " +
										  ex.getTargetException().toString());
			} catch (IllegalAccessException ex) {
				throw new XOMException("Illegal access while retrieving "
										  + "plugin class: " +
										  ex.getMessage());
			}			
		}

		// Class is inappropriate.
		throw new XOMException("Plugin class " + managerClass.getName()
								  + " is not an appropriate plugin class; "
								  + "getXMLDefClass() is not defined.");
	}

	/**
	 * Retrieve an Attribute from the parser.  The Attribute may be of any
	 * Java class, provided that the class supports a constructor from the
	 * String class.  The Attribute's value will be returned as an Object,
	 * which must then be cast to the appropraite type.  If the attribute
	 * is not defined and has no default, either null is returned (if
	 * required is false), or a XOMException is thrown (if required is
	 * true).
	 * @param attrName the name of the attribute to retreive.
	 * @param attrType a String naming a Java Class to serve as the type.
	 * If attrType contains a "." character, the class is looked up directly
	 * from the type name.  Otherwise, the class is looked up in the
	 * java.lang package.  Finally, the class must have a constructor which
	 * takes a String as an argument.
	 * @param defaultValue the default value for this attribute.  If values
	 * is set, the defaultValue must also be one of the set of values.
	 * defaultValue may be null.
	 * @param values an array of possible values for the attribute.  If
	 * this parameter is not null, then the attribute's value must be one
	 * of the listed set of values or an exception will be thrown.
	 * @param required if set, then this function will throw an exception
	 * if the attribute has no value and defaultValue is null.
	 * @return the Attribute's value as an Object.  The actual class of
	 * this object is determined by attrType.
	 */
	public Object getAttribute(String attrName, String attrType, 
							   String defaultValue, String[] values,
							   boolean required)
		throws XOMException
	{
		// Retrieve the attribute type class
		if(attrType.indexOf('.') == -1)
			attrType = "java.lang." + attrType;
		Class typeClass = null;
		try {
			typeClass = Class.forName(attrType);
		} catch (ClassNotFoundException ex) {
			throw new XOMException("Class could not be found for attribute "
									  + "type: " + attrType + ": "
									  + ex.getMessage());
		}		

		// Get a constructor from the type class which takes a String as
		// input.  If one does not exist, throw an exception.
		Class[] classArray = new Class[1];
		classArray[0] = java.lang.String.class;
		Constructor stringConstructor = null;
		try {
			stringConstructor = typeClass.getConstructor(classArray);
		} catch (NoSuchMethodException ex) {
			throw new XOMException("Attribute type class " +
									  attrType + " does not have a "
									  + "constructor which takes a String: "
									  + ex.getMessage());
		}

		// Get the Attribute of the given name
		Object attrVal = wrapper.getAttribute(attrName);
		if (attrVal == null) 
			attrVal = defaultValue;

		// Check for null
		if (attrVal == null) {
			if (required)
				throw new XOMException("Attribute " + attrName
									  + " is unset and has no default "
									  + " value.");
			else
				return null;
		}

		// Make sure it is on the list of acceptable values
		if (values != null) {
			if (!stringInArray(attrVal.toString(), values))
				throw new XOMException("Value " + attrVal.toString()
										  + " of attribute "
										  + attrName + " has illegal value "
										  + attrVal + ".  Legal values: "
										  + arrayToString(values));
		}

		// Invoke the constructor to get the final object
		Object[] args = new Object[1];
		args[0] = attrVal.toString();
		try {
			return stringConstructor.newInstance(args);
		} catch (InstantiationException ex) {
			throw new XOMException("Unable to construct a " + attrType
									  + " from value \"" + attrVal + "\": "
									  + ex.getMessage());
		} catch (InvocationTargetException ex) {
			throw new XOMException("Unable to construct a " + attrType
									  + " from value \"" + attrVal + "\": "
									  + ex.getMessage());
		} catch (IllegalAccessException ex) {
			throw new XOMException("Unable to construct a " + attrType
									  + " from value \"" + attrVal + "\": "
									  + ex.getMessage());
		}				
	}
	
}


// End ElementParser.java
