/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000, 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 26 December, 2000
*/

package mondrian.xom;
import java.util.*;
import java.io.*;

/**
 * <code>MetaGenerator</code> is a utility class which reads a Mining Meta
 * Model description in XML and generates the corresponding .dtd and .java
 * definition files.  MetaGenerator is invoked during the build process to help
 * generate files for the build.
 **/
public class MetaGenerator {

	/**
	 * Private member to hold the active model to be generated.
	 */
	private MetaDef.Model model;

	/**
	 * Private member.  This is model.prefix, except that it is "" if
	 * model.prefix is null, rather than null.
	 */
	private String prefix;

	private Hashtable keywordMap;
	private Hashtable typeMap;
	private Hashtable infoMap;
	private Hashtable subclassMap;
	private Vector allTypes;
	private boolean testMode;

	private static final String newLine = System.getProperty("line.separator");
	private static final char fileSep = System.getProperty("file.separator").charAt(0);

	/**
	 * This helper class contains all necessary information about a type.
	 * The information is collected here to help support inheritence and
	 * other advanced features.
	 */
	private class TypeInfo
	{
		// XML definition of the type.  Includes defined attributes,
		// content, and other information.
		public MetaDef.Definition def;

		// Documentation and code, here for easy reference.
		public String doc;
		public String code;

		// Name of the class and associated XML tag.
		public String name;
		public String className;
		public String tagName;

		// This array holds all attributes, inherited or otherwise, that may
		// be used by this type.
		public MetaDef.Attribute[] allAttributes;

		// This array holds all attributes that are overridden by this type.
		public MetaDef.Attribute[] ovrAttributes;

		// This array holds all new attributes defined only in this type
		// and not overriding any inherited attributes.
		public MetaDef.Attribute[] newAttributes;

		// This array holds all content, inherited or otherwise, that may
		// be used by this type.
		public MetaDef.Content[] allContent;

		// This array holds all new content defined only in this type.
		public MetaDef.Content[] newContent;

		// True if content is <Any> (either defined or inherited).
		public boolean isAny;

		// True if content is <CData> (either defined or inherited).
		public boolean isCData;

		// Reference to superclass info (if any)
		public TypeInfo superInfo;

		// Class to use when importing elements.
		public Class impClass;
		public String impName; // e.g. "foo.MetaDef.Tag"

		public String contentModel;

		public TypeInfo(MetaDef.Definition elt)
			throws XOMException
		{
			def = elt;

			// Get the name and superclass name
			name = null;
			String superName = null;
			MetaDef.Attribute[] attributes = null;
			MetaDef.Content[] content = null;
			contentModel = "sequential";
			if(elt instanceof MetaDef.Element) {
				MetaDef.Element element = (MetaDef.Element)elt;
				name = element.type;
				if(element.dtdName != null)
					tagName = element.dtdName;
				else
					tagName = prefix + name;
				superName = element._class;
				attributes = element.attributes;
				content = element.content;
				contentModel = element.contentModel;
				doc = element.doc;
				code = element.code;
				impClass = null;
				impName = null;
			}
			else if(elt instanceof MetaDef.Plugin) {
				name = ((MetaDef.Plugin)elt).type;
				tagName = prefix + name;
				superName = ((MetaDef.Plugin)elt)._class;
				attributes = ((MetaDef.Plugin)elt).attributes;
				content = new MetaDef.Content[0];
				doc = ((MetaDef.Plugin)elt).doc;
				code = ((MetaDef.Plugin)elt).code;
				impClass = null;
				impName = null;
			}
			else if(elt instanceof MetaDef.Class) {
				name = ((MetaDef.Class)elt)._class;
				tagName = "(%" + name + ";)";
				superName = ((MetaDef.Class)elt).superclass;
				attributes = ((MetaDef.Class)elt).attributes;
				content = ((MetaDef.Class)elt).content;
				doc = ((MetaDef.Class)elt).doc;
				code = ((MetaDef.Class)elt).code;
				impClass = null;
				impName = null;
			}
			else if(elt instanceof MetaDef.StringElement) {
				name = ((MetaDef.StringElement)elt).type;
				tagName = prefix + name;
				superName = null;
				attributes = new MetaDef.Attribute[0];
				content = new MetaDef.Content[0];
				doc = ((MetaDef.StringElement)elt).doc;
				code = null;
				impClass = null;
				impName = null;
			}
			else if (elt instanceof MetaDef.Import) {
				MetaDef.Import imp = (MetaDef.Import)elt;
				name = imp.type;
				if(imp.dtdName != null)
					tagName = imp.dtdName;
				else
					tagName = prefix + name;
				superName = null;
				attributes = new MetaDef.Attribute[0];
				content = new MetaDef.Content[0];
				doc = null;
				code = null;
				try {
					impName = imp.defPackage + "." + imp.defClass + "." + name;
					impClass = Class.forName(imp.defPackage + "."
											 + imp.defClass + "$"
											 + name);
				} catch(ClassNotFoundException ex) {
//  					throw new XOMException(
//  						"Import " + name + " references Java Class "
//  						+ imp.defPackage + "." + imp.defClass
//  						+ "." + name + " that does not exist.");
				}
			} else {
				throw new XOMException("Illegal element type "
									   + elt.getClass().getName());
			}
			className = XOMUtil.capitalize(name);

			// Get the TypeInfo record for the superclass.  If we don't find
			// it, we'll have to create it by looking up its definition.
			superInfo = null;
			if(superName != null) {
				superInfo = (TypeInfo)(infoMap.get(superName));
				if(superInfo == null) {
					MetaDef.Definition superDef =
						(MetaDef.Definition)(infoMap.get(superName));
					if(superDef == null)
						throw new XOMException(
							"Parent class " + superName + " of element "
							+ name + " was never defined.");
					superInfo = new TypeInfo(superDef);
				}
			}

			// Check for special content (<Any> or <CData>).  If we find it,
			// it must be the only content defined.
			boolean newAny = false;
			boolean newCData = false;
			if(content.length == 1) {
				if(content[0] instanceof MetaDef.CData)
					newCData = true;
				else if(content[0] instanceof MetaDef.Any)
					newAny = true;
			}

			// Make sure that <Any> or <CData> occurs only by itself.
			if(!newAny && !newCData) {
				for(int i=0; i<content.length; i++) {
					if(content[i] instanceof MetaDef.CData ||
					   content[i] instanceof MetaDef.Any)
						throw new XOMException(
							"Type " + name + " defines <Any> or <CData> "
							+ "content as well as other content.");
				}
			}

			// Do we have a superclass/supertype?
			if(superInfo == null) {
				// No supertype, so consider this type by itself.
				allAttributes = attributes;
				ovrAttributes = new MetaDef.Attribute[0];
				newAttributes = allAttributes;

				if(newAny || newCData) {
					isAny = newAny;
					isCData = newCData;
					allContent = new MetaDef.Content[0];
				}
				else {
					isAny = isCData = false;
					allContent = content;
				}
				newContent = allContent;
			}
			else {
				// Reconcile attributes.
				Hashtable attrHash = new Hashtable();
				Hashtable ovrHash = new Hashtable();
				Vector allAttrs = new Vector();
				Vector ovrAttrs = new Vector();
				Vector newAttrs = new Vector();

				for(int i=0; i<superInfo.allAttributes.length; i++) {
					attrHash.put(superInfo.allAttributes[i].name,
								 superInfo.allAttributes[i]);
				}
				for(int i=0; i<attributes.length; i++) {
					// Does the attribute already exist?
					MetaDef.Attribute inhAttr =
						(MetaDef.Attribute)(attrHash.get(attributes[i].name));
					if(inhAttr == null) {
						// attribute doesn't exist, so add to all and new.
						allAttrs.addElement(attributes[i]);
						newAttrs.addElement(attributes[i]);
					}
					else {
						// attribute does exist.  Type must match exactly.
						if(!(attributes[i].type.equals(inhAttr.type)))
							throw new XOMException(
								"Element " + name + " inherits attribute "
								+ inhAttr.name + " of type " + inhAttr.type
								+ " but redefines it to be of type "
								+ attributes[i].type);

						// Add to overridden vector and overridden hashtable
						ovrAttrs.addElement(attributes[i]);
						ovrHash.put(attributes[i].name,
									attributes[i]);
					}
				}

				// Add all non-overridden attributes to the allAttributes vector
				for(int i=0; i<superInfo.allAttributes.length; i++) {
					if(ovrHash.get(superInfo.allAttributes[i].name) == null)
						allAttrs.addElement(superInfo.allAttributes[i]);
				}

				// Add all overridden attributes to the allAttributes vector
				for(int i=0; i<ovrAttrs.size(); i++)
					allAttrs.addElement(ovrAttrs.elementAt(i));

				allAttributes = new MetaDef.Attribute[allAttrs.size()];
				for(int i=0; i<allAttributes.length; i++)
					allAttributes[i] =
						(MetaDef.Attribute)(allAttrs.elementAt(i));
				ovrAttributes = new MetaDef.Attribute[ovrAttrs.size()];
				for(int i=0; i<ovrAttributes.length; i++)
					ovrAttributes[i] =
						(MetaDef.Attribute)(ovrAttrs.elementAt(i));
				newAttributes = new MetaDef.Attribute[newAttrs.size()];
				for(int i=0; i<newAttributes.length; i++)
					newAttributes[i] =
						(MetaDef.Attribute)(newAttrs.elementAt(i));

				// Reconcile content.  First check for specials.
				if(newAny || newCData) {
					if(superInfo.isAny || superInfo.isCData)
						throw new XOMException(
							"Element " + name + " both defines and inherits "
							+ "<CData> or <Any> content.");
					if(superInfo.allContent.length > 0)
						throw new XOMException(
							"Element " + name + " inherits standard content "
							+ "but defines <CData> or <Any> content.");
					isAny = newAny;
					isCData = newCData;
					allContent = new MetaDef.Content[0];
					newContent = new MetaDef.Content[0];
				}
				else if (superInfo.isAny || superInfo.isCData) {
					if(content.length > 0)
						throw new XOMException(
							"Element " + name + " inherits <CData> or <Any> "
							+ "content but defines standard content.");
					isAny = superInfo.isAny;
					isCData = superInfo.isCData;
					allContent = new MetaDef.Content[0];
					newContent = new MetaDef.Content[0];
				}
				else {
					isAny = isCData = false;

					// Overriding of content is forbidden.
					Hashtable contentHash = new Hashtable();
					Vector allContentVec = new Vector();
					for(int i=0; i<superInfo.allContent.length; i++) {
						contentHash.put(getContentName(superInfo.allContent[i]),
										superInfo.allContent[i]);
						allContentVec.addElement(superInfo.allContent[i]);
					}
					for(int i=0; i<content.length; i++) {
						MetaDef.Content inhContent =
							(MetaDef.Content)
							(contentHash.get(getContentName(content[i])));
						if(inhContent != null)
							throw new XOMException(
								"Content named " + getContentName(content[i])
								+ " defined in element " + name + " was "
								+ "already defined in an inherited element.");
						allContentVec.addElement(content[i]);
					}
					allContent = new MetaDef.Content[allContentVec.size()];
					for(int i=0; i<allContent.length; i++)
						allContent[i] =
							(MetaDef.Content)(allContentVec.elementAt(i));
					newContent = content;
				}
			}

			// Add ourself to the hashtable if we're not already there
			if(infoMap.get(name) == null)
				infoMap.put(name, this);
		}

		public void writeJavaClass(PrintWriter out)
			throws XOMException
		{
			// Documentation first
			if(doc != null)
				writeJavaDoc(out, 1, doc);

			// Then create the inner class.
			String abs = (def instanceof MetaDef.Class) ? "abstract " :
				"";
			out.print("\tpublic static " + abs + "class " + className + " extends ");
			if(superInfo == null)
				out.println("mondrian.xom.ElementDef");
			else
				out.println(superInfo.className);
			if (isAny) {
				out.println("\t\timplements mondrian.xom.Any");
			}
			out.println("\t{");

			// Default constructor
			out.println("\t\tpublic " + className + "()");
			out.println("\t\t{");
			out.println("\t\t}");
			out.println();

			// mondrian.xom.DOMWrapper Constructor
			out.println("\t\tpublic " + className
						+ "(mondrian.xom.DOMWrapper _def)");
			out.println("\t\t\tthrows mondrian.xom.XOMException");
			out.println("\t\t{");

			// Body of constructor.  Special case for completely empty
			// model (no content and no attributes) to avoid warnings
			// about unused things.
			boolean mixed = contentModel.equals("mixed");
			if(allContent.length == 0 && allAttributes.length == 0 &&
			   !isAny && !isCData &&
				!(def instanceof MetaDef.Plugin))
				;  // constructor has no body
			else {
				if (def instanceof MetaDef.Element &&
					((MetaDef.Element) def).keepDef.booleanValue()) {
					out.println("\t\t\tthis._def = _def;");
				}

				out.println("\t\t\ttry {");

				// Plugins: read defPackage and defClass here.
				if(def instanceof MetaDef.Plugin) {
					out.println("\t\t\t\tdefPackage = "
								+ "mondrian.xom.DOMElementParser."
								+ "requiredDefAttribute("
								+ "_def, \"defPackage\", \"mondrian.xom\");");
					out.println("\t\t\t\tdefClass = mondrian.xom.DOMElementParser."
								+ "requiredDefAttribute("
								+ "_def, \"defClass\", null);");

					// Get the enclosure class we'll be using
					out.println("\t\t\t\tClass _pluginClass = "
								+ "mondrian.xom.DOMElementParser.getPluginClass("
								+ "defPackage, defClass);");
				}

				// Create the parser.  If using a Plugin, parse from a
				// different enclosure class.
				out.print("\t\t\t\tmondrian.xom.DOMElementParser _parser "
						  + "= new mondrian.xom.DOMElementParser("
						  + "_def, ");
				if(def instanceof MetaDef.Plugin)
					out.println("\"\", _pluginClass);");
				else {
					if(model.prefix == null)
						out.print("\"\", ");
					else
						out.print("\"" + model.prefix + "\", ");
					out.println(model.className + ".class);");
				}

				// This line is emitted to avoid unused warnings.
				out.println("\t\t\t\t_parser = _parser;");

				// Define a temp array if any Array elements are used
				if(hasContentType(allContent, MetaDef.Array.class)) {
					out.println("\t\t\t\tmondrian.xom.NodeDef[] "
								+ "_tempArray = null;");
					out.println("\t\t\t\t_tempArray = _tempArray;");
				}

				// Generate statements to read in all attributes.
				for(int i=0; i<allAttributes.length; i++)
					writeJavaGetAttribute(out, allAttributes[i]);

				// Generate statements to read in all content.
				if(def instanceof MetaDef.Plugin)
					writeJavaGetPluginContent(out, mixed);
				else if(isAny)
					writeJavaGetAnyContent(out, mixed);
				else if(isCData)
					writeJavaGetCDataContent(out);
				else
					for(int i=0; i<allContent.length; i++)
						writeJavaGetContent(out, allContent[i]);

				out.println("\t\t\t} catch(mondrian.xom.XOMException _ex) {");
				out.println("\t\t\t\tthrow new mondrian.xom.XOMException("
							+ "\"In \" + getName() + \": \" + _ex.getMessage());");
				out.println("\t\t\t}");
			}

			// Finish the constructor
			out.println("\t\t}");
			out.println();

			// Declare all new attributes
			for(int i=0; i<newAttributes.length; i++)
				writeJavaDeclareAttribute(out, newAttributes[i]);
			if(def instanceof MetaDef.Plugin)
				writeJavaDeclarePluginAttributes(out);
			if (def instanceof MetaDef.Element &&
				((MetaDef.Element) def).keepDef.booleanValue()) {
				out.println("\t\tpublic mondrian.xom.DOMWrapper _def;");
			}
			out.println();

			// Declare all new content
			if(def instanceof MetaDef.Plugin)
				writeJavaDeclarePluginContent(out, mixed);
			else if(isAny)
				writeJavaDeclareAnyContent(out, mixed);
			else if(isCData)
				writeJavaDeclareCDataContent(out);
			else
				for(int i=0; i<newContent.length; i++)
					writeJavaDeclareContent(out, newContent[i]);
			out.println();

			// Create the getName() function
			out.println("\t\tpublic String getName()");
			out.println("\t\t{");
			out.println("\t\t\treturn \"" + className + "\";");
			out.println("\t\t}");
			out.println();

			// Create the display() function
			out.println("\t\tpublic void display(java.io.PrintWriter _out, "
						+ "int _indent)");
			out.println("\t\t{");
			if(def instanceof MetaDef.Class && !isAny && !isCData &&
			   allContent.length == 0 && allAttributes.length == 0)
				;
			else
				out.println("\t\t\t_out.println(getName());");
			for(int i=0; i<allAttributes.length; i++)
				writeJavaDisplayAttribute(out, allAttributes[i]);
			if(def instanceof MetaDef.Plugin)
				writeJavaDisplayPluginAttributes(out);

			if(def instanceof MetaDef.Plugin)
				writeJavaDisplayPluginContent(out);
			else if(isAny)
				writeJavaDisplayAnyContent(out);
			else if(isCData)
				writeJavaDisplayCDataContent(out);
			else
				for(int i=0; i<allContent.length; i++)
					writeJavaDisplayContent(out, allContent[i]);
			out.println("\t\t}");

			// Create the displayXML() function
			out.println("\t\tpublic void displayXML("
						+ "mondrian.xom.XMLOutput _out, "
				        + "int _indent)");
			out.println("\t\t{");
			out.println("\t\t\t_out.beginTag(\""
						+ tagName + "\", "
						+ "new mondrian.xom.XMLAttrVector()");
			for(int i=0; i<allAttributes.length; i++)
				writeJavaDisplayXMLAttribute(out, allAttributes[i]);
			if(def instanceof MetaDef.Plugin)
				writeJavaDisplayXMLPluginAttributes(out);
			out.println("\t\t\t\t);");

			if(def instanceof MetaDef.Plugin)
				writeJavaDisplayXMLPluginContent(out);
			else if(isAny)
				writeJavaDisplayXMLAnyContent(out);
			else if(isCData)
				writeJavaDisplayXMLCDataContent(out);
			else
				for(int i=0; i<allContent.length; i++)
					writeJavaDisplayXMLContent(out, allContent[i]);

			out.println("\t\t\t_out.endTag(\"" + tagName + "\");");
			out.println("\t\t}");

			// Create the displayDiff() function
			out.println("\t\tpublic boolean displayDiff("
						+ "mondrian.xom.ElementDef _other, "
						+ "java.io.PrintWriter _out, "
						+ "int _indent)");
			out.println("\t\t{");
			out.println("\t\t\tboolean _diff = true;");
			if(allAttributes.length > 0 ||
			   allContent.length > 0 || isAny || isCData ||
			   def instanceof MetaDef.Plugin)
				out.println("\t\t\t" + className + " _cother = ("
							+ className + ")_other;");
			for(int i=0; i<newAttributes.length; i++)
				writeJavaDisplayDiffAttribute(out, allAttributes[i]);
			if(def instanceof MetaDef.Plugin)
				writeJavaDisplayDiffPluginAttributes(out);

			if(def instanceof MetaDef.Plugin)
				writeJavaDisplayDiffPluginContent(out);
			else if(isAny)
				writeJavaDisplayDiffAnyContent(out);
			else if(isCData)
				writeJavaDisplayDiffCDataContent(out);
			else
				for(int i=0; i<allContent.length; i++)
					writeJavaDisplayDiffContent(out, allContent[i]);
			out.println("\t\t\treturn _diff;");
			out.println("\t\t}");

			// Add the code section, if defined
			if(code != null)
				writeJavaCode(out, 2, code);

			// Complete the class definition and finish with a blank.
			out.println("\t}");
			out.println();
		}
	}

	/**
	 * Get the name of any piece of content of any type.
	 * @return the name of the piece of content.
	 * @throws XOMException if the content is <Any> or <CData>.
	 */
	private static String getContentName(MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			return ((MetaDef.Object)content).name;
		}
		else if (content instanceof MetaDef.Array) {
			return ((MetaDef.Array)content).name;
		}
		else {
			throw new XOMException(
				"Content of type " + content.getClass().getName()
				+ " does not have a name.");
		}
	}

	/**
	 * Return the TypeInfo class associated with the given name.
	 *
	 * @post fail == false || return != null
	 * @exception XOMException if the type has not been defined
	 */
	public TypeInfo getTypeInfo(String name, boolean fail)
		throws XOMException
	{
		TypeInfo info = (TypeInfo) infoMap.get(name);
		if (info == null && fail == true) {
			throw new XOMException(
				"Type " + name + " does not exist.");
		}
		return info;
	}

	/**
	 * Construct a MetaGenerator from an XML file.  The XML should meet the
	 * specifications of the Mining Meta Model.
	 * @param xmlFile a filename for the xml description of the model to be
	 * processed.
	 */
	public MetaGenerator(String xmlFile, boolean testMode)
			throws XOMException, IOException {
		this(xmlFile, testMode, null);
	}

	protected MetaGenerator(String xmlFile, boolean testMode, String className)
			throws XOMException, IOException {
		this.testMode = testMode;
		// Create a non-validating XML parser to parse the file
		FileInputStream in = new FileInputStream(xmlFile);
		Parser parser = XOMUtil.createDefaultParser();
		try {
			DOMWrapper def = parser.parse(in);
			model = new MetaDef.Model(def);
		} catch (XOMException ex) {
			throw new XOMException(ex, "Failed to parse XML file: " + xmlFile);
		}

		// check that class names are consistent
		if (className != null) {
			if (model.className == null) {
				model.className = className;
			} else {
				String modelClassName = model.className;
				if (model.packageName != null &&
					!model.packageName.equals("")) {
					modelClassName = model.packageName + "." +
						model.className;
				}
				if (!className.equals(modelClassName)) {
					throw new XOMException(
						"className parameter (" + className +
						") is inconsistent with model's packageName and " +
						"className attributes (" + modelClassName + ")");
				}
			}
		}

		// Construct the meta model from its XML description
		prefix = model.prefix;
		if(prefix == null)
			prefix = "";

		// Setup the Hashtable maps
		initKeywordMap();
		initTypeMap();
		initSubclassMap();
	}

	/**
	 * Initialize the keyword map.  This class maps all Java keywords to safe
	 * versions (prepended with an underscore) which may be used for generated
	 * names.  Java keywords are listed in the java spec at
	 * <a href="http://java.sun.com/docs/books/jls/html/3.doc.html#229308">
	 * http://java.sun.com/docs/books/jls/html/3.doc.html#229308</a>
	 */
	private void initKeywordMap()
	{
		keywordMap = new Hashtable();
		keywordMap.put("abstract", "_abstract");
		keywordMap.put("boolean", "_boolean");
		keywordMap.put("break", "_break");
		keywordMap.put("byte", "_byte");
		keywordMap.put("case", "_case");
		keywordMap.put("catch", "_catch");
		keywordMap.put("char", "_char");
		keywordMap.put("class", "_class");
		keywordMap.put("const", "_const");
		keywordMap.put("continue", "_continue");
		keywordMap.put("default", "_default");
		keywordMap.put("do", "_do");
		keywordMap.put("double", "_double");
		keywordMap.put("else", "_else");
		keywordMap.put("extends", "_extends");
		keywordMap.put("final", "_final");
		keywordMap.put("finally", "_finally");
		keywordMap.put("float", "_float");
		keywordMap.put("for", "_for");
		keywordMap.put("if", "_if");
		keywordMap.put("implements", "_implements");
		keywordMap.put("import", "_import");
		keywordMap.put("instanceof", "_instanceof");
		keywordMap.put("int", "_int");
		keywordMap.put("interface", "_interface");
		keywordMap.put("long", "_long");
		keywordMap.put("native", "_native");
		keywordMap.put("new", "_new");
		keywordMap.put("goto", "_goto");
		keywordMap.put("package", "_package");
		keywordMap.put("private", "_private");
		keywordMap.put("protected", "_protected");
		keywordMap.put("public", "_public");
		keywordMap.put("return", "_return");
		keywordMap.put("short", "_short");
		keywordMap.put("static", "_static");
		keywordMap.put("super", "_super");
		keywordMap.put("switch", "_switch");
		keywordMap.put("synchronized", "_synchronized");
		keywordMap.put("this", "_this");
		keywordMap.put("throw", "_throw");
		keywordMap.put("throws", "_throws");
		keywordMap.put("transient", "_transient");
		keywordMap.put("try", "_try");
		keywordMap.put("void", "_void");
		keywordMap.put("volatile", "_volatile");
		keywordMap.put("while", "_while");
		keywordMap.put("true", "_true");
		keywordMap.put("false", "_false");
		keywordMap.put("null", "_null");
	}

	/**
	 * All Elements in the meta model have an associated type name which
	 * identifies the element.  The type map allows the XMLDef.ElementType
	 * object describing an element to be retrieved from its name.  It is
	 * used to resolve references to element type names appearing
	 * throughout a model.
	 */
	private void initTypeMap()
		throws XOMException
	{
		typeMap = new Hashtable();
		allTypes = new Vector();
		for(int i=0; i<model.elements.length; i++) {
			MetaDef.Definition elt = model.elements[i];
			String name = null;
			if(elt instanceof MetaDef.Element)
				name = ((MetaDef.Element)elt).type;
			else if(elt instanceof MetaDef.Plugin)
				name = ((MetaDef.Plugin)elt).type;
			else if(elt instanceof MetaDef.Class)
				name = ((MetaDef.Class)elt)._class;
			else if(elt instanceof MetaDef.StringElement)
				name = ((MetaDef.StringElement)elt).type;
			else if(elt instanceof MetaDef.Import)
				name = ((MetaDef.Import)elt).type;
			else
				throw new XOMException("Illegal element type "
										  + elt.getClass().getName());
			typeMap.put(name, elt);
			allTypes.addElement(name);
		}

		infoMap = new Hashtable();
		for(int i=0; i<model.elements.length; i++) {
			// Get the element
			MetaDef.Definition elt = model.elements[i];

			// Construct the new TypeInfo object and add to the hashtable
			TypeInfo info = new TypeInfo(elt);
			infoMap.put(info.name, info);
		}
	}

	/**
	 * In a few cases, a complete list of all subclasses of a class
	 * object is required.  The subclass map maps each class object
	 * (identified by its name) to a Vector containing all of its
	 * subclasses.  Currently, all subclasses must be Element types.
	 */
	private void initSubclassMap()
		throws XOMException
	{
		subclassMap = new Hashtable();

		// First, iterate through all Class elements in the model,
		// initializing a location in the hashtable for each.
		for(int i=0; i<model.elements.length; i++) {
			MetaDef.Definition elt = model.elements[i];
			if(elt instanceof MetaDef.Class) {
				MetaDef.Class _class = (MetaDef.Class)elt;
				subclassMap.put(_class._class, new Vector());
			}
		}

		// Now, iterate through all Element elements in the model.
		// For each one, go through all of its superclasses and add itself to
		// the vector of each.
		// If a class is not found, it is an error.
		for(int i=0; i<model.elements.length; i++) {
			MetaDef.Definition elt = model.elements[i];
			if(elt instanceof MetaDef.Element) {
				MetaDef.Element elem = (MetaDef.Element)elt;
				TypeInfo info = getTypeInfo(elem.type, true);
				addToSubclassMap(elem, info);
			}
		}
	}

	/**
	 * Helper method for initSubclassMap:
	 * Add this element to the subclass map for each superclass of info.
	 */
	private void addToSubclassMap(MetaDef.Element elem, TypeInfo info)
		throws XOMException
	{
		while(info.superInfo != null) {
			// Add the element to this class's vector.
			Vector vec = (Vector)(subclassMap.get(info.superInfo.name));
			if(vec == null)
				throw new XOMException("Class " + info.superInfo.name +
										  " of element " + elem.type
										  + " is not defined.");
			vec.addElement(elem);

			// Add to all superclasses as well
			info = info.superInfo;
		}
	}

	/**
	 * Create all files associated with the metamodel, including a Java class
	 * and a DTD file.  The DTD is primarily for reference--it will not work
	 * if any advanced features (plugins, includes) are used.
	 * @param outputDir the output directory in which to generate the files.
	 */
	public void writeFiles(String outputDirName, String dtdFileName)
		throws XOMException, IOException
	{
		// Compute the output file names
		if (dtdFileName != null) {
			if (model.dtdName == null) {
				model.dtdName = dtdFileName;
			} else {
				if (!dtdFileName.equals(model.dtdName)) {
					throw new XOMException(
						"dtdFileName parameter (" + dtdFileName +
						") is inconsistent with model's dtdName " +
						"attribute (" + model.dtdName + ")");
				}
			}
		}
		File javaOutputDir = new File(outputDirName);

		if (!testMode &&
			model.packageName != null &&
			!model.packageName.equals("")) {
			javaOutputDir = new File(
					javaOutputDir, model.packageName.replace('.',fileSep));
		}
		File javaFile = new File(javaOutputDir, model.className + ".java");
		File outputDir = javaFile.getParentFile();
		File dtdFile = new File(outputDir, model.dtdName);

		// If the output file is MetaDef.java, and we start writing to
		// MetaDef.java before we have loaded MetaDef.class, the system thinks
		// that the class is out of date.  So load MetaDef.class before that
		// point.
		XOMUtil.discard(new MetaDef());

		// Create directories if necessary.
		outputDir.mkdir();

		// Open the files for writing
		FileWriter dtdWriter = new FileWriter(dtdFile);
		PrintWriter dtdOut = new PrintWriter(dtdWriter);
		FileWriter javaWriter = new FileWriter(javaFile);
		PrintWriter javaOut = new PrintWriter(javaWriter);

		if(!testMode)
			System.out.println("Writing " + dtdFile);
		writeDtd(dtdOut);
		dtdOut.flush();
		dtdWriter.close();

		if(!testMode)
			System.out.println("Writing " + javaFile);
		writeJava(javaOut);
		javaOut.flush();
		javaWriter.close();

		if(!testMode)
			System.out.println("Done");
	}

	public void writeDtd(PrintWriter out)
		throws XOMException
	{
		// Write header information for the dtd
		out.println("<!--");
		out.println("     This dtd file was automatically generated from "
			      + "mining model " + model.name + ".");
		out.println("     Do not edit this file by hand.");
		out.println("  -->");
		out.println();

		// Write toplevel documentation here
		writeDtdDoc(out, model.doc);

		// For each CLASS definition, write an entity definition.  These must
		// be done before regular elements because entities must be defined
		// before use.
		for(int i=0; i<model.elements.length; i++) {
			if(model.elements[i] instanceof MetaDef.Class) {
				writeDtdEntity(out, (MetaDef.Class)(model.elements[i]));
			}
		}

		// Write each element in turn
		for(int i=0; i<model.elements.length; i++)
			writeDtdElement(out, model.elements[i]);
	}

	public void writeJava(PrintWriter out)
		throws XOMException
	{
		// Write header information for the java file
		out.println("/" + "*");
		out.println("/" + "/ This java file was automatically generated");
		out.println("/" + "/ from mining model '" + model.name + "'");
		if(!testMode)
			out.println("/" + "/ on " + new Date().toString());
		out.println("/" + "/ Do not edit this file by hand.");
		out.println("*" + "/");
		out.println();

		if (!testMode &&
			!(model.packageName == null || model.packageName.equals(""))) {
			out.println("package " + model.packageName + ";");
		}

		// Write the toplevel documentation for the package.  This becomes
		// the toplevel documentation for the class and is also placed at
		// the top of the Dtd.
		String extraDoc = newLine + "<p>This class was generated from mining model '"
			+ model.name + "' on " + new Date().toString();
		if(testMode)
			extraDoc = "";

		writeJavaDoc(out, 0, model.doc + extraDoc);

		// Begin the class.  Include a getXMLDefClass() function which
		// simply returns this class.
		out.println("public class " + model.className + " {");
		out.println();
		out.println("\tpublic static java.lang.Class getXMLDefClass()");
		out.println("\t{");
		out.println("\t\treturn " + model.className + ".class;");
		out.println("\t}");
		out.println();

		// Create a static member that names all Elements that may be
		// used within this class.
		out.println("\tpublic static String[] _elements = {");
		for(int i=0; i<allTypes.size(); i++) {
			String type = (String) allTypes.elementAt(i);
			out.print("\t\t\"" + type + "\"");
			if(i<allTypes.size()-1)
				out.println(",");
			else
				out.println();
		}
		out.println("\t};");
		out.println();

		// Create an inner class for each Class/Object definition.
		for(int i=0; i<model.elements.length; i++)
			writeJavaElement(out, model.elements[i]);

		// End the class
		out.println();
		out.println("}");
	}

	/**
	 * Writes an entity definition based on a defined Class.  Because entity
	 * definitions must appear before use in a DTD, this function must be
	 * called for each defined class before processing the rest of the model.
	 * @param out PrintWriter to write the DTD.
	 * @param class Class definition on which the Entity will be based.
	 */
	private void writeDtdEntity(PrintWriter out, MetaDef.Class _class)
		throws XOMException
	{
		// Documentation first
		if(_class.doc != null)
			writeDtdDoc(out, _class.doc);

		// Lookup the subclass vector for this class.  Use this to generate
		// the entity definition.
		Vector subclassVec = (Vector)(subclassMap.get(_class._class));
		out.print("<!ENTITY % " + _class._class + " \"");
		if(subclassVec == null)
			throw new AssertFailure(
				"Missing subclass vector for class " + _class._class);
		for(int i=0; i<subclassVec.size(); i++) {
			MetaDef.Element elem =
				(MetaDef.Element)(subclassVec.elementAt(i));

			// Print the dtd version of the element name
			if(elem.dtdName != null)
				out.print(elem.dtdName);
			else
				out.print(prefix + elem.type);

			if(i<subclassVec.size()-1)
				out.print("|");
		}
		out.println("\">");
		out.println();
	}

	private void writeDtdElement(PrintWriter out, MetaDef.Definition elt)
		throws XOMException
	{
		// What is written into the dtd depends on the class of elt.
		if(elt instanceof MetaDef.Element) {
			// Get the info class for this element.
			MetaDef.Element element = (MetaDef.Element)elt;
			TypeInfo info = getTypeInfo(element.type, false);
			if(info == null)
				throw new AssertFailure("Element type " + element.type + " is missing from the "
								 + "type map.");

			// Documentation first
			if(element.doc != null)
				writeDtdDoc(out, element.doc);

			// Then content model.  Special case empty models.
			out.print("<!ELEMENT " + info.tagName + " ");
			if(info.allContent.length == 0 && !info.isAny && !info.isCData)
				out.print("EMPTY");
			else {
				if(info.isAny)
					out.print("ANY");
				else if(info.isCData)
					out.print("(#PCDATA)");
				else {
					out.print("(");
					for(int i=0; i<info.allContent.length; i++) {
						writeDtdContent(out, info.allContent[i]);
						if(i<info.allContent.length-1)
							out.print(",");
					}
					out.print(")");
				}
			}
			out.println(">");

			// Finally, attribute list
			if(info.allAttributes.length > 0) {
				out.println("<!ATTLIST " + info.tagName);
				for(int i=0; i<info.allAttributes.length; i++)
					writeDtdAttribute(out, info.allAttributes[i]);
				out.println(">");
			}

			// Finish with a blank
			out.println();
		}
		else if (elt instanceof MetaDef.Class) {
			// Do nothing--entities are handled ahead of time.
		}
		else if (elt instanceof MetaDef.StringElement) {
			// Get the info class for this element.
			MetaDef.StringElement element = (MetaDef.StringElement)elt;
			TypeInfo info = (TypeInfo)(infoMap.get(element.type));
			if(info == null)
				throw new AssertFailure("StringElement type " + element.type +
								 " is missing from the "
								 + "type map.");

			// Documentation first
			if(element.doc != null)
				writeDtdDoc(out, element.doc);

			// Then content model.  It is always (#PCDATA).
			out.println("<!ELEMENT " + info.tagName + " (#PCDATA)>");
			out.println();
		}
		else if (elt instanceof MetaDef.Plugin) {
			// Get the info class for this element.
			MetaDef.Plugin plugin = (MetaDef.Plugin)elt;
			TypeInfo info = (TypeInfo)(infoMap.get(plugin.type));
			if(info == null)
				throw new AssertFailure("Plugin element " + plugin.type +
								 " is missing from the "
								 + "type map.");

			// Documentation first
			if(plugin.doc != null)
				writeDtdDoc(out, plugin.doc);

			// Then content model.  It is always ANY.
			out.println("<!ELEMENT " + info.tagName + " ANY>");

			// Finally, attribute list.  Don't allow use of plugin reserved
			// attributes defPackage and defClass.
			out.println("<!ATTLIST " + info.tagName);
			for(int i=0; i<info.allAttributes.length; i++) {
				if(info.allAttributes[i].name.equals("defPackage") ||
				   info.allAttributes[i].name.equals("defClass"))
					throw new XOMException(
						"The attribute \"" + info.allAttributes[i].name
						+ "\" is reserved and may not be redefined in "
						+ "or inherited by a Plugin.");
				writeDtdAttribute(out, info.allAttributes[i]);
			}

			// Add attribute definitions for defPackage and defClass
			out.println("defPackage CDATA \"mondrian.xom\"");
			out.println("defClass CDATA #REQUIRED");

			// Complete the attribute list
			out.println(">");
			out.println();
		}
		else if (elt instanceof MetaDef.Import) {
			// Get the info class for this element.
			MetaDef.Import imp = (MetaDef.Import)elt;
			TypeInfo info = getTypeInfo(imp.type, true);

			// Imports can't really be handled, so just generate a placeholder
			// ANY element for show.
			out.println("<!ELEMENT " + info.name + " ANY>");
			out.println();
		}
		else {
			throw new XOMException("Unrecognized element type definition: "
									  + elt.getClass().getName());
		}
	}

	private void writeDtdDoc(PrintWriter out, String doc)
	{
		out.println("<!--");

		// Process the String line-by-line.  Trim whitespace from each
		// line and ignore fully blank lines.
		try {
			LineNumberReader reader = new LineNumberReader(new StringReader(doc));
			String line;
			while((line = reader.readLine()) != null) {
				String trimLine = line.trim();
				if(!trimLine.equals("")) {
					out.print("     ");
					out.println(trimLine);
				}
			}
		}
		catch(IOException ex) {
			throw new AssertFailure(ex);
		}

		out.println("  -->");
	}

	private void writeJavaDoc(PrintWriter out, int indent, String doc)
	{
		for(int i=0; i<indent; i++)
			out.print("\t");
		out.println("/" + "**");

		// Process the String line-by-line.  Trim whitespace from each
		// line and ignore fully blank lines.
		try {
			LineNumberReader reader = new LineNumberReader(new StringReader(doc));
			String line;
			while((line = reader.readLine()) != null) {
				String trimLine = line.trim();
				if(!trimLine.equals("")) {
					for(int i=0; i<indent; i++)
						out.print("\t");
					out.print(" * ");
					out.println(trimLine);
				}
			}
		}
		catch(IOException ex) {
			throw new AssertFailure(ex);
		}

		for(int i=0; i<indent; i++)
			out.print("\t");
		out.println(" *" + "/");
	}

	private void writeJavaCode(PrintWriter out, int indent, String code)
	{
		for(int i=0; i<indent; i++)
			out.print("\t");
		out.println("/" + "/ BEGIN pass-through code block ---");

		// Process the String line-by-line.  Don't trim lines--just echo
		try {
			LineNumberReader reader = new LineNumberReader(new StringReader(code));
			String line;
			while((line = reader.readLine()) != null) {
				out.println(line);
			}
		}
		catch(IOException ex) {
			throw new AssertFailure(ex);
		}

		for(int i=0; i<indent; i++)
			out.print("\t");
		out.println("/" + "/ END pass-through code block ---");
	}

	private MetaDef.Definition getType(String name)
		throws XOMException
	{
		// The type mapping hash table maps element type names to their
		// MetaDef.Definition objects.  First, look up the element type associated
		// with the name.
		MetaDef.Definition type = (MetaDef.Definition) typeMap.get(name);
		if(type == null)
			throw new XOMException("Element type name " + name + " was never "
									  + "defined.");
		return type;
	}

	/**
	 * Deterimines if a name conflicts with a Java keyword.  If so, it returns
	 * an alternate form of the name (typically the same name with an
	 * underscore preprended).
	 * @param name a name to be used in a Java program.
	 * @return a safe form of the name; either the name itself or a modified
	 * version if the name is a keyword.
	 */
	private String getDeclaredName(String name)
		throws XOMException
	{
		String mappedName = (String)(keywordMap.get(name));
		if(mappedName == null)
			return name;
		else
			return mappedName;
	}

	private void writeDtdContent(PrintWriter out, MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			MetaDef.Object obj = (MetaDef.Object)content;
			TypeInfo info = (TypeInfo)(infoMap.get(obj.type));
			if(info == null)
				throw new XOMException(
					"Object " + obj.name + " has undefined type "
					+ obj.type);
			out.print(info.tagName);
			if(!obj.required.booleanValue())
				out.print("?");
		}
		else if (content instanceof MetaDef.Array) {
			MetaDef.Array array = (MetaDef.Array)content;
			TypeInfo info = (TypeInfo)(infoMap.get(array.type));
			if(info == null)
				throw new XOMException(
					"Array " + array.name + " has undefined type "
					+ array.type);
			out.print("(" + info.tagName + ")");
			if(array.min.intValue() > 0)
				out.print("+");
			else
				out.print("*");
		}
		else {
			throw new XOMException("Unrecognized content type definition: "
									  + content.getClass().getName());
		}
	}

	private void writeDtdAttribute(PrintWriter out, MetaDef.Attribute attr)
		throws XOMException
	{
		// Attribute name
		out.print(attr.name + " ");

		// Values, or CDATA if unspecified
		if(attr.values == null || attr.values.length == 0) {
			if (attr.type.equalsIgnoreCase("Boolean")) {
				out.print("(true|false) ");
			} else {
				out.print("CDATA ");
			}
		} else {
			out.print("(");
			for(int i=0; i<attr.values.length; i++) {
				out.print(attr.values[i]);
				if(i < attr.values.length-1)
					out.print("|");
			}
			out.print(") ");
		}

		// Default value
		if(attr._default == null) {
			if(attr.required.booleanValue())
				out.println("#REQUIRED");
			else
				out.println("#IMPLIED");
		}
		else {
			out.print("\"" + attr._default + "\"");
			out.println();
		}
	}

	/**
	 * This helper function returns true if any member of the given content
	 * array is of the specified type.
	 * @param content an array of content descriptors.
	 * @param match a Class describing the class to match.
	 * @return true if any member of the given content array matches
	 * the given match type.
	 */
	private static boolean hasContentType(MetaDef.Content[] content,
										  Class match)
	{
		for (int i=0; i<content.length; i++) {
			if (content[i].getClass() == match) {
				return true;
			}
		}
		return false;
	}

	private void writeJavaElement(PrintWriter out, MetaDef.Definition elt)
		throws XOMException
	{
		// What is written into the dtd depends on the class of elt.
		if(elt instanceof MetaDef.Element) {
			MetaDef.Element element = (MetaDef.Element)elt;
			TypeInfo info = (TypeInfo)(infoMap.get(element.type));
			if(info == null)
				throw new XOMException(
					"Element type " + element.type + " was never defined.");
			info.writeJavaClass(out);
		}
		else if (elt instanceof MetaDef.Plugin) {
			MetaDef.Plugin plugin = (MetaDef.Plugin)elt;
			TypeInfo info = (TypeInfo)(infoMap.get(plugin.type));
			if(info == null)
				throw new XOMException(
					"Plugin type " + plugin.type + " was never defined.");
			info.writeJavaClass(out);
		}
		else if (elt instanceof MetaDef.Class) {
			MetaDef.Class _class = (MetaDef.Class)elt;
			TypeInfo info = (TypeInfo)(infoMap.get(_class._class));
			if(info == null)
				throw new XOMException(
					"Class type " + _class._class + " was never defined.");
			info.writeJavaClass(out);
		}
		else if (elt instanceof MetaDef.StringElement) {
			// Documentation first
			MetaDef.StringElement element = (MetaDef.StringElement)elt;
			if(element.doc != null)
				writeJavaDoc(out, 1, element.doc);

			// Declare the name as a constant
			out.println("\tpublic static final String "
						+ element.type + " = \""
						+ element.type + "\";");
			out.println();
		}
		else if (elt instanceof MetaDef.Import) {
			// Do nothing--imports are handled inline
		}
		else {
			throw new XOMException("Unrecognized element type definition: "
									  + elt.getClass().getName());
		}
	}

	public void writeJavaGetAttribute(PrintWriter out,
									  MetaDef.Attribute attr)
		throws XOMException
	{
		// Setup an array for attribute values if required
		if(attr.values != null && attr.values.length > 0) {
			out.print("\t\t\t\tString[] _" + getDeclaredName(attr.name)
					  + "_values = {");
			for(int i=0; i<attr.values.length; i++) {
				out.print("\"" + attr.values[i] + "\"");
				if(i < attr.values.length-1)
					out.print(", ");
			}
			out.println("};");
		}

		out.print("\t\t\t\t" + getDeclaredName(attr.name) + " = ");
		out.print("(" + attr.type + ")_parser.getAttribute(");
		out.print("\"" + attr.name + "\", \"" + attr.type + "\", ");
		if(attr._default == null) {
			if (attr.values.length == 0 && attr.type.equals("String") &&
				attr.required.booleanValue() == true)
				out.print("\"\", ");
			else
				out.print("null, ");
		} else {
			out.print("\"" + attr._default + "\", ");
		}
		if(attr.values == null || attr.values.length == 0)
			out.print("null, ");
		else
			out.print("_" + getDeclaredName(attr.name)
					  + "_values, ");
		if(attr.required.booleanValue())
			out.print("true");
		else
			out.print("false");
		out.println(");");
	}

	public void writeJavaDeclareAttribute(PrintWriter out,
										  MetaDef.Attribute attr)
		throws XOMException
	{
		// Generate the declaration, including a quick comment
		out.print("\t\tpublic " + attr.type + " "
				  + getDeclaredName(attr.name) + ";  /" + "/ ");
		if(attr._default != null)
			out.print("attribute default: " + attr._default);
		else if(attr.required.booleanValue())
			out.print("required attribute");
		else
			out.print("optional attribute");
		out.println();
	}

	public void writeJavaDisplayAttribute(PrintWriter out,
										  MetaDef.Attribute attr)
		throws XOMException
	{
		// Generate the display line
		out.println("\t\t\tdisplayAttribute(_out, \"" + attr.name + "\", "
					+ getDeclaredName(attr.name) + ", _indent+1);");
	}

	public void writeJavaDisplayXMLAttribute(PrintWriter out,
											 MetaDef.Attribute attr)
		throws XOMException
	{
		out.println("\t\t\t\t.add(\"" + attr.name
					+ "\", " + getDeclaredName(attr.name) + ")");
	}

	public void writeJavaDisplayDiffAttribute(PrintWriter out,
											  MetaDef.Attribute attr)
		throws XOMException
	{
		out.println("\t\t\t_diff = _diff && displayAttributeDiff(\"" + attr.name
					+ "\", " + getDeclaredName(attr.name)
					+ ", _cother." + getDeclaredName(attr.name)
					+ ", _out, _indent+1);");
	}

	public void writeJavaGetContent(PrintWriter out,
									MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			// Get the object and its type
			MetaDef.Object obj = (MetaDef.Object)content;
			MetaDef.Definition type = getType(obj.type);
			TypeInfo info = getTypeInfo(obj.type, true);

			out.print("\t\t\t\t"
					  + getDeclaredName(obj.name) + " = ");

			// Behavior depends on the type
			if (type != null && type instanceof MetaDef.Import) {
				// Get the info object for the import
				info = getTypeInfo(((MetaDef.Import)type).type, true);

				// Call the class constructor directly.
				out.print("(" + info.impName + ")_parser.getElement(");
				out.print(info.impName + ".class, ");
			}
			else if (type != null && type instanceof MetaDef.StringElement) {
				out.print("_parser.getString(" + info.className + ", ");
			}
			else {
				out.print("(" + info.className + ")_parser.getElement(");
				out.print(info.className + ".class, ");
			}

			if(obj.required.booleanValue())
				out.print("true");
			else
				out.print("false");
			out.println(");");
		}
		else if (content instanceof MetaDef.Array) {
			// Get the object and its type
			MetaDef.Array array = (MetaDef.Array)content;
			MetaDef.Definition type = getType(array.type);
			String typeName = getTypeInfo(array.type, true).className;

			if (type instanceof MetaDef.Import) {
				// Get the info object for the import
				TypeInfo info = getTypeInfo(((MetaDef.Import)type).type, true);

				// Construct the array
				out.print("\t\t\t\t_tempArray = _parser.getArray(");
				out.print(info.impName + ".class, ");
				out.println(array.min + ", " + array.max + ");");
				out.println("\t\t\t\t"
							+ getDeclaredName(array.name)
							+ " = new " + info.impName + "[_tempArray.length];");
				out.println("\t\t\t\tfor(int _i=0; _i<"
							+ getDeclaredName(array.name)
							+ ".length; _i++)");
				out.println("\t\t\t\t\t" + getDeclaredName(array.name) + "[_i] = "
							+ "(" + typeName + ")_tempArray[_i];");
			}
			else if (type instanceof MetaDef.StringElement) {
				out.print("\t\t\t\t" + getDeclaredName(array.name)
						  + " = _parser.getStringArray(");
				out.println("\"" + typeName + "\", " + array.min
							+ ", " + array.max + ");");
			}
			else {
				out.print("\t\t\t\t_tempArray = _parser.getArray(");
				out.print(typeName + ".class, ");
				out.println(array.min + ", " + array.max + ");");
				out.println("\t\t\t\t"
							+ getDeclaredName(array.name)
							+ " = new " + typeName + "[_tempArray.length];");
				out.println("\t\t\t\tfor(int _i=0; _i<"
							+ getDeclaredName(array.name)
							+ ".length; _i++)");
				out.println("\t\t\t\t\t" + getDeclaredName(array.name) + "[_i] = "
							+ "(" + typeName + ")_tempArray[_i];");
			}
		}
		else {
			throw new XOMException("Unrecognized content type definition: "
									  + content.getClass().getName());
		}
	}

	public void writeJavaGetAnyContent(PrintWriter out, boolean mixed)
	{
		if (mixed) {
			out.println("\t\t\t\tchildren = getMixedChildren(" +
						"_def, " +
						model.className + ".class, " +
						"\"" + prefix + "\");");
		} else {
			out.println("\t\t\t\tchildren = getElementChildren(" +
						"_def, " +
						model.className + ".class, " +
						"\"" + prefix + "\");");
		}
	}

	public void writeJavaGetCDataContent(PrintWriter out)
	{
		out.println("\t\t\t\tcdata = _parser.getText();");
	}

	public void writeJavaDeclareContent(PrintWriter out,
										MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			// Write documentation (if any)
			MetaDef.Object obj = (MetaDef.Object)content;
			if(obj.doc != null)
				writeJavaDoc(out, 2, obj.doc);

			// Handle includes
			MetaDef.Definition type = getType(obj.type);
			String typeName = getTypeInfo(obj.type, true).className;

			// Write content declaration.
			if(type instanceof MetaDef.Import) {
				// Get the info object for the import
				TypeInfo info = getTypeInfo(((MetaDef.Import)type).type, true);
				typeName = info.impName;
				out.print("\t\tpublic " + typeName + " "
						  + getDeclaredName(obj.name) + ";  /" + "/");
			}
			else if(type instanceof MetaDef.StringElement)
				out.print("\t\tpublic String "
						  + getDeclaredName(obj.name) + ";  /" + "/");
			else
				out.print("\t\tpublic " + typeName + " "
						  + getDeclaredName(obj.name) + ";  /" + "/");

			// Write a brief comment.
			if(obj.required.booleanValue())
				out.println("required element");
			else
				out.println("optional element");
		}
		else if (content instanceof MetaDef.Array) {
			// Write documentation (if any)
			MetaDef.Array array = (MetaDef.Array)content;
			if(array.doc != null)
				writeJavaDoc(out, 2, array.doc);
			MetaDef.Definition type = getType(array.type);
			String typeName = getTypeInfo(array.type, true).className;

			// Write content declaration.
			if(type instanceof MetaDef.Import) {
				// Get the info object for the import
				TypeInfo info = getTypeInfo(((MetaDef.Import)type).type, true);
				typeName = info.impName;
				out.print("\t\tpublic " + typeName + "[] "
						  + getDeclaredName(array.name) + ";  /" + "/");
			}
			else if(type instanceof MetaDef.StringElement)
				out.print("\t\tpublic String[] "
						  + getDeclaredName(array.name) + ";  /" + "/");
			else
				out.print("\t\tpublic " + typeName + "[] "
						  + getDeclaredName(array.name) + ";  /" + "/");

			// Write a brief comment.
			if(array.min.intValue() <= 0 &&
			   array.max.intValue() <= 0)
				out.println("optional array");
			else {
				if(array.min.intValue() > 0)
					out.print("min " + array.min);
				if(array.max.intValue() > 0)
					out.print("max " + array.max);
				out.println();
			}
		}
		else {
			throw new XOMException("Unrecognized content type definition: "
									  + content.getClass().getName());
		}
	}

	public void writeJavaDeclareAnyContent(PrintWriter out, boolean mixed)
	{
		out.println("\t\tpublic mondrian.xom." +
					(mixed ? "NodeDef" : "ElementDef") +
					"[] children;  /" + "/holder for variable-type children");
		out.println("\t\t// implement Any");
		out.println("\t\tpublic mondrian.xom.NodeDef[] getChildren()");
		out.println("\t\t{");
		out.println("\t\t\treturn children;");
		out.println("\t\t}");
		out.println("\t\t// implement Any");
		out.println("\t\tpublic void setChildren(mondrian.xom.NodeDef[] children)");
		out.println("\t\t{");
		out.println("\t\t\tthis.children = " +
					(mixed ? "" : "(mondrian.xom.ElementDef[]) ") +
					"children;");
		out.println("\t\t}");
	}

	public void writeJavaDeclareCDataContent(PrintWriter out)
	{
		out.print("\t\tpublic String cdata;  /"
				  + "/ All text goes here");
	}

	public void writeJavaDisplayContent(PrintWriter out,
										MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			MetaDef.Object obj = (MetaDef.Object)content;
			MetaDef.Definition type = getType(obj.type);

			if(type instanceof MetaDef.StringElement)
				out.println("\t\t\tdisplayString(_out, \""
							+ obj.name + "\", " + getDeclaredName(obj.name)
							+ ", _indent+1);");
			else
				out.println("\t\t\tdisplayElement(_out, \""
							+ obj.name + "\", " + getDeclaredName(obj.name)
							+ ", _indent+1);");
		}
		else if (content instanceof MetaDef.Array) {
			MetaDef.Array array = (MetaDef.Array)content;
			MetaDef.Definition type = getType(array.type);

			if(type instanceof MetaDef.StringElement)
				out.println("\t\t\tdisplayStringArray(_out, \""
							+ array.name + "\", " + getDeclaredName(array.name)
							+ ", _indent+1);");
			else
				out.println("\t\t\tdisplayElementArray(_out, \""
							+ array.name + "\", " + getDeclaredName(array.name)
							+ ", _indent+1);");
		}
		else {
			throw new XOMException("Unrecognized content type definition: "
									  + content.getClass().getName());
		}
	}

	public void writeJavaDisplayAnyContent(PrintWriter out)
	{
		// Display the fixed children array
		out.println("\t\t\tdisplayElementArray(_out, \"children\""
					+ ", children, _indent+1);");
	}

	public void writeJavaDisplayCDataContent(PrintWriter out)
	{
		// Display the text as "cdata"
		out.println("\t\t\tdisplayString(_out, \"cdata\", "
					+ "cdata, _indent+1);");
	}

	public void writeJavaDisplayXMLContent(PrintWriter out,
										   MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			MetaDef.Object obj = (MetaDef.Object)content;
			MetaDef.Definition type = getType(obj.type);

			if(type instanceof MetaDef.StringElement)
				out.println("\t\t\tdisplayXMLString(_out, \""
							+ getTypeInfo(obj.type, true).tagName + "\", "
							+ getDeclaredName(obj.name) + ");");
			else
				out.println("\t\t\tdisplayXMLElement(_out, "
							+ getDeclaredName(obj.name) + ");");
		}
		else if (content instanceof MetaDef.Array) {
			MetaDef.Array array = (MetaDef.Array)content;
			MetaDef.Definition type = getType(array.type);

			if(type instanceof MetaDef.StringElement)
				out.println("\t\t\tdisplayXMLStringArray(_out, \""
							+ getTypeInfo(array.type, true).tagName + "\", "
							+ getDeclaredName(array.name) + ");");
			else
				out.println("\t\t\tdisplayXMLElementArray(_out, "
							+ getDeclaredName(array.name) + ");");
		}
		else if (content instanceof MetaDef.Any) {
			// Display the fixed children array
			out.println("\t\t\tdisplayXMLElementArray(_out, children);");
		}
		else if (content instanceof MetaDef.CData) {
			// Display the CDATA section
			out.println("\t\t\t_out.cdata(cdata);");
		}
		else {
			throw new XOMException("Unrecognized content type definition: "
									  + content.getClass().getName());
		}
	}

	public void writeJavaDisplayXMLAnyContent(PrintWriter out)
	{
		// Display the fixed children array
		out.println("\t\t\tdisplayXMLElementArray(_out, children);");
	}

	public void writeJavaDisplayXMLCDataContent(PrintWriter out)
	{
		// Display the CDATA section
		out.println("\t\t\t_out.cdata(cdata);");
	}

	public void writeJavaDisplayDiffContent(PrintWriter out,
											MetaDef.Content content)
		throws XOMException
	{
		if(content instanceof MetaDef.Object) {
			MetaDef.Object obj = (MetaDef.Object)content;
			MetaDef.Definition type = getType(obj.type);

			if(type instanceof MetaDef.StringElement)
				out.println("\t\t\t_diff = _diff && displayStringDiff(\""
							+ obj.name + "\", "
							+ getDeclaredName(obj.name) + ", "
							+ "_cother." + getDeclaredName(obj.name) + ", "
							+ "_out, _indent+1);");
			else
				out.println("\t\t\t_diff = _diff && displayElementDiff(\""
							+ obj.name + "\", "
							+ getDeclaredName(obj.name) + ", "
							+ "_cother." + getDeclaredName(obj.name) + ", "
							+ "_out, _indent+1);");
		}
		else if (content instanceof MetaDef.Array) {
			MetaDef.Array array = (MetaDef.Array)content;
			MetaDef.Definition type = getType(array.type);

			if(type instanceof MetaDef.StringElement)
				out.println("\t\t\t_diff = _diff && displayStringArrayDiff(\""
							+ array.name + "\", "
							+ getDeclaredName(array.name) + ", "
							+ "_cother." + getDeclaredName(array.name) + ", "
							+ "_out, _indent+1);");
			else
				out.println("\t\t\t_diff = _diff && displayElementArrayDiff(\""
							+ array.name + "\", "
							+ getDeclaredName(array.name) + ", "
							+ "_cother." + getDeclaredName(array.name) + ", "
							+ "_out, _indent+1);");
		}
		else {
			throw new XOMException("Unrecognized content type definition: "
									  + content.getClass().getName());
		}
	}

	public void writeJavaDisplayDiffAnyContent(PrintWriter out)
	{
		// Display the fixed children array
		out.println("\t\t\t_diff = _diff && displayElementArrayDiff(\"children\", "
					+ "children, _cother.children, _out, _indent+1);");
	}

	public void writeJavaDisplayDiffCDataContent(PrintWriter out)
	{
		out.println("\t\t\t_diff = _diff && displayStringDiff(\"cdata\", "
					+ "cdata, _cother.cdata, _out, _indent+1);");
	}

	public void writeJavaDeclarePluginAttributes(PrintWriter out)
	{
		writeJavaDoc(out, 2, "defPackage is a built-in attribute "
					 + "defining the package of the plugin class.");
		out.println("\t\tpublic String defPackage;");
		out.println();

		writeJavaDoc(out, 2, "defClass is a built-in attribute "
					 + "definition the plugin parser class.");
		out.println("\t\tpublic String defClass;");
		out.println();
	}

	public void writeJavaDisplayPluginAttributes(PrintWriter out)
	{
		// Generate two display lines
		out.println("\t\t\tdisplayAttribute(_out, \"defPackage\", "
					+ "defPackage, _indent+1);");
		out.println("\t\t\tdisplayAttribute(_out, \"defClass\", "
					+ "defClass, _indent+1);");
	}

	public void writeJavaDisplayXMLPluginAttributes(PrintWriter out)
	{
		out.println("\t\t\t\t.add(\"defPackage\", defPackage)");
		out.println("\t\t\t\t.add(\"defClass\", defClass)");
	}

	public void writeJavaDisplayDiffPluginAttributes(PrintWriter out)
	{
		out.println("\t\t\t_diff = _diff && displayAttributeDiff(\""
					+ "defPackage\", defPackage, _cother.defPackage"
					+ ", _out, _indent+1);");
		out.println("\t\t\t_diff = _diff && displayAttributeDiff(\""
					+ "defClass\", defClass, _cother.defClass"
					+ ", _out, _indent+1);");
	}

	public void writeJavaGetPluginContent(PrintWriter out, boolean mixed)
	{
		if (mixed) {
			out.println("\t\t\t\tchildren = getMixedChildren(" +
						"_def, _pluginClass, \"\");");
		} else {
			out.println("\t\t\t\tchildren = getElementChildren(" +
						"_def, _pluginClass, \"\");");
		}
	}

	public void writeJavaDeclarePluginContent(PrintWriter out, boolean mixed)
	{
		out.println("\t\tpublic mondrian.xom." +
					(mixed ? "NodeDef" : "ElementDef") +
					"[] children;  /" + "/holder for variable-type children");
	}

	public void writeJavaDisplayPluginContent(PrintWriter out)
	{
		// Display the fixed children array
		out.println("\t\t\tdisplayElementArray(_out, \"children\""
					+ ", children, _indent+1);");
	}

	public void writeJavaDisplayXMLPluginContent(PrintWriter out)
	{
		// Display the fixed children array
		out.println("\t\t\tdisplayXMLElementArray(_out, children);");
	}

	public void writeJavaDisplayDiffPluginContent(PrintWriter out)
	{
		// Display the fixed children array
		out.println("\t\t\t_diff = _diff && displayElementArrayDiff(\"children\", "
					+ "children, _cother.children, _out, _indent+1);");
	}

	/**
	 * Write the name of the dtd file and java class to standard output.
	 * This output is used by shell scripts to grab these values.
	 * The output is only produced in test mode.
	 */
	public void writeOutputs()
	{
		if(testMode)
			System.out.println(model.dtdName + " " + model.className);
	}

	/**
	 * Main function for MetaGenerator. Arguments:
	 * <ol>
	 * <li>Name of XML file describing input model.
	 * <li>Name of output file directory.
	 * </ol>
	 */
	public static void main(String[] args)
	{
		int firstArg = 0;
		boolean testMode = false;
		if (firstArg < args.length && args[firstArg].equals("-debug")) {
			System.err.println("MetaGenerator pausing for debugging.  "
							   + "Attach your debugger "
							   + "and press return.");
			try {
				System.in.read();
				firstArg++;
			}
			catch(IOException ex) {
				// Do nothing
			}
		}
		if (firstArg < args.length && args[firstArg].equals("-test")) {
			System.err.println("Ignoring package name.");
			testMode = true;
			firstArg++;
		}

		if(args.length != 2 + firstArg) {
			System.err.println(
				"Usage: java MetaGenerator [-debug] [-test] " +
				"<XML model file> <output directory>");
			System.exit(2);
		}

		try {
			MetaGenerator generator = new MetaGenerator(
				args[0+firstArg], testMode);
			generator.writeFiles(args[1+firstArg], null);
			generator.writeOutputs();
		}
		catch(XOMException ex) {
			System.err.println("Generation of model failed:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(1);
		}
		catch(IOException ex) {
			System.err.println("Generation of model failed:");
			System.err.println(ex.toString());
			ex.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Display information about this generator for debug purposes.
	 */
	public void debugDisplay()
	{
		System.out.println("Model:");
		System.out.println(model.toString());
	}
}


// End MetaGenerator.java
