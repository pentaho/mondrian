/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 December, 2001
*/

package mondrian.resource;
import mondrian.xom.DOMWrapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.*;

/**
 * <code>ResourceGen</code> parses an XML file containing error messages, and
 * generates .java file to access the errors. Usage:<blockquote>
 *
 * <pre>ResourceGen xmlFile</pre>
 *
 * </blockquote>For example,<blockquote>
 *
 * <pre>jview mondrian.resource.ResourceGen ServerResource_en.xml</pre>
 *
 * </blockquote></p>
 *
 * <p>This will create class {@link Broadbase.util.ServerResource}, with a
 * function corresponding to each error message in
 * <code>ServerResource_en.xml</code>.  The installer is required to set root
 * path to a appropriate properties files <code>global.asa</code> or
 * <code>bbinst.ini</code></p>.
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class ResourceGen
{
	public static void main(String [] args) throws IOException
	{
		PrintWriter pw = new PrintWriter(System.out);
		ResourceGen rg = new ResourceGen(args,pw);
		rg.genResource(pw);
	}

	String strFile; // e.g. Broadbase/util/ServerResource_en.xml
	ResourceDef.BaflResourceList resourceList;
	String errorClass; // e.g. BBReposError

	ResourceGen(String[] args,PrintWriter pw) throws IOException
	{
		if (args.length < 1) {
			throw new java.lang.Error("No input file specified.");
		}
		if (args.length > 1) {
			throw new java.lang.Error("Too many arguments.");
		}
		strFile = args[0];
		URL url = mondrian.resource.Util.stringToUrl(strFile);
		resourceList = Util.load(url);
		errorClass = resourceList.errorClass;
	}

	/** Get the message. */
	static String getMessage(ResourceDef.BaflResourceText resource)
	{
		return resource.cdata.trim();
	}

	/** Get any comment relating to the message. */
	static String getComment(ResourceDef.BaflResourceText resource)
	{
		DOMWrapper[] children = resource._def.getChildren();
		for (int i = 0; i < children.length; i++) {
			DOMWrapper child = children[i];
			if (child.getType() == DOMWrapper.COMMENT) {
				return child.getText(); // first comment only
			}
		}
		return null; // no comment
	}

	/**
	 * the number and types of parameters in the given error message,
	 * expressed as an array of Strings (legal values are
	 * currently "String" or "int") ordered by parameter number
	 */
	static String [] getArgTypes(String message)
	{
		Vector v = new Vector();
		int offset = 0;
		for (;;) {
			offset = message.indexOf('%',offset);
			if (offset == -1) {
				break;
			}
			offset++;
			if (offset >= message.length()) {
				break; // '%' is last character in string
			}
			char c = message.charAt(offset);
			String type = "String";
			if (!Character.isDigit(c)) {
				if (c != 'i') {
					// we allow percent signs in some contexts
					continue;
				}
				type = "int";
				c = message.charAt(offset+1);
				if (!Character.isDigit(c)) {
					throw new java.lang.Error(
						"message format error in '"+message+"'");
				}
			}
			int ordinal = c - '0';
			if ((ordinal < 1) || (ordinal > 5)) {
				throw new java.lang.Error(
					"invalid parameter ordinal in '"+message+"'");
			}
			if (v.size() < ordinal) {
				v.setSize(ordinal);
			}
			v.setElementAt(type,ordinal-1);
		}
		String [] strings = new String[v.size()];
		v.copyInto(strings);
		for (int i = 0; i < strings.length; i++) {
			if (strings[i] == null) {
				throw new java.lang.Error(
					"missing parameter in '"+message+"'");
			}
		}
		return strings;
	}

	private void genResource(PrintWriter pw)
	{
		pw.println("/" + "/ This class is generated. Do NOT modify it, or");
		pw.println("/" + "/ add it to source control or the J++ project.");
		pw.println();
		pw.println("package " + resourceList.packageName + ";");
		pw.println();
		pw.println("/" + "**");
		pw.println(" * This class was generated");
		pw.println(" * by " + getClass());
		pw.println(" * from " + strFile);
		pw.println(" * on " + new Date().toString() + ".");
		pw.println(" * It contains a list of messages, and methods to");
		pw.println(" * retrieve and format those messages.");
		pw.println(" **/");
		pw.println();
		pw.print("public class " + resourceList.className);
		if (resourceList.baseClass != null) {
			pw.print(" extends " + resourceList.baseClass);
		}
		pw.println( " implements mondrian.resource.Resource");
		pw.println("{");


		pw.println("\tpublic " + resourceList.className + "() {}");

		pw.println("\tpublic " + resourceList.className + "(java.net.URL url, java.util.Locale locale) throws java.io.IOException {");
		pw.println("\t\tinit(url, locale);");
		pw.println("\t}");

		if (resourceList.code != null) {
			pw.println("\t// begin of included code");
			pw.print(resourceList.code.cdata);
			pw.println("\t// end of included code");
		}

		for (int j = 0; j < resourceList.resources.length; j++) {
			ResourceDef.BaflResourceText resource = resourceList.resources[j];
			Integer id = resource.id;
			String message = getMessage(resource);
			String comment = getComment(resource);
			int nArgs = getArgTypes(message).length;
			String [] argTypes = getArgTypes(message);
			String strName = resource.macroName;
			String strSuffix = strName;
			if (strName.equals(strName.toUpperCase())) {
				strSuffix = "_" + strSuffix;
			} else {
				strSuffix = strName.substring(0,1).toUpperCase() +
					strName.substring(1);
			}

			// Generate:
//	/oo A comment.
//	o
//	o e: "MDX cube '%1' not found" o/
			pw.print("\t/" + "** ");
			if (comment != null) {
				Util.fillText(pw, comment, "\t * ", "", 70);
				pw.println();
				pw.println("\t *");
				pw.print("\t * ");
			}
			Util.fillText(
				pw, resource.type + ": " + message,
				"\t * ", "", -1);
			pw.println(" */");

			// Generate:
//	public static final int MdxCubeNotFound = 321000;
			pw.println(
				"\tpublic static final int " + strName + " = " + id + ";");

			// Generate:
//	public void getMdxCubeNotFound(String arg1) {
//		return res.formatError(321000,new String[]{arg1});
//	}
			pw.print(
				"\tpublic String get" + strSuffix + "(");
			ListGenerator listGen = new ListGenerator(pw, "", ", ", "");
			if (resource.context.booleanValue()) {
				listGen.emit(resourceList.contextParams);
			}
			for (int i = 0; i < nArgs; i++) {
				listGen.emit(argTypes[i] + " p" + i);
			}
			pw.println(") {");
			pw.print("\t\treturn formatError(" + id + ", new Object[] {");
			listGen.reset();
			if (resource.context.booleanValue()) {
				listGen.emit(resourceList.contextArgs);
			}
			for (int i = 0; i < nArgs; i++) {
				if (argTypes[i].equals("int")) {
					listGen.emit("Integer.toString(p" + i + ")");
				} else {
					listGen.emit("p" + i);
				}
			}
			pw.println("});");
			pw.println("\t}");

			if (resourceList.generateNew.booleanValue() &&
				(resource.getSeverity() != Resource.SEVERITY_INFO ||
				 true)) {

				// Generate:
//	public MdxError newMdxCubeNotFound(String p1) {
//		return new MdxError(null, this, MdxCubeNotFound, new Object[]{p1});
//	}
//	public MdxError newMdxCubeNotFound(Throwable err, String p1) {
//		return new MdxError(err, MdxCubeNotFound, new Object[]{p1});

				for (int k = 0; k < 2; k++) {
					if (k == 1 &&
						false &&
						resource.getSeverity() != Resource.SEVERITY_ERR) {
						// We don't want to accidentally throw warnings, so don't
						// generate the "...(Throwable err, ..." form unless it's
						// an error.
						continue;
					}

					pw.print("\tpublic " + errorClass + " new" + strSuffix + "(");
					listGen.reset();
					if (k == 1) {
						listGen.emit("Throwable err");
					}
					if (resource.context.booleanValue()) {
						listGen.emit(resourceList.contextParams);
					}
					for (int i = 0; i < nArgs; i++) {
						listGen.emit(argTypes[i] + " p" + i);
					}
					pw.println(") {");
					pw.print("\t\treturn new " + errorClass + "(");
					listGen.reset();
					listGen.emit(k == 0 ? "null" : "err");
					listGen.emit("this");
					listGen.emit(strName);
					if (resource.context.booleanValue()) {
						listGen.emit(resourceList.contextArgs);
					}
					listGen.emit("new Object[] {");
					listGen.reset();
					for (int i = 0; i < nArgs; i++) {
						if (argTypes[i].equals("int")) {
							listGen.emit("Integer.toString(p" + i + ")");
						} else {
							listGen.emit("p" + i);
						}
					}
					pw.println("});");
					pw.println("\t}");
				}
			}
		}

		pw.println("");
		pw.println("}");
		pw.close();
	}
}

/**
 * Code-generator helper class for generating a list of items.
 */
class ListGenerator
{
	PrintWriter pw;
	String first;
	String last;
	String mid;
	int nEmitted;

	ListGenerator(PrintWriter pw, String first, String mid, String last)
	{
		this.pw = pw;
		this.first = first;
		this.last = last;
		this.mid = mid;
		reset();
	}

	boolean isEmpty()
	{
		return nEmitted == 0;
	}

	void emit(String s)
	{
		if (nEmitted++ == 0) {
			pw.print(first);
		} else {
			pw.print(mid);
		}
		pw.print(s);
	}

	void end()
	{
		if (nEmitted > 0) {
			pw.print(last);
		}
		reset();
	}

	void reset()
	{
		nEmitted = 0;
	}
}

// End ResourceGen.java
