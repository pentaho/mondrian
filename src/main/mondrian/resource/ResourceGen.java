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

import java.io.*;
import java.net.URL;
import java.util.*;
import java.text.MessageFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.DateFormat;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import org.apache.tools.ant.BuildException;

/**
 * <code>ResourceGen</code> parses an XML file containing error messages, and
 * generates .java file to access the errors. Usage:<blockquote>
 *
 * <pre>ResourceGen xmlFile</pre>
 *
 * </blockquote>For example,<blockquote>
 *
 * <pre>java mondrian.resource.ResourceGen MyResource_en.xml</pre>
 *
 * </blockquote></p>
 *
 * <p>This will create class <code>MyResource</code>, with a
 * function corresponding to each error message in
 * <code>MyResource_en.xml</code>.</p>
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class ResourceGen
{
	public static void main(String [] args) throws IOException
	{
		RootArgs rootArgs = parse(args);
		run(rootArgs);
	}

	static RootArgs parse(String[] args) throws IOException
	{
		if (args.length < 1) {
			throw new java.lang.Error("No input file specified.");
		}
		RootArgs rootArgs = new RootArgs();
		rootArgs.dest = new File(System.getProperty("user.dir"), "src/main");
		rootArgs.packageName = "mondrian.olap"; // todo:
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			ResourceArgs resourceArgs = new ResourceArgs();
			rootArgs.add(resourceArgs);
			resourceArgs.setFile(new File(args[0]));
		}
		return rootArgs;
	}

	static void run(RootArgs rootArgs) throws IOException {
		rootArgs.validate();
		final ResourceArgs[] resources = rootArgs.getResourceArgs();
		for (int i = 0; i < resources.length; i++) {
			resources[i].process();
		}
	}

	/** Prints a message to the output stream. **/
	static void comment(String message) {
		System.out.println(message);
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
	 * Returns the number and types of parameters in the given error message,
	 * expressed as an array of Strings (legal values are
	 * currently "String", "Number", "java.util.Date", and null) ordered by
	 * parameter number.
	 */
	static String [] getArgTypes(String message) {
		Format[] argFormats;
		try {
			// We'd like to do
			//  argFormats = format.getFormatsByArgumentIndex()
			// but it doesn't exist until JDK 1.4, and we'd like this code
			// to work earlier.
			Method method = MessageFormat.class.getMethod("getFormatsByArgumentIndex", null);
			try {
				MessageFormat format = new MessageFormat(message);
				argFormats = (Format[]) method.invoke(format, null);
				String[] argTypes = new String[argFormats.length];
				for (int i = 0; i < argFormats.length; i++) {
					Format argFormat = argFormats[i];
					if (argFormat == null) {
						argTypes[i] = "String";
					} else if (argFormat instanceof NumberFormat) {
						argTypes[i] = "Number";
					} else if (argFormat instanceof DateFormat) {
						argTypes[i] = "java.util.Date"; // might be date or time
					} else {
						argTypes[i] = "String";
					}
				}
				return argTypes;
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e.toString());
			} catch (IllegalArgumentException e) {
				throw new RuntimeException(e.toString());
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.toString());
			}
		} catch (NoSuchMethodException e) {
			// Fallback pre JDK 1.4
			return getArgTypesByHand(message);
		} catch (SecurityException e) {
			throw new RuntimeException(e.toString());
		}
	}

	static String [] getArgTypesByHand(String message) {
		String[] argTypes = new String[10];
		int length = 0;
		for (int i = 0; i < 10; i++) {
			argTypes[i] = getArgType(i, message);
			if (argTypes[i] != null) {
				length = i + 1;
			}
		}
		// Created a truncated copy (but keep intervening nulls).
		String[] argTypes2 = new String[length];
		System.arraycopy(argTypes, 0, argTypes2, 0, length);
		return argTypes2;
	}

	private static String getArgType(int i, String message) {
		String arg = "{" + Integer.toString(i); // e.g. "{1"
		int index = message.lastIndexOf(arg);
		if (index < 0) {
			return null; // argument does not occur
		}
		index += arg.length();
		int end = message.length();
		while (index < end && message.charAt(index) == ' ') {
			index++;
		}
		if (index < end && message.charAt(index) == ',') {
			index++;
			while (index < end && message.charAt(index) == ' ') {
				index++;
			}
			if (index < end) {
				String sub = message.substring(index);
				if (sub.startsWith("number")) {
					return "Number";
				} else if (sub.startsWith("date")) {
					return "java.util.Date";
				} else if (sub.startsWith("time")) {
					return "java.util.Date";
				} else if (sub.startsWith("choice")) {
					return null;
				}
			}
		}
		return "String";
	}

	/**
	 * Generates a Java class, e.g. com/foo/MyResource.java.
	 */
	static void genJava(
			ResourceArgs resourceArgs,
			ResourceDef.BaflResourceList resourceList,
			Locale locale) {
		String fileName = resourceArgs.getClassName(locale) + ".java";
		File file = new File(resourceArgs.getPackageDirectory(), fileName);
		if (file.exists() &&
				file.lastModified() >= resourceArgs.file.lastModified()) {
			comment(file + " is up to date");
			return;
		}
		comment("Generating " + file);
		final FileOutputStream out;
		try {
			if (file.getParentFile() != null) {
				file.getParentFile().mkdirs();
			}
			out = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			throw new BuildException("Error while writing " + file, e);
		}
		PrintWriter pw = new PrintWriter(out);
		try {
			if (locale == null) {
				generateJava(resourceList, pw, resourceArgs);
			} else {
				generateLocaleJava(resourceList, pw, resourceArgs, locale);
			}
		} finally {
			pw.close();
		}
	}

	/**
	 * Generates a class containing a line for each resource.
	 */
	private static void generateJava(
			ResourceDef.BaflResourceList resourceList, PrintWriter pw,
			ResourceArgs resourceArgs) {
		generateHeader(pw, resourceArgs);
		pw.print("public class " + resourceArgs.getClassName(null));
		final String baseClass = resourceArgs.getBaseClass();
		if (baseClass != null) {
			pw.print(" extends " + baseClass);
		}
		String className = resourceArgs.getClassName(null);
		pw.println(" {");
		pw.println("	public " + className + "() throws IOException {");
		pw.println("	}");
		pw.println("	private static String baseName = " + className + ".class.getName();");
		pw.println("	/**");
		pw.println("	 * Retrieves the singleton instance of {@link " + className + "}. If");
		pw.println("	 * the application has called {@link #setThreadLocale}, returns the");
		pw.println("	 * resource for the thread's locale.");
		pw.println("	 */");
		pw.println("	public static synchronized " + className + " instance() {");
		pw.println("		return (" + className + ") instance(baseName);");
		pw.println("	}");
		pw.println("	/**");
		pw.println("	 * Retrieves the instance of {@link " + className + "} for the given locale.");
		pw.println("	 */");
		pw.println("	public static synchronized " + className + " instance(Locale locale) {");
		pw.println("		return (" + className + ") instance(baseName, locale);");
		pw.println("	}");
		pw.println("	private static final Object[] emptyObjectArray = new Object[0];");
		if (resourceList.code != null) {
			pw.println("\t// begin of included code");
			pw.print(resourceList.code.cdata);
			pw.println("\t// end of included code");
		}

		for (int j = 0; j < resourceList.resources.length; j++) {
			ResourceDef.BaflResourceText resource = resourceList.resources[j];
			Integer id = resource.id;
			String message = resourceArgs.getMessage(resource);
			String comment = getComment(resource);
			final String resourceInitcap = resourceArgs.getResourceInitcap(
					resource);// e.g. "Internal"

			String definitionClass = "mondrian.resource.ResourceDefinition";
			// e.g. "mondrian.resource.ChainableRuntimeException"
			String errorClass = resourceArgs.getErrorClass(resource);
			String parameterList = resourceArgs.getParameterList(resource);
			String argumentList = resourceArgs.getArgumentList(resource);
			pw.print("\t/" + "** ");
			if (comment != null) {
				Util.fillText(pw, comment, "\t * ", "", 70);
				pw.println();
				pw.println("\t *");
				pw.print("\t * ");
			}
			Util.fillText(pw, resource.type + ": " + message, "\t * ", "", -1);
			pw.println("	*/");
			pw.println("	public static final " + definitionClass + " " + resourceInitcap + " = new " + definitionClass + "(\"" + resourceInitcap + "\", " + Util.quoteForJava(message) + ");");
			pw.println("	public String get" + resourceInitcap + "(" + parameterList + ") {");
			pw.println("		return " + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString();");
			pw.println("	}");
			if (errorClass != null) {
				pw.println("	public " + errorClass + " new" + resourceInitcap + "(" + parameterList + ") {");
				pw.println("		return new " + errorClass + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "), null);");
				pw.println("	}");
				pw.println("	public " + errorClass + " new" + resourceInitcap + "(" + addLists(parameterList, "Throwable err") + ") {");
				pw.println("		return new " + errorClass + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "), err);");
				pw.println("	}");
			}
		}
		pw.println("");
		pw.println("}");
	}


	private static String addLists(String x, String y) {
		if (x == null || x.equals("")) {
			if (y == null || y.equals("")) {
				return "";
			} else {
				return y;
			}
		} else if (y == null || y.equals("")) {
			return x;
		} else {
			return x + ", " + y;
		}
	}

	private static void generateHeader(PrintWriter pw, ResourceArgs resourceArgs) {
		pw.println("/" + "/ This class is generated. Do NOT modify it, or");
		pw.println("/" + "/ add it to source control.");
		pw.println();
		pw.println("package " + resourceArgs.getPackageName() + ";");
		pw.println("import java.io.IOException;");
		pw.println("import java.util.Locale;");
		pw.println();
		pw.println("/" + "**");
		pw.println(" * This class was generated");
		pw.println(" * by " + ResourceGen.class);
		pw.println(" * from " + resourceArgs.file);
		pw.println(" * on " + new Date().toString() + ".");
		pw.println(" * It contains a list of messages, and methods to");
		pw.println(" * retrieve and format those messages.");
		pw.println(" **/");
		pw.println();
	}

	/**
	 * Generates a class containing a line for each resource.
	 */
	private static void generateLocaleJava(
			ResourceDef.BaflResourceList resourceList, PrintWriter pw,
			ResourceArgs resourceArgs, Locale locale) {
		generateHeader(pw, resourceArgs);
		// e.g. "MyResource_en_US"
		String className = resourceArgs.getClassName(locale);
		// e.g. "MyResource"
		String baseClass = resourceArgs.getClassName(null);
		pw.println("public class " + className + " extends " + baseClass + " {");
		pw.println("	public " + className + "() throws IOException {");
		pw.println("	}");
		pw.println("}");
		pw.println("");
		generateFooter(pw, className);
	}

	private static void generateFooter(PrintWriter pw, String className) {
		pw.println("// End " + className + ".java");
	}

	private static void generateProperties(
			ResourceArgs resourceArgs,
			ResourceDef.BaflResourceList resourceList,
			Locale locale) {
		String fileName = resourceArgs.getClassName(locale) + ".properties";
		File file = new File(resourceArgs.getPackageDirectory(), fileName);
		if (file.exists() &&
				file.lastModified() >= resourceArgs.file.lastModified()) {
			comment(file + " is up to date");
			return;
		}
		comment("Generating " + file);
		final FileOutputStream out;
		try {
			if (file.getParentFile() != null) {
				file.getParentFile().mkdirs();
			}
			out = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			throw new BuildException("Error while writing " + file, e);
		}
		PrintWriter pw = new PrintWriter(out);
		try {
			generateProperties(resourceList, pw, resourceArgs, locale);
		} finally {
			pw.close();
		}
	}

	/**
	 * Generates a class containing a line for each resource.
	 */
	private static void generateProperties(
			ResourceDef.BaflResourceList resourceList, PrintWriter pw,
			ResourceArgs resourceArgs, Locale locale) {
		String fullClassName = resourceArgs.getFullClassName(locale);
		pw.println("# This file contains the resources for");
		pw.println("# class '" + fullClassName + "' and locale '" + locale + "'.");
		pw.println("# It was generated by " + ResourceGen.class);
		pw.println("# from " + resourceArgs.file);
		pw.println("# on " + new Date().toString() + ".");
		pw.println();
		for (int i = 0; i < resourceList.resources.length; i++) {
			ResourceDef.BaflResourceText resource = resourceList.resources[i];
			final String name = resource.macroName;
			final String message = resourceArgs.getMessage(resource);
			pw.println(name + "=" + Util.quoteForProperties(message));
		}
		pw.println("# End " + fullClassName + ".properties");
	}
	private static String removeSuffix(String s, final String suffix) {
		if (s.endsWith(suffix)) {
			s = s.substring(0,s.length()-suffix.length());
		}
		return s;
	}

	static class RootArgs {
		private ArrayList resources = new ArrayList();
		File dest;
		String packageName = "";
		void add(ResourceArgs resourceArgs) {
			resources.add(resourceArgs);
			resourceArgs.root = this;
		}
		void validate() {
			if (dest == null) {
				throw new BuildException(
						"You must specify 'dest' attribute of <ResourceGen> task");
			}
			final ResourceArgs[] args = getResourceArgs();
			for (int i = 0; i < args.length; i++) {
				ResourceArgs arg = args[i];
				arg.validate();
			}
		}
		ResourceArgs[] getResourceArgs() {
			return (ResourceArgs[]) resources.toArray(new ResourceArgs[0]);
		}
	}

	static class ResourceArgs {
		RootArgs root;
		/** XML source file, e.g. foo/MyResource_en.xml **/
		File file;
		/** Class name. **/
		String className;
		/** Package name, for example "com.foo". "" means the root package,
		 * whereas null will revert to {@link RootArgs#packageName}. */
		String packageName;
		/** Base class. */
		String baseClassName;

		void validate() throws BuildException {
			if (baseClassName == null) {
				baseClassName = "mondrian.resource.ShadowResourceBundle";
			}
		}
		void setFile(File file) {
			this.file = file;
		}
		public void setBaseClass(String baseClassName) {
			this.baseClassName = baseClassName;
		}
		String getBaseClass() {
			return baseClassName;
		}
		String getPackageName() {
			if (packageName != null) {
				return packageName;
			} else {
				return root.packageName;
			}
		}
		File getPackageDirectory() {
			File file = root.dest;
			final String packageName = getPackageName();
			if (packageName == null) {
				return file;
			}
			char fileSep = System.getProperty("file.separator").charAt(0);
			return new File(file, packageName.replace('.', fileSep));
		}
		void setClassName(String className) {
			this.className = className;
		}
		/** For example, if the input file is
		 * <code>foo/MyResource_en.xml</code>, returns the locale "en".
		 */
		Locale getLocale() {
			String s = file.getName();
			s = removeSuffix(s, ".xml");
			int score = s.indexOf('_');
			if (score <= 0) {
				return null;
			} else {
				String localeName = s.substring(score + 1);
				int score1 = localeName.indexOf('_');
				String language, country = "", variant = "";
				if (score1 < 0) {
					language = localeName;
				} else {
					language = localeName.substring(0, score1);
					int score2 = localeName.indexOf('_',score1 + 1);
					if (score2 < 0) {
						country = localeName.substring(score1 + 1);
					} else {
						country = localeName.substring(score1 + 1, score2);
						variant = localeName.substring(score2 + 1);
					}
				}
				return new Locale(language,country,variant);
			}
		}
		String getClassName(Locale locale) {
			String s = file.getName();
			s = removeSuffix(s, ".xml");
			int score = s.indexOf('_');
			if (score >= 0) {
				s = s.substring(0,score);
			}
			if (locale != null) {
				s += '_' + locale.toString();
			}
			return s;
		}
		String getFullClassName(Locale locale) {
			String packageName = getPackageName();
			String className = getClassName(locale);
			if (packageName == null) {
				return className;
			} else {
				return packageName + "." + className;
			}
		}
		/**
		 * Returns the type of error which is to be thrown by this resource.
		 * Result is null if this is not an error.
		 */
		String getErrorClass(ResourceDef.BaflResourceText resource) {
			if (resource.type.equals("e")) {
				return "mondrian.resource.ChainableRuntimeException";
			} else {
				return null;
			}
		}
		/**
		 * Returns the name of the resource with the first letter capitalized,
		 * suitable for use in method names. For example, "MyErrorMessage".
		 */
		String getResourceInitcap(ResourceDef.BaflResourceText resource) {
			String name = resource.macroName;
			if (name.equals(name.toUpperCase())) {
				return "_" + name;
			} else {
				return name.substring(0,1).toUpperCase() + name.substring(1);
			}
		}
		/**
		 * Returns the message.
		 */
		String getMessage(ResourceDef.BaflResourceText resource) {
			return resource.cdata.trim();
		}
		/**
		 * Returns a parameter list string, e.g. "String p0, int p1".
		 */
		String getParameterList(ResourceDef.BaflResourceText resource) {
			final String [] types = getArgTypes(resource.cdata);
			if (types.length == 0) {
				return "";
			}
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < types.length; i++) {
				String type = types[i];
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(type);
				sb.append(" p");
				sb.append(Integer.toString(i));
			}
			return sb.toString();
		}
		/**
		 * Returns a parameter list string, e.g.
		 * "new Object[] {p0, new Integer(p1)}"
		 */
		String getArgumentList(ResourceDef.BaflResourceText resource) {
			final String [] types = getArgTypes(resource.cdata);
			if (types.length == 0) {
				return "emptyObjectArray";
			}
			StringBuffer sb = new StringBuffer("new Object[] {");
			for (int i = 0; i < types.length; i++) {
				String type = types[i];
				if (i > 0) {
					sb.append(", ");
				}
				sb.append("p");
				sb.append(Integer.toString(i));
			}
			sb.append("}");
			return sb.toString();
		}

		void process() throws IOException {
			URL url = Util.convertPathToURL(file);
			ResourceDef.BaflResourceList resourceList = Util.load(url);
			genJava(this, resourceList, null);
			Locale locale = getLocale();
			if (locale != null) {
				genJava(this, resourceList, locale);
			}
			generateProperties(this, resourceList, locale);
		}
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
