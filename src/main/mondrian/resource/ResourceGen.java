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
import java.lang.reflect.Constructor;

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
		ResourceGenTask rootArgs = parse(args);
		new ResourceGen().run(rootArgs);
	}

	static ResourceGenTask parse(String[] args) throws IOException
	{
		if (args.length < 1) {
			throw new java.lang.Error("No input file specified.");
		}
		ResourceGenTask rootArgs = new ResourceGenTask();
		rootArgs.dest = new File(System.getProperty("user.dir"), "src/main");
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-srcdir") && i + 1 < args.length) {
				rootArgs.setSrcdir(new File(args[++i]));
			} else if (arg.equals("-destdir") && i + 1 < args.length) {
				rootArgs.setDestdir(new File(args[++i]));
			} else {
				ResourceGenTask.Include resourceArgs =
						new ResourceGenTask.Include();
				rootArgs.addInclude(resourceArgs);
				resourceArgs.setName(arg);
			}
		}
		return rootArgs;
	}

	void run(ResourceGenTask rootArgs) throws IOException {
		rootArgs.validate();
		final ResourceGenTask.Include[] includes = rootArgs.getIncludes();
		for (int i = 0; i < includes.length; i++) {
			includes[i].process(this);
		}
	}

	/** Prints a message to the output stream. **/
	static void comment(String message) {
		System.out.println(message);
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
	 * Returns the name of the resource with the first letter capitalized,
	 * suitable for use in method names. For example, "MyErrorMessage".
	 */
	static String getResourceInitcap(ResourceDef.Resource resource) {
		String name = resource.name;
		if (name.equals(name.toUpperCase())) {
			return "_" + name;
		} else {
			return name.substring(0,1).toUpperCase() + name.substring(1);
		}
	}
	/**
	 * Returns a parameter list string, e.g. "String p0, int p1".
	 */
	static String getParameterList(String message) {
		final String [] types = ResourceGen.getArgTypes(message);
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
	static String getArgumentList(String message) {
		final String [] types = ResourceGen.getArgTypes(message);
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
	/** Get any comment relating to the message. */
	static String getComment(ResourceDef.Resource resource)
	{
		DOMWrapper[] children = resource.getDef().getChildren();
		for (int i = 0; i < children.length; i++) {
			DOMWrapper child = children[i];
			if (child.getType() == DOMWrapper.COMMENT) {
				return child.getText(); // first comment only
			}
		}
		return null; // no comment
	}

	FileTask createXmlTask(
			ResourceGenTask.Include include, String fileName, String className,
			String baseClassName) {
		return new XmlFileTask(include, fileName, className, baseClassName);
	}

	FileTask createPropertiesTask(
			ResourceGenTask.Include include, String fileName) {
		return new PropertiesFileTask(include, fileName);
	}

	static abstract class FileTask {
		ResourceGenTask.Include include;
		String className;
		String fileName;
		Locale locale;

		abstract void process(ResourceGen generator) throws IOException;
		abstract void generateBaseJava(
				ResourceGen generator,
				ResourceDef.ResourceBundle resourceList, PrintWriter pw);

		/** XML source file, e.g. happy/BirthdayResource_en.xml **/
		File getFile() {
			return new File(include.root.src, fileName);
		}
		String getPackageName() {
			int lastDot = className.lastIndexOf('.');
			if (lastDot < 0) {
				return null;
			} else {
				return className.substring(0,lastDot);
			}
		}
		File getPackageDirectory() {
			File file = include.root.dest;
			final String packageName = getPackageName();
			if (packageName == null) {
				return file;
			}
			return new File(file, packageName.replace('.', Util.fileSep));
		}
		/** If class name is <code>happy.BirthdayResource</code>, and locale is
		 * <code>en_US</code>, returns <code>BirthdayResource_en_US</code>. */
		String getClassNameSansPackage(Locale locale) {
			String s = className;
			int lastDot = className.lastIndexOf('.');
			if (lastDot >= 0) {
				s = s.substring(lastDot + 1);
			}
			if (locale != null) {
				s += '_' + locale.toString();
			}
			return s;
		}
		/**
		 * Generates a Java class, e.g. com/foo/MyResource.java or
		 * com/foo/MyResource_en_US.java, depending upon whether locale is
		 * null.
		 */
		void generateJava(
				ResourceGen generator,
				ResourceDef.ResourceBundle resourceList,
				Locale locale) {
			String fileName = getClassNameSansPackage(locale) + ".java";
			File file = new File(getPackageDirectory(), fileName);
			if (file.exists() &&
					file.lastModified() >= getFile().lastModified()) {
				generator.comment(file + " is up to date");
				return;
			}
			generator.comment("Generating " + file);
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
					generateBaseJava(generator, resourceList, pw);
				} else {
					generateLocaleJava(generator, resourceList, pw, locale);
				}
			} finally {
				pw.close();
			}
		}
		/**
		 * Generates a class containing a line for each resource.
		 */
		protected void generateLocaleJava(
				ResourceGen generator,
				ResourceDef.ResourceBundle resourceList, PrintWriter pw,
				Locale locale) {
			generateHeader(pw);
			// e.g. "MyResource_en_US"
			String className = getClassNameSansPackage(locale);
			// e.g. "MyResource"
			String baseClass = getClassNameSansPackage(null);
			pw.println("public class " + className + " extends " + baseClass + " {");
			pw.println("	public " + className + "() throws IOException {");
			pw.println("	}");
			pw.println("}");
			pw.println("");
			generateFooter(pw, className);
		}
		protected void generateHeader(PrintWriter pw) {
			pw.println("/" + "/ This class is generated. Do NOT modify it, or");
			pw.println("/" + "/ add it to source control.");
			pw.println();
			String packageName = getPackageName();
			if (packageName != null) {
				pw.println("package " + packageName + ";");
			}
			pw.println("import java.io.IOException;");
			pw.println("import java.util.Locale;");
			pw.println("import mondrian.resource.*;");
			pw.println();
			pw.println("/" + "**");
			pw.println(" * This class was generated");
			pw.println(" * by " + ResourceGen.class);
			pw.println(" * from " + getFile());
			pw.println(" * on " + new Date().toString() + ".");
			pw.println(" * It contains a list of messages, and methods to");
			pw.println(" * retrieve and format those messages.");
			pw.println(" **/");
			pw.println();
		}
		private void generateFooter(PrintWriter pw, String className) {
			pw.println("// End " + className + ".java");
		}
	}
}


class XmlFileTask extends ResourceGen.FileTask {
	String baseClassName;

	XmlFileTask(ResourceGenTask.Include include, String fileName, String className, String baseClassName) {
		this.include = include;
		this.fileName = fileName;
		if (className == null) {
			className = Util.fileNameToClassName(fileName, ".xml");
		}
		this.className = className;
		if (baseClassName == null) {
			baseClassName = "mondrian.resource.ShadowResourceBundle";
		}
		this.baseClassName = baseClassName;
		this.locale = Util.fileNameToLocale(fileName, ".xml");
	}
	void process(ResourceGen generator) throws IOException {
		URL url = Util.convertPathToURL(getFile());
		ResourceDef.ResourceBundle resourceList = Util.load(url);
		if (resourceList.locale == null) {
			throw new BuildException(
					"Resource file " + url + " must have locale");
		}
		this.locale = Util.parseLocale(resourceList.locale);
		if (this.locale == null) {
			throw new BuildException(
					"Invalid locale " + resourceList.locale);
		}
		generateJava(generator, resourceList, null);
		if (locale != null) {
			generateJava(generator, resourceList, locale);
		}
		if (locale == null) {
			locale = Locale.getDefault();
		}
		generateProperties(generator, resourceList, locale);
	}

	/**
	 * Generates a class containing a line for each resource.
	 */
	void generateBaseJava(
			ResourceGen generator,
			ResourceDef.ResourceBundle resourceList, PrintWriter pw) {
		generateHeader(pw);
		pw.print("public class " + getClassNameSansPackage(null));
		final String baseClass = baseClassName;
		if (baseClass != null) {
			pw.print(" extends " + baseClass);
		}
		String className = getClassNameSansPackage(null);
		pw.println(" {");
		pw.println("	public " + className + "() throws IOException {");
		pw.println("	}");
		pw.println("	private static String baseName = " + Util.quoteForJava(getClassName(null)) + ";");
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
		if (resourceList.code != null) {
			pw.println("\t// begin of included code");
			pw.print(resourceList.code.cdata);
			pw.println("\t// end of included code");
		}

		for (int j = 0; j < resourceList.resources.length; j++) {
			ResourceDef.Resource resource = resourceList.resources[j];
			if (resource.text == null) {
				throw new BuildException(
						"Resource '" + resource.name + "' has no message");
			}
			String text = resource.text.cdata;
			String comment = ResourceGen.getComment(resource);
			final String resourceInitcap = ResourceGen.getResourceInitcap(resource);// e.g. "Internal"

			String definitionClass = "mondrian.resource.ResourceDefinition";
			// e.g. "mondrian.resource.ChainableRuntimeException"
			String parameterList = ResourceGen.getParameterList(text);
			String argumentList = ResourceGen.getArgumentList(text);
			pw.print("\t/" + "** ");
			if (comment != null) {
				Util.fillText(pw, comment, "\t * ", "", 70);
				pw.println();
				pw.println("\t *");
				pw.print("\t * ");
			}
			Util.fillText(pw, "<code>" + resource.name + "</code> is '" + text + "'", "\t * ", "", -1);
			pw.println("	*/");
			pw.println("	public static final " + definitionClass + " " + resourceInitcap + " = new " + definitionClass + "(\"" + resourceInitcap + "\", " + Util.quoteForJava(text) + ");");
			pw.println("	public String get" + resourceInitcap + "(" + parameterList + ") {");
			pw.println("		return " + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString();");
			pw.println("	}");
			if (resource instanceof ResourceDef.Exception) {
				ResourceDef.Exception exception = (ResourceDef.Exception) resource;
				String errorClassName = getErrorClass(resourceList, exception);
				// Figure out what constructors the exception class has. We'd
				// prefer to use
				//   <init>(ResourceDefinition rd)
				//   <init>(ResourceDefinition rd, Throwable e)
				// if it has them, but we can use
				//   <init>(String s)
				//   <init>(String s, Throwable e)
				// as a fall-back.
				boolean hasInstCon = false, hasInstThrowCon = false,
					hasStringCon = false, hasStringThrowCon = false;
				try {
					Class errorClass;
					try {
						errorClass = Class.forName(errorClassName);
					} catch (ClassNotFoundException e) {
						// Might be in the java.lang package, for which we
						// allow them to omit the package name.
						errorClass = Class.forName("java.lang." + errorClassName);
					}
					Constructor[] constructors = errorClass.getConstructors();
					for (int i = 0; i < constructors.length; i++) {
						Constructor constructor = constructors[i];
						Class[] types = constructor.getParameterTypes();
						if (types.length == 1 &&
								ResourceInstance.class.isAssignableFrom(types[0])) {
							hasInstCon = true;
						}
						if (types.length == 1 &&
								String.class.isAssignableFrom(types[0])) {
							hasStringCon = true;
						}
						if (types.length == 2 &&
								ResourceInstance.class.isAssignableFrom(types[0]) &&
								Throwable.class.isAssignableFrom(types[1])) {
							hasInstThrowCon = true;
						}
						if (types.length == 2 &&
								String.class.isAssignableFrom(types[0]) &&
								Throwable.class.isAssignableFrom(types[1])) {
							hasStringThrowCon = true;
						}
					}
				} catch (ClassNotFoundException e) {
				}
				if (hasInstCon) {
					pw.println("	public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
					pw.println("		return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "));");
					pw.println("	}");
				} else if (hasInstThrowCon) {
					pw.println("	public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
					pw.println("		return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "), null);");
					pw.println("	}");
				} else if (hasStringCon) {
					pw.println("	public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
					pw.println("		return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString());");
					pw.println("	}");
				} else if (hasStringThrowCon) {
					pw.println("	public " + errorClassName + " new" + resourceInitcap + "(" + parameterList + ") {");
					pw.println("		return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString(), null);");
					pw.println("	}");
				}
				if (hasInstThrowCon) {
					pw.println("	public " + errorClassName + " new" + resourceInitcap + "(" + addLists(parameterList, "Throwable err") + ") {");
					pw.println("		return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + "), err);");
					pw.println("	}");
				} else if (hasStringThrowCon) {
					pw.println("	public " + errorClassName + " new" + resourceInitcap + "(" + addLists(parameterList, "Throwable err") + ") {");
					pw.println("		return new " + errorClassName + "(" + resourceInitcap + ".instantiate(" + addLists("this", argumentList) + ").toString(), err);");
					pw.println("	}");
				}
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


	/**
	 * Returns the type of error which is to be thrown by this resource.
	 * Result is null if this is not an error.
	 */
	static String getErrorClass(
			ResourceDef.ResourceBundle resourceList,
			ResourceDef.Exception exception) {
		if (exception.className != null) {
			return exception.className;
		} else if (resourceList.exceptionClassName != null) {
			return resourceList.exceptionClassName;
		} else {
			return "mondrian.resource.ChainableRuntimeException";
		}
	}

	private void generateProperties(
			ResourceGen generator,
			ResourceDef.ResourceBundle resourceList,
			Locale locale) {
		String fileName = getClassNameSansPackage(locale) + ".properties";
		File file = new File(getPackageDirectory(), fileName);
		if (file.exists() &&
				file.lastModified() >= getFile().lastModified()) {
			generator.comment(file + " is up to date");
			return;
		}
		generator.comment("Generating " + file);
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
			generateProperties(resourceList, pw, locale);
		} finally {
			pw.close();
		}
	}
	/**
	 * Generates a class containing a line for each resource.
	 *
	 * @pre locale != null
	 */
	private void generateProperties(
			ResourceDef.ResourceBundle resourceList, PrintWriter pw,
			Locale locale) {
		String fullClassName = getClassName(locale);
		pw.println("# This file contains the resources for");
		pw.println("# class '" + fullClassName + "' and locale '" + locale + "'.");
		pw.println("# It was generated by " + ResourceGen.class);
		pw.println("# from " + getFile());
		pw.println("# on " + new Date().toString() + ".");
		pw.println();
		String localeName = null;
		if (locale != null) {
			localeName = locale.toString();
		}
		for (int i = 0; i < resourceList.resources.length; i++) {
			ResourceDef.Resource resource = resourceList.resources[i];
			final String name = resource.name;
			if (resource.text == null) {
				throw new BuildException(
						"Resource '" + name + "' has no message");
			}
			final String message = resource.text.cdata;
			if (message == null) {
				continue;
			}
			pw.println(name + "=" + Util.quoteForProperties(message));
		}
		pw.println("# End " + fullClassName + ".properties");
	}
	String getClassName(Locale locale) {
		String s = className;
		if (locale != null) {
			s += '_' + locale.toString();
		}
		return s;
	}
}

class PropertiesFileTask extends ResourceGen.FileTask {
	PropertiesFileTask(ResourceGenTask.Include include, String fileName) {
		this.include = include;
		this.fileName = fileName;
		this.className = Util.fileNameToClassName(fileName, ".properties");
		this.locale = Util.fileNameToLocale(fileName, ".properties");
	}

	/**
	 * Given an existing properties file such as
	 * <code>happy/Birthday_fr_FR.properties</code>, generates the
	 * corresponding Java class happy.Birthday_fr_FR.java</code>.
	 *
	 * <p>todo: Validate.
	 */
	void process(ResourceGen generator) throws IOException {
		// e.g. happy/Birthday_fr_FR.properties
		String s = Util.fileNameSansLocale(fileName, ".properties");
		File file = new File(include.root.src, s + ".xml");
		URL url = Util.convertPathToURL(file);
		ResourceDef.ResourceBundle resourceList = Util.load(url);
		generateJava(generator, resourceList, locale);
	}

	void generateBaseJava(
			ResourceGen generator,
			ResourceDef.ResourceBundle resourceList, PrintWriter pw) {
		throw new UnsupportedOperationException();
	}
}

// End ResourceGen.java
