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
 * <p>See also the ANT Task, {@link ResourceGenTask}.</p>
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class ResourceGen
{
    private static final String JAVA_STRING = "String";
    private static final String CPP_STRING = "const std::string &";

    private static final String JAVA_NUMBER = "Number";
    private static final String CPP_NUMBER = "int";

    private static final String JAVA_DATE_TIME = "java.util.Date";
    private static final String CPP_DATE_TIME = "time_t";

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
            if (arg.equals("-mode") && i + 1 < args.length) {
                rootArgs.setMode(args[++i]);
            } else if (arg.equals("-srcdir") && i + 1 < args.length) {
				rootArgs.setSrcdir(new File(args[++i]));
			} else if (arg.equals("-destdir") && i + 1 < args.length) {
				rootArgs.setDestdir(new File(args[++i]));
      } else if (arg.equals("-locales") && i + 1 < args.length) {
        rootArgs.setLocales(args[++i]);
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
	static String [] getArgTypes(String message, boolean forJava) {
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
						argTypes[i] = forJava ? JAVA_STRING : CPP_STRING;
					} else if (argFormat instanceof NumberFormat) {
						argTypes[i] = forJava ? JAVA_NUMBER : CPP_NUMBER;
					} else if (argFormat instanceof DateFormat) {
                        // might be date or time
						argTypes[i] = forJava ? JAVA_DATE_TIME : CPP_DATE_TIME;
					} else {
						argTypes[i] = forJava ? JAVA_STRING : CPP_STRING;
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
			return getArgTypesByHand(message, forJava);
		} catch (SecurityException e) {
			throw new RuntimeException(e.toString());
		}
	}

	static String [] getArgTypesByHand(String message, boolean forJava) {
		String[] argTypes = new String[10];
		int length = 0;
		for (int i = 0; i < 10; i++) {
			argTypes[i] = getArgType(i, message, forJava);
			if (argTypes[i] != null) {
				length = i + 1;
			}
		}
		// Created a truncated copy (but keep intervening nulls).
		String[] argTypes2 = new String[length];
		System.arraycopy(argTypes, 0, argTypes2, 0, length);
		return argTypes2;
	}

	private static String getArgType(int i, String message, boolean forJava) {
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
					return forJava ? JAVA_NUMBER : CPP_NUMBER;
				} else if (sub.startsWith("date")) {
					return forJava ? JAVA_DATE_TIME : CPP_DATE_TIME;
				} else if (sub.startsWith("time")) {
					return forJava ? JAVA_DATE_TIME : CPP_DATE_TIME;
				} else if (sub.startsWith("choice")) {
					return null;
				}
			}
		}
		return forJava ? JAVA_STRING : CPP_STRING;
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
	static String getParameterList(String message, boolean forJava) {
		final String [] types = ResourceGen.getArgTypes(message, forJava);
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
            
            if (!forJava && (type.endsWith("&") || type.endsWith("*"))) {
                sb.append("p");
            } else {
                sb.append(" p");
            }

			sb.append(Integer.toString(i));
		}
		return sb.toString();
	}
	/**
	 * Returns a parameter list string, e.g.
	 * "new Object[] {p0, new Integer(p1)}"
	 */
	static String getArgumentList(String message, boolean forJava) {
		final String [] types = ResourceGen.getArgTypes(message, forJava);
        
        if (forJava) {
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
        } else {
            if (types.length == 0) return "";
            
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < types.length; i++) {
                String type = types[i];
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("p");
                sb.append(Integer.toString(i));
            }
            return sb.toString();
        }
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
			String baseClassName, boolean outputJava, String cppClassName,
            String cppBaseClassName, boolean outputCpp) {
		return new XmlFileTask(include, fileName, className, baseClassName,
                               outputJava, cppClassName, cppBaseClassName,
                               outputCpp);
	}

	FileTask createPropertiesTask(
			ResourceGenTask.Include include, String fileName) {
		return new PropertiesFileTask(include, fileName);
	}

	static abstract class FileTask {
		ResourceGenTask.Include include;
		String className;
		String fileName;

        String cppClassName;
    
        boolean outputJava;
        boolean outputCpp;

		abstract void process(ResourceGen generator) throws IOException;

		abstract protected void generateBaseJava(
				ResourceGen generator,
				ResourceDef.ResourceBundle resourceList, PrintWriter pw);


        abstract protected void generateCpp(
            ResourceGen generator, ResourceDef.ResourceBundle resourceList);

		/** XML source file, e.g. happy/BirthdayResource_en.xml **/
		File getFile() {
			return new File(include.root.src, fileName);
		}

        boolean checkUpToDate(ResourceGen generator, File file) {
			if (file.exists() &&
                file.lastModified() >= getFile().lastModified()) {
				generator.comment(file + " is up to date");
				return true;
			}
      
            return false;
        }

        void makeParentDirs(File file) {
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
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
            
            if (checkUpToDate(generator, file)) {
                return;
            }

			generator.comment("Generating " + file);
			final FileOutputStream out;
			try {
                makeParentDirs(file);

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

        protected void generateDoNotModifyHeader(PrintWriter pw) {
			pw.println("// This class is generated. Do NOT modify it, or");
			pw.println("// add it to source control.");
			pw.println();
        }

        protected void generateGeneratedByBlock(PrintWriter pw) {
			pw.println("/**");
			pw.println(" * This class was generated");
			pw.println(" * by " + ResourceGen.class);
			pw.println(" * from " + getFile());
			pw.println(" * on " + new Date().toString() + ".");
			pw.println(" * It contains a list of messages, and methods to");
			pw.println(" * retrieve and format those messages.");
			pw.println(" **/");
			pw.println();
        }

		protected void generateHeader(PrintWriter pw) {
            generateDoNotModifyHeader(pw);
			String packageName = getPackageName();
			if (packageName != null) {
				pw.println("package " + packageName + ";");
			}
			pw.println("import java.io.IOException;");
			pw.println("import java.util.Locale;");
			pw.println("import mondrian.resource.*;");
			pw.println();
            generateGeneratedByBlock(pw);
		}

		private void generateFooter(PrintWriter pw, String className) {
			pw.println("// End " + className + ".java");
		}

	}
}

// End ResourceGen.java
