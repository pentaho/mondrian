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
import mondrian.xom.XOMException;
import mondrian.xom.Parser;
import mondrian.xom.XOMUtil;
import mondrian.xom.DOMWrapper;

import java.io.*;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.rmi.RemoteException;

/**
 * Miscellaneous utility methods for the <code>mondrian.resource</code>
 * package, all them <code>static</code> and package-private.
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
abstract class Util {

	private static final Throwable[] emptyThrowableArray = new Throwable[0];

    /** loads URL into Document and returns set of resources **/
	static ResourceDef.ResourceBundle load(URL url)
		throws IOException
	{
		return load(url.openStream());
	}

	/** loads InputStream and returns set of resources **/
	static ResourceDef.ResourceBundle load(InputStream inStream)
		throws IOException
	{
		try {
			Parser parser = XOMUtil.createDefaultParser();
			DOMWrapper def = parser.parse(inStream);
			ResourceDef.ResourceBundle xmlResourceList = new
				ResourceDef.ResourceBundle(def);
			return xmlResourceList;
		} catch (XOMException err) {
			throw new IOException(err.toString());
		}
	}

	/**
	 * Left-justify a block of text.  Line breaks are preserved, but long lines
	 * are broken.
	 *
	 * @param pw where to output the formatted text
	 * @param text the text to be written
	 * @param linePrefix a string to prepend to each output line
	 * @param lineSuffix a string to append to each output line
	 * @param maxTextPerLine the maximum number of characters to place on
	 *        each line, not counting the prefix and suffix.  If this is -1,
	 *        never break lines.
	 **/
	static void fillText(
		PrintWriter pw, String text, String linePrefix, String lineSuffix,
		int maxTextPerLine)
	{
		int i = 0;
		for (;;) {
			int end = text.length();
			if (end <= i) {
				// Nothing left.  We're done.
				break;
			}

			if (i > 0) {
				// End the previous line and start another.
				pw.println(lineSuffix);
				pw.print(linePrefix);
			}

			int nextCR = text.indexOf("\r", i);
			if (nextCR >= 0 && nextCR < end) {
				end = nextCR;
			}
			int nextLF = text.indexOf("\n", i);
			if (nextLF >= 0 && nextLF < end) {
				end = nextLF;
			}

			if (maxTextPerLine > 0 && i + maxTextPerLine <= end) {
				// More than a line left.  Break at the last space before the
				// line limit.
				end = text.lastIndexOf(" ",i + maxTextPerLine);
				if (end < i) {
					// No space exists before the line limit; look beyond it.
					end = text.indexOf(" ",i);
					if (end < 0) {
						// No space anywhere in the line.  Take the whole line.
						end = text.length();
					}
				}
			}

			pw.print(text.substring(i, end));

			// The line is short enough.  Print it, and find where the next one
			// starts.
			i = end;
			while (i < text.length() &&
				   (text.charAt(i) == ' ' ||
					text.charAt(i) == '\r' ||
					text.charAt(i) == '\n')) {
				i++;
			}
		}
	}

	static URL stringToUrl(String strFile) throws IOException
	{
		try {
			File f = new File(strFile);
			return convertPathToURL(f);
		} catch (Throwable err) {
			throw new IOException(err.toString());
		}
	}

	/**
	 * Creates a file-protocol URL for the given filename.
	 **/
	static URL convertPathToURL(File file)
	{
		try {
			String path = file.getAbsolutePath();
			// This is a bunch of weird code that is required to
			// make a valid URL on the Windows platform, due
			// to inconsistencies in what getAbsolutePath returns.
			String fs = System.getProperty("file.separator");
			if (fs.length() == 1)
			{
				char sep = fs.charAt(0);
				if (sep != '/')
					path = path.replace(sep, '/');
				if (path.charAt(0) != '/')
					path = '/' + path;
			}
			path = "file://" + path;
			return new URL(path);
		} catch (MalformedURLException e) {
			throw new java.lang.Error(e.getMessage());
		}
	}

	static String formatError(String template, Object[] args)
	{
		String s = template;
		for (int i = 0; i < args.length; i++) {
			String arg = args[i].toString();
			s = replace(s, "%" + (i + 1), arg);
			s = replace(s, "%i" + (i + 1), arg);
		}
		return s;
	}

	/** Returns <code>s</code> with every instance of <code>find</code>
	 * converted to <code>replace</code>. */
	static String replace(String s,String find,String replace) {
		// let's be optimistic
		int found = s.indexOf(find);
		if (found == -1) {
			return s;
		}
		StringBuffer sb = new StringBuffer(s.length());
		int start = 0;
		for (;;) {
			for (; start < found; start++) {
				sb.append(s.charAt(start));
			}
			if (found == s.length()) {
				break;
			}
			sb.append(replace);
			start += find.length();
			found = s.indexOf(find,start);
			if (found == -1) {
				found = s.length();
			}
		}
		return sb.toString();
	}

	/** Return <code>val</code> in double-quotes, suitable as a string in a
	 * Java or JScript program.
	 *
	 * @param val the value
	 * @param nullMeansNull whether to print a null value as <code>null</code>
	 *   (the default), as opposed to <code>""</code>
	 */
	static String quoteForJava(String val,boolean nullMeansNull)
	{
		if (val == null) {
			return nullMeansNull ? "null" : "";
		}
		String s0;
		s0 = replace(val, "\\", "\\\\");
		s0 = replace(val, "\"", "\\\"");
		s0 = replace(s0, "\n\r", "\\n");
		s0 = replace(s0, "\n", "\\n");
		s0 = replace(s0, "\r", "\\r");
		return "\"" + s0 + "\"";
	}

	static String quoteForJava(String val)
	{
		return quoteForJava(val,true);
	}

	/**
	 * Returns a string quoted so that it can appear in a resource file.
	 */
	static String quoteForProperties(String val) {
		String s0;
		s0 = replace(val, "\\", "\\\\");
//		s0 = replace(val, "\"", "\\\"");
		s0 = replace(s0, "'", "''");
		s0 = replace(s0, "\n\r", "\\n");
		s0 = replace(s0, "\n", "\\n");
		s0 = replace(s0, "\r", "\\r");
		s0 = replace(s0, "\t", "\\t");
		return s0;
	}

	static final char fileSep = System.getProperty("file.separator").charAt(0);

	static String fileNameToClassName(String fileName, String suffix) {
		String s = fileName;
		s = removeSuffix(s, suffix);
		s = s.replace(fileSep, '.');
		s = s.replace('/', '.');
		int score = s.indexOf('_');
		if (score >= 0) {
			s = s.substring(0,score);
		}
		return s;
	}

	static String removeSuffix(String s, final String suffix) {
		if (s.endsWith(suffix)) {
			s = s.substring(0,s.length()-suffix.length());
		}
		return s;
	}


	/**
	 * Given <code>happy/BirthdayResource_en_US.xml</code>,
	 * returns the locale "en_US".
	 */
	static Locale fileNameToLocale(String fileName, String suffix) {
		String s = removeSuffix(fileName, suffix);
		int score = s.indexOf('_');
		if (score <= 0) {
			return null;
		} else {
			String localeName = s.substring(score + 1);
			return parseLocale(localeName);
		}
	}

	/**
	 * Parses 'localeName' into a locale.
	 */
	private static Locale parseLocale(String localeName) {
		int score1 = localeName.indexOf('_');
		String language, country = "", variant = "";
		if (score1 < 0) {
			language = localeName;
		} else {
			language = localeName.substring(0, score1);
			if (language.length() != 2) {
				return null;
			}
			int score2 = localeName.indexOf('_',score1 + 1);
			if (score2 < 0) {
				country = localeName.substring(score1 + 1);
				if (country.length() != 2) {
					return null;
				}
			} else {
				country = localeName.substring(score1 + 1, score2);
				if (country.length() != 2) {
					return null;
				}
				variant = localeName.substring(score2 + 1);
			}
		}
		return new Locale(language,country,variant);
	}

	/**
	 * Given "happy/BirthdayResource_fr_FR.properties" and ".properties",
	 * returns "happy/BirthdayResource".
	 */
	static String fileNameSansLocale(String fileName, String suffix) {
		String s = removeSuffix(fileName, suffix);
		// If there are directory names, start reading after the last one.
		int from = s.lastIndexOf(fileSep);
		if (from < 0) {
			from = 0;
		}
		while (from < s.length()) {
			// See whether the rest of the filename after the current
			// underscore is a valid locale name. If it is, return the
			// segment of the filename before the current underscore.
			int score = s.indexOf('_', from);
			Locale locale = parseLocale(s.substring(score+1));
			if (locale != null) {
				return s.substring(0,score);
			}
			from = score + 1;
		}
		return s;
	}

	/**
	 * Converts a chain of {@link Throwable}s into an array.
	 **/
	static Throwable[] toArray(Throwable err)
	{
		ArrayList list = new ArrayList();
		while (err != null) {
			list.add(err);
			err = getCause(err);
		}
		return (Throwable[]) list.toArray(emptyThrowableArray);
	}

	private static final Class[] emptyClassArray = new Class[0];

	private static Throwable getCause(Throwable err) {
		if (err instanceof ChainableThrowable) {
			return ((ChainableThrowable) err).getCause();
		}
		if (err instanceof InvocationTargetException) {
			return ((InvocationTargetException) err).getTargetException();
		}
		try {
			Method method = err.getClass().getMethod(
					"getCause", emptyClassArray);
			if (Throwable.class.isAssignableFrom(method.getReturnType())) {
				return (Throwable) method.invoke(err, new Object[0]);
			}
		} catch (NoSuchMethodException e) {
		} catch (SecurityException e) {
		} catch (IllegalAccessException e) {
		} catch (IllegalArgumentException e) {
		} catch (InvocationTargetException e) {
		}
		try {
			Method method = err.getClass().getMethod(
					"getNestedThrowable", emptyClassArray);
			if (Throwable.class.isAssignableFrom(method.getReturnType())) {
				return (Throwable) method.invoke(err, new Object[0]);
			}
		} catch (NoSuchMethodException e) {
		} catch (SecurityException e) {
		} catch (IllegalAccessException e) {
		} catch (IllegalArgumentException e) {
		} catch (InvocationTargetException e) {
		}
		return null;
	}

	/**
	 * Formats an error, which may have chained errors, as a string.
	 */
	static String toString(Throwable err)
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		Throwable[] throwables = toArray(err);
		for (int i = 0; i < throwables.length; i++) {
			Throwable throwable = throwables[i];
			if (i > 0) {
				pw.println();
				pw.print("Caused by: ");
			}
			if (throwable instanceof ChainableThrowable) {
				pw.print(throwable.getMessage());
				pw.print(" at ");
				throwable.printStackTrace(pw);
			} else {
				pw.print(throwable.toString());
			}
		}
		return sw.toString();
	}

	static void printStackTrace(Throwable throwable, PrintWriter s) {
		Throwable[] stack = Util.toArray(throwable);
		PrintWriter pw = new DummyPrintWriter(s);
		for (int i = 0; i < stack.length; i++) {
			if (i > 0) {
				pw.println("caused by");
			}
			stack[i].printStackTrace(pw);
		}
		pw.flush();
	}

	static void printStackTrace(Throwable throwable, PrintStream s) {
		Throwable[] stack = Util.toArray(throwable);
		PrintStream ps = new DummyPrintStream(s);
		for (int i = 0; i < stack.length; i++) {
			if (i > 0) {
				ps.println("caused by");
			}
			stack[i].printStackTrace(ps);
		}
		ps.flush();
	}

	/**
	 * So we know to avoid recursively calling {@link printStackTrace(PrintWriter)}.
	 */
	static class DummyPrintWriter extends PrintWriter {
		public DummyPrintWriter(Writer out) {
			super(out);
		}
	}

	/**
	 * So we know to avoid recursively calling {@link printStackTrace(PrintStream)}.
	 */
	static class DummyPrintStream extends PrintStream {
		public DummyPrintStream(OutputStream out) {
			super(out);
		}
	}

}

// End Util.java
