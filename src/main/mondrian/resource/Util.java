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

/**
 * todo:
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class Util {

	private static final Throwable[] emptyThrowableArray = new Throwable[0];

    /** loads URL into Document and returns set of resources **/
	public static ResourceDef.BaflResourceList load(URL url)
		throws IOException
	{
		return load(url.openStream());
	}

	/** loads InputStream and returns set of resources **/
	static ResourceDef.BaflResourceList load(InputStream inStream)
		throws IOException
	{
		try {
			Parser parser = XOMUtil.createDefaultParser();
			DOMWrapper def = parser.parse(inStream);
			ResourceDef.BaflResourceList xmlResourceList = new
				ResourceDef.BaflResourceList(def);
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
	public static void fillText(
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

	public static URL stringToUrl(String strFile) throws IOException
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
	public static URL convertPathToURL(File file)
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

	public static String formatError(String template, Object[] args)
	{
		String s = template;
		for (int i = 0; i < args.length; i++) {
			String arg = args[i].toString();
			s = replace(s, "%" + (i + 1), arg);
			s = replace(s, "%i" + (i + 1), arg);
		}
		return s;
	}

	/** Does not modify the original string */
	public static String replace(String s,String find,String replace)
	{
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

	/**
	 * Converts a chain of {@link Throwable}s into an array.
	 **/
	public static Throwable[] toArray(Throwable err)
	{
		ArrayList list = new ArrayList();
		while (err != null) {
			list.add(err);
			if (err instanceof ChainableThrowable) {
				err = ((ChainableThrowable) err).getNextThrowable();
			} else {
				err = null;
			}
		}
		return (Throwable[]) list.toArray(emptyThrowableArray);
	}

	/**
	 * Formats an error, which may have chained errors, as a string.
	 */
	public static String toString(Throwable err)
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
}

// End Util.java
