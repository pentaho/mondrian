/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 October, 2001
*/

package mondrian.xom;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.IOException;

/**
 * Utilities for dealing with XML data.  These methods must NOT depend upon any
 * XML parser or object model (MSXML, DOM, SAX, etc.)
 *
 * @author jhyde
 * @since 3 October, 2001
 * @version $Id$
 **/
public class XMLUtil {

	/**
	 * Determine if a String contains any XML special characters, return true
	 * if it does.  If this function returns true, the string will need to be
	 * encoded either using the stringEncodeXML function above or using a
	 * CDATA section.  Note that MSXML has a nasty bug whereby whitespace
	 * characters outside of a CDATA section are lost when parsing.  To
	 * avoid hitting this bug, this method treats many whitespace characters
	 * as "special".
	 * @param input the String to scan for XML special characters.
	 * @return true if the String contains any such characters.
	 */
	public static boolean stringHasXMLSpecials(String input)
	{
		for(int i=0; i<input.length(); i++) {
			char c = input.charAt(i);
			switch(c) {
			case '<':
			case '>':
			case '"':
			case '\'':
			case '&':
			case '\t':
			case '\n':
			case '\r':
				return true;
			}
		}
		return false;
	}

	/**
	 * Encode a String for XML output, displaying it to a PrintWriter.
	 * The String to be encoded is displayed, except that
	 * special characters are converted into entities.
	 * @param input a String to convert.
	 * @param out a PrintWriter to which to write the results.
	 */
	public static void stringEncodeXML(String input, PrintWriter out)
	{
		for(int i=0; i<input.length(); i++) {
			char c = input.charAt(i);
			switch(c) {
			case '<':
			case '>':
			case '"':
			case '\'':
			case '&':
			case '\t':
			case '\n':
			case '\r':
				out.print("&#" + (int)c + ";");
				break;
			default:
				out.print(c);
			}
		}		
	}

	/**
	 * Quote a string, and write to a {@link PrintWriter}.
	 *
	 * <pFor example, <code>"a string"</code> becomes <code>&lt![CDATA[a
	 * string]]&gt;</code>.  If the string contains ']]&gt;' (which commonly
	 * occurs when wrapping other XML documents), we give up on using
	 * <code>&lt![CDATA[</code> ... <code>]]&gt;</code>, and just encode the
	 * string.  For example, <code>A string with ]]&gt; in it</code> becomes
	 * <code>A string with ]]&amp;&gt; in it</code>.</p>
	 **/
	public static void printPCDATA(PrintWriter pw, String data)
	{
		if (data.indexOf("]]>") > -1) {
			String s = StringEscaper.xmlEscaper.escapeString(data);
			pw.print(s);
		} else {
			pw.print("<![CDATA[");
			pw.print(data);
			pw.print("]]>");
		}
	}

	/**
	 * Quote a string.
	 *
	 * @see #printPCDATA(PrintWriter,String)
	 **/
	public static String quotePCDATA(String data)
	{
		if (data.indexOf("]]>") > -1) {
			return StringEscaper.xmlEscaper.escapeString(data);
		} else {
			return "<![CDATA[" + data + "]]>";
		}
	}

	/**
	 * Quote a string in an element and a CDATA, and write to a {@link
	 * PrintWriter}.  For example, it <code>tag</code> is "Value", then
	 * <code>"a string"</code> becomes <code>&ltValue&gt;&lt![CDATA[a
	 * string]]&gt;&lt/Value&gt;.
	 *
	 * @param newline whether to print a newline after the element
	 * @see #printPCDATA(PrintWriter,String)
	 **/
	public static void printPCDATA(
		PrintWriter pw, String tag, String data, boolean newline)
	{
		if (data == null || data.length() == 0) {
			return;
		}
		pw.print("<");
		pw.print(tag);
		pw.print(">");
		printPCDATA(pw,data);
		pw.print("</");
		pw.print(tag);
		pw.print(">");
		if (newline) {
			pw.println();
		}
	}

	public static void printPCDATA(PrintWriter pw, String tag, String data)
	{
		boolean newline = false;
		printPCDATA(pw, tag, data, newline);
	}

	private static String escapeForQuoting(String val)
	{
		return StringEscaper.xmlNumericEscaper.escapeString(val);
	}

	/** Quote a string so that it can be included as an XML attribute value. */
	public static String quoteAtt(String val)
	{
		return "\"" + escapeForQuoting(val) + "\"";
	}

	/** Return an XML attribute/value pair for String val */
	public static String quoteAtt(String name, String val)
	{
		if ((val == null) || val.equals(""))
			return "";
		return " " + name + "=" + quoteAtt(val);
	}

	/** Return an XML attribute/value pair for int val */
	public static String quoteAtt(String name, int val)
	{
		return " " + name + "=\"" + val + "\"";
	}

	/** Return an XML attribute/value pair for boolean val */
	public static String quoteAtt(String name, boolean val)
	{
		return " " + name + "=\"" + (val ? "TRUE" : "FALSE") + "\"";
	}

	/** Quote a string so that it can be included as an XML attribute value. */
	public static void printAtt(PrintWriter pw, String val)
	{
		pw.print("\"");
		pw.print(escapeForQuoting(val));
		pw.print("\"");
	}

	/** Print an XML attribute name and value for string val */
	public static void printAtt(PrintWriter pw, String name, String val)
	{
		if (val != null /* && !val.equals("") */) {
			pw.print(" ");
			pw.print(name);
			pw.print("=\"");
			pw.print(escapeForQuoting(val));
			pw.print("\"");
		}
	}

	/** Print an XML attribute name and value for int val */
	public static void printAtt(PrintWriter pw, String name, int val)
	{
		pw.print(" ");
		pw.print(name);
		pw.print("=\"");
		pw.print(val);
		pw.print("\"");
	}

	/** Print an XML attribute name and value for boolean val */
	public static void printAtt(PrintWriter pw, String name, boolean val)
	{
		pw.print(" ");
		pw.print(name);
		pw.print(val ? "=\"true\"" : "=\"false\"");
	}

	/**
	 * Retrieve the name of the first tag in the XML document specified by the
	 * given Reader, without parsing the full file/string.  This function is
	 * useful to identify the DocType of an XML document before parsing,
	 * possibly to send the document off to different pieces of code.
	 * For performance reasons, the function attempts to read as little of
	 * the file or string as possible before making its decision about the
	 * first tag.  Leading comments are ignored.
	 * @param xml a Reader containing an XML document.
	 * @return the first tag name, as a String, or null if no first tag
	 * can be found.
	 */
	public static String getFirstTagName(Reader xml)
	{
		final int OUTSIDE = 0;  // constant: identify outside state
		final int BRACKET = 1;  // constant: bracket, contents unknown
		final int COMMENT = 2;  // constant: identify a comment section
		final int IGNORE = 3;   // constant: identify an ignored section
		final int TAG = 4;      // constant: identify a tag section
		
		int state = OUTSIDE;
		String commentMatch = null;
		StringBuffer tagBuffer = null;
		boolean sawBang = false;

		try {
			int c = xml.read();
			for(;;) {
				// No tag found if we hit EOF first.
				if(c == -1)
					return null;

				switch(state) {
				case OUTSIDE:
					// Start of any sort of tag
					if(c == '<') {
						state = BRACKET;
						commentMatch = "!--";
						sawBang = false;
						c = xml.read();
					}

					// Other non-whitespace characters outside of any tag
					else if(!Character.isWhitespace((char)c))
						return null;

					// Whitespace characters are ignored
					else
						c = xml.read();
					break;

				case BRACKET:
					// Check for the start of a comment.
					if (commentMatch != null) {
						if(c == commentMatch.charAt(0)) {
							// This match indicates a comment
							if(commentMatch.length() == 1) {
								c = xml.read();
								commentMatch = "-->";
								state = COMMENT;
							} 
							else {
								// Remove the first character from commentMatch,
								// then process the character as usual.
								commentMatch = 
									commentMatch.substring(1, commentMatch.length());
							}
						}
						else
							// No longer eligible for comment.
							commentMatch = null;					
					} 

					// Hit whitespace; ignore the character.
					if(Character.isWhitespace((char)c)) {
						c = xml.read();
						break;
					}

					switch(c) {
					case '?':
						c = xml.read();
						state = IGNORE;
						break;
					case '!':
						// Enter an ignored section unless eligible for comment.
						c = xml.read();
						sawBang = true;
						if(commentMatch == null)
							state = IGNORE;
						break;
					case '-':
						// Enter an ignored section unless eligible for comment.
						c = xml.read();
						if(commentMatch == null)
							state = IGNORE;
						break;
					case '>':
						// Return to OUTSIDE state immediately
						c = xml.read();
						state = OUTSIDE;
						break;
					default:
						// State depends on whether we saw a ! or not.
						if(sawBang)
							state = IGNORE;
						else
							state = TAG;
						tagBuffer = new StringBuffer();
					}
					break;
				
				case COMMENT:
					// Did we match the next expected end-of-comment character?
					if(c == commentMatch.charAt(0)) {
						c = xml.read();
						if(commentMatch.length() == 1)
							// Done with the comment
							state = OUTSIDE;
						else
							commentMatch = 
								commentMatch.substring(1, commentMatch.length());
					} 

					// If not, restart our quest for the end-of-comment character.
					else {
						c = xml.read();
						commentMatch = "-->";
					}
					break;				

				case IGNORE:
					// Drop out on a close >.  Ignore all other characters.
					if(c == '>') {
						c = xml.read();
						state = OUTSIDE;
					}
					else
						c = xml.read();
					break;

				case TAG:
					// Store characters in the tag buffer until we hit whitespace.
					// When we hit whitespace or '>' or '/', return the name of the tag.
					if(Character.isWhitespace((char)c) || c == '>'
						|| c == '/')
						return tagBuffer.toString();
					else {
						tagBuffer.append((char)c);
						c = xml.read();
					}				
					break;
				}
			}			
		} catch (IOException ex) {
			// On exception, we can't determine the first tag, so return null.
			return null;
		}		
	}	
}


// End XOMUtil.java
