/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// dsommerfield, 12 December, 2000
*/

package mondrian.xom;

import java.util.Vector;
import java.io.*;

/**
 * XMLOutput is a class which implements streaming XML output.  Use this class
 * to write XML to any streaming source.  While the class itself is
 * unstructured and doesn't enforce any DTD specification, use of the class
 * does ensure that the output is syntactically valid XML.
 */
public class XMLOutput {

	// This Writer is the underlying output stream to which all XML is
	// written.
	private PrintWriter out;

	// The tagStack is maintained to check that tags are balanced.
	private Vector tagStack;

	// The class maintains an indentation level to improve output quality.
	private int indent;

	// The class also maintains the total number of tags written.  This
	// is used to monitor changes to the output
	private int tagsWritten;

	// This flag is set to true if the output should be compacted.
	// Compacted output is free of extraneous whitespace and is designed
	// for easier transport.
	private boolean compact;

	/** @see setIndentString **/
	private String indentString = "\t";

	/** @see setGlob **/
	private boolean glob;

	/**
	 * Whether we have started but not finished a start tag. This only happens
	 * if <code>glob</code> is true. The start tag is automatically closed
	 * when we start a child node. If there are no child nodes, {@link #endTag}
	 * creates an empty tag.
	 **/
	private boolean inTag;

	/** @see #setAlwaysQuoteCData */
	private boolean alwaysQuoteCData;

	/** @see #setIgnorePcdata **/
	private boolean ignorePcdata;

	/**
	 * Private helper function to display a degree of indentation
	 * @param out the PrintWriter to which to display output.
	 * @param indent the degree of indentation.
	 */
	private void displayIndent(PrintWriter out, int indent)
	{
		if(!compact) {
			for (int i = 0; i < indent; i++) {
				out.print(indentString);
			}
		}
	}	

	/**
	 * Constructs a new XMLOutput based on any Writer.
	 * @param out the writer to which this XMLOutput generates results.
	 */
	public XMLOutput(Writer out) 
	{
		this.out = new PrintWriter(out, true);
		indent = 0;
		tagsWritten = 0;
		tagStack = new Vector();
	}

	/**
	 * Sets or unsets the compact mode.  Compact mode causes the generated
	 * XML to be free of extraneous whitespace and other unnecessary
	 * characters.
	 * @param comp true to turn on compact mode, or false to turn it off.
	 */
	public void setCompact(boolean compact)
	{
		this.compact = compact;
	}

	public boolean getCompact()
	{
		return compact;
	}

	/**
	 * Sets the string to print for each level of indentation. The default is a
	 * tab. The value must not be <code>null</code>. Set this to the empty
	 * string to achieve no indentation (note that <code>{@link
	 * #setCompact}(true)</code> removes indentation <em>and</em> newlines).
	 **/
	public void setIndentString(String indentString)
	{
		this.indentString = indentString;
	}

	/**
	 * Sets whether to detect that tags are empty.
	 **/
	public void setGlob(boolean glob)
	{
		this.glob = glob;
	}

	/**
	 * Sets whether to always quote cdata segments (even if they don't contain
	 * special characters).
	 **/
	public void setAlwaysQuoteCData(boolean alwaysQuoteCData)
	{
		this.alwaysQuoteCData = alwaysQuoteCData;
	}

	/**
	 * Sets whether to ignore unquoted text, such as whitespace.
	 **/
	public void setIgnorePcdata(boolean ignorePcdata)
	{
		this.ignorePcdata = ignorePcdata;
	}

	public boolean getIgnorePcdata()
	{
		return ignorePcdata;
	}

	/**
	 * Sends a string directly to the output stream, without escaping any
	 * characters.  Use with caution!
	 **/
	public void print(String s)
	{
		out.print(s);
	}

	/**
	 * Start writing a new tag to the stream.  The tag's name must be given and
	 * its attributes should be specified by a fully constructed AttrVector
	 * object.
	 * @param tagName the name of the tag to write.
	 * @param attributes an XMLAttrVector containing the attributes to include
	 * in the tag.
	 */
	public void beginTag(String tagName, XMLAttrVector attributes)
	{
		beginBeginTag(tagName);
		if (attributes != null) {
			attributes.display(out, indent);
		}
		endBeginTag(tagName);
	}

	public void beginBeginTag(String tagName)
	{
		if (inTag) {
			// complete the parent's start tag
			if (compact) {
				out.print(">");
			} else {
				out.println(">");
			}
			inTag = false;
		}
		displayIndent(out, indent);
		out.print("<");
		out.print(tagName);
	}

	public void endBeginTag(String tagName)
	{
		if (glob) {
			inTag = true;
		} else if (compact) {
			out.print(">");
		} else {
			out.println(">");
		}
		out.flush();
		tagStack.addElement(tagName);
		indent++;
		tagsWritten++;
	}

	/**
	 * Write an attribute.
	 **/
	public void attribute(String name, String value)
	{
		XMLUtil.printAtt(out, name, value);
	}

	/**
	 * If we are currently inside the start tag, finish it off.
	 **/
	public void beginNode()
	{
		if (inTag) {
			// complete the parent's start tag
			if (compact) {
				out.print(">");
			} else {
				out.println(">");
			}
			inTag = false;
		}
	}

	/**
	 * Complete a tag.  This outputs the end tag corresponding to the
	 * last exposed beginTag.  The tag name must match the name of the
	 * corresponding beginTag.
	 * @param tagName the name of the end tag to write.
	 */
	public void endTag(String tagName)
	{
		// Check that the end tag matches the corresponding start tag
		int stackSize = tagStack.size();
		String matchTag = (String)(tagStack.elementAt(stackSize-1));
		if(!tagName.equalsIgnoreCase(matchTag))
			throw new AssertFailure(
				"End tag <" + tagName + "> does not match " +
				" start tag <" + matchTag + ">");
		tagStack.removeElementAt(stackSize-1);		

		// Lower the indent and display the end tag
		indent--;
		if (inTag) {
			// we're still in the start tag -- this element had no children
			if (compact) {
				out.print("/>");
			} else {
				out.println("/>");
			}
			inTag = false;
		} else {
			displayIndent(out, indent);
			out.print("</");
			out.print(tagName);
			if (compact) {
				out.print(">");
			} else {
				out.println(">");
			}
		}
		out.flush();
	}	

	/**
	 * Write an empty tag to the stream.  An empty tag is one with no
	 * tags inside it, although it may still have attributes.
	 * @param tagName the name of the empty tag.
	 * @param attributes an XMLAttrVector containing the attributes to
	 * include in the tag.
	 */
	public void emptyTag(String tagName, XMLAttrVector attributes)
	{
		if (inTag) {
			// complete the parent's start tag
			if (compact) {
				out.print(">");
			} else {
				out.println(">");
			}
			inTag = false;
		}
		displayIndent(out, indent);
		out.print("<");
		out.print(tagName);
		if(attributes != null) {
			out.print(" ");
			attributes.display(out, indent);
		}

		if(compact)
			out.print("/>");
		else
			out.println("/>");
		out.flush();
		tagsWritten++;
	}

	/**
	 * Write a CDATA section.  Such sections always appear on their own line.
	 * The nature in which the CDATA section is written depends on the actual
	 * string content with respect to these special characters/sequences:
	 * <ul>
	 * <li><code>&amp;</code>
	 * <li><code>&quot;</code>
	 * <li><code>&apos;</code>
	 * <li><code>&lt;</code>
	 * <li><code>&gt;</code>
	 * </ul>
	 * Additionally, the sequence <code>]]&gt;</code> is special.
	 * <ul>
	 * <li>Content containing no special characters will be left as-is.
	 * <li>Content containing one or more special characters but not the
	 * sequence <code>]]&gt;</code> will be enclosed in a CDATA section.
	 * <li>Content containing special characters AND at least one
	 * <code>]]&gt;</code> sequence will be left as-is but have all of its
	 * special characters encoded as entities.
	 * </ul>
	 * These special treatment rules are required to allow cdata sections
	 * to contain XML strings which may themselves contain cdata sections.
	 * Traditional CDATA sections <b>do not nest</b>.
	 */
	public void cdata(String data)
	{
		cdata(data, false);
	}

	/**
	 * Writes a CDATA section (as {@link #cdata(String)}).
	 *
	 * @param data string to write
	 * @param quote if true, quote in a <code>&lt;![CDATA[</code>
	 *        ... <code>]]&gt;</code> regardless of the content of
	 *        <code>data</code>; if false, quote only if the content needs it
	 **/
	public void cdata(String data, boolean quote)
	{
		if (inTag) {
			// complete the parent's start tag
			if (compact) {
				out.print(">");
			} else {
				out.println(">");
			}
			inTag = false;
		}
		if (data == null) {
			data = "";
		}
		boolean specials = false;
		boolean cdataEnd = false;

		// Scan the string for special characters
		// If special characters are found, scan the string for ']]>'
		if(XOMUtil.stringHasXMLSpecials(data)) {
			specials = true;
			if(data.indexOf("]]>") > -1)
				cdataEnd = true;
		}
		
		// Display the result
		displayIndent(out, indent);
		if (quote || alwaysQuoteCData) {
			out.print("<![CDATA[");
			out.print(data);
			out.println("]]>");
		} else if (!specials && !cdataEnd) {
			out.print(data);
		} else {
			XMLUtil.stringEncodeXML(data, out);
		}
		
		out.flush();
		tagsWritten++;
	}

	/**
	 * Write a String tag; a tag containing nothing but a CDATA section.
	 */
	public void stringTag(String name, String data)
	{
		beginTag(name, null);
		cdata(data);
		endTag(name);
	}	

	/**
	 * Write content.
	 */
	public void content(String content)
	{
		if(content != null) {
			indent++;
			LineNumberReader in = new LineNumberReader(new StringReader(content));
			try {
				String line;
				while((line = in.readLine()) != null) {
					displayIndent(out, indent);
					out.println(line);
				}
			} catch (IOException ex) {
				throw new AssertFailure(ex);
			}
			indent--;
			out.flush();
		}
		tagsWritten++;
	}
	
	/**
	 *  Write header. Use default version 1.0.
	 */
	public void header()
	{
		out.println("<? xml version=\"1.0\" ?>");
		out.flush();
		tagsWritten++;
	}
	
	/**
	 * Write header, take version as input.
	 */
	public void header(String version)
	{
		out.print("<? xml version=\"");
		out.print(version);
		out.println("\" ?>");
		out.flush();
		tagsWritten++;
	}

	/**
	 * Get the total number of tags written
	 * @return the total number of tags written to the XML stream.
	 */
	public int numTagsWritten()
	{
		return tagsWritten;		
	}
	
}


// End XMLOutput.java
