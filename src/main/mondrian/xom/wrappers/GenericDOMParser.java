/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.xom.wrappers;

import mondrian.xom.DOMWrapper;
import mondrian.xom.XOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.*;
import java.net.URL;

/**
 * A <code>GenericDOMParser</code> is an abstract base class for {@link
 * XercesDOMParser} and {@link JaxpDOMParser}.
 *
 * @author jhyde
 * @since Aug 29, 2002
 * @version $Id$
 **/
abstract class GenericDOMParser implements ErrorHandler, mondrian.xom.Parser {

	// Used for capturing error messages as they occur.
	StringWriter errorBuffer = null;
	PrintWriter errorOut = null;
	/** The document which spawns elements. The constructor of the derived
	 * class must set this. **/
	protected Document document;

	static final String LOAD_EXTERNAL_DTD_FEATURE =
			"http://apache.org/xml/features/nonvalidating/load-external-dtd";
	static final String VALIDATION_FEATURE =
			"http://xml.org/sax/features/validation";

	// implement Parser
	public DOMWrapper create(String tagName) {
		Element element = document.createElement(tagName);
		return new W3CDOMWrapper(element);
	}

	// implement Parser
	public DOMWrapper parse(InputStream is) throws XOMException {
		InputSource source = new InputSource(is);
		Document document = parseInputSource(source);
		return new W3CDOMWrapper(document.getDocumentElement());
	}

	// implement Parser
	public DOMWrapper parse(String xmlString) throws XOMException {
		return parse(new StringReader(xmlString));
	}

	// todo: add this to Parser interface
	public DOMWrapper parse(Reader reader) throws XOMException {
		Document document = parseInputSource(new InputSource(reader));
		return new W3CDOMWrapper(document);
	}

	/** Parses the specified URI and returns the document. */
	protected abstract Document parseInputSource(InputSource in)
			throws XOMException;

	/** Warning. */
	public void warning(SAXParseException ex) {
		errorOut.println("[Warning] " +
				getLocationString(ex) + ": " +
				ex.getMessage());
	}

	/** Error. */
	public void error(SAXParseException ex) {
		errorOut.println("[Error] " +
				getLocationString(ex) + ": " +
				ex.getMessage());
	}

	/** Fatal error. */
	public void fatalError(SAXParseException ex)
			throws SAXException {
		errorOut.println("[Fatal Error] " +
				getLocationString(ex) + ": " +
				ex.getMessage());
		throw ex;
	}

	/** Returns a string of the location. */
	private String getLocationString(SAXParseException ex) {
		StringBuffer str = new StringBuffer();

		String systemId = ex.getSystemId();
		if (systemId != null) {
			int index = systemId.lastIndexOf('/');
			if (index != -1)
				systemId = systemId.substring(index + 1);
			str.append(systemId);
		}
		str.append(':');
		str.append(ex.getLineNumber());
		str.append(':');
		str.append(ex.getColumnNumber());
		return str.toString();
	}

	// implement Parser
	public DOMWrapper parse(URL url)
			throws XOMException {
		try {
			return parse(new BufferedInputStream(url.openStream()));
		} catch (IOException ex) {
			throw new XOMException(ex, "Document parse failed");
		}
	}

	// Helper: reset the error buffer to prepare for a new parse.
	protected void prepareParse() {
		errorBuffer = new StringWriter();
		errorOut = new PrintWriter(errorBuffer);
	}

	// Helper: throw an exception with messages of any errors
	// accumulated during the parse.
	protected void handleErrors() throws XOMException {
		errorOut.flush();
		String errorStr = errorBuffer.toString();
		if (errorStr.length() > 0) {
			throw new XOMException("Document parse failed: " + errorStr);
		}
	}
}

// End GenericDOMParser.java
