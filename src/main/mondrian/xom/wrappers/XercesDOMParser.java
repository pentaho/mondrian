/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// klo, 1 August, 2001
*/

package mondrian.xom.wrappers;

import java.io.*;
import java.net.URL;
import mondrian.xom.XOMException;
import mondrian.xom.DOMWrapper;

// Imports used for XERCES parser
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.InputSource;

/**
* This private helper class presents a DOMParser using Xerces, with
* simple error handling appropriate for a testing environment.
*/
	
public class XercesDOMParser implements ErrorHandler, mondrian.xom.Parser
{
	org.apache.xerces.parsers.DOMParser parser = 
	new org.apache.xerces.parsers.DOMParser();

	org.apache.xerces.dom.DocumentImpl document =
	new org.apache.xerces.dom.DocumentImpl();

	// Used for capturing error messages as they occur.
	StringWriter errorBuffer = null;
	PrintWriter errorOut = null;

	/**
	 * Constructs a new Xerces DOM Parser.
	 */
	public XercesDOMParser() 
		throws XOMException
	{
		this(false);
	}

	/**
	 * Constructs a new Xerces DOM Parser.
	 * @param validate set to true to enable validation, false otherwise.
	 */
	public XercesDOMParser(boolean validate) 
		throws XOMException
	{
		//@@ Always disable validation in XERCES for now
		try {
			parser.setFeature(
				"http://xml.org/sax/features/validation", 
				false);
			parser.setFeature(
				"http://apache.org/xml/features/nonvalidating/load-external-dtd",
				false);
		} catch (SAXException e) {
			throw new XOMException(e, "Error setting up validation");
		}

		parser.setErrorHandler(this);			
	}		

	/** Warning. */
	public void warning(SAXParseException ex) 
	{
		errorOut.println("[Warning] "+
						 getLocationString(ex)+": "+
						 ex.getMessage());
	}

	/** Error. */
	public void error(SAXParseException ex) 
	{
		errorOut.println("[Error] "+
						 getLocationString(ex)+": "+
						 ex.getMessage());
	}

	/** Fatal error. */
	public void fatalError(SAXParseException ex) 
		throws SAXException 
	{
		errorOut.println("[Fatal Error] "+
						 getLocationString(ex)+": "+
						 ex.getMessage());
		throw ex;
	}
		
	/** Returns a string of the location. */
	private String getLocationString(SAXParseException ex) 
	{
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
	public DOMWrapper parse(String sXml)
		throws XOMException 
	{
		org.w3c.dom.Document doc = parse(new InputSource(new StringReader(sXml)));
		return new W3CDOMWrapper(doc.getDocumentElement());
	}

	// implement Parser
	public DOMWrapper parse(InputStream is)
		throws XOMException 
	{
		org.w3c.dom.Document doc = parse(new InputSource(is));
		return new W3CDOMWrapper(doc.getDocumentElement());
	}

	// implement Parser
	public DOMWrapper parse(URL url)
		throws XOMException 
	{
		try {
			return parse(new BufferedInputStream(url.openStream()));
		} catch (IOException ex) {
			throw new XOMException(ex, "Document parse failed");
		}
	}

	// implement Parser
	public DOMWrapper create(String tagName)
	{
		org.w3c.dom.Node node = document.createElement(tagName);
		return new W3CDOMWrapper(node);
	}

	/** Parses the specified URI and returns the document. */
	public org.w3c.dom.Document parse(InputSource in)
		throws XOMException 
	{
		prepareParse();
		try {
			parser.parse(in);
		} catch (SAXException ex) {
			// Display any pending errors
			handleErrors();
			throw new XOMException(ex, "Document parse failed");
		} catch (IOException ex) {
			// Display any pending errors
			handleErrors();
			throw new XOMException(ex, "Document parse failed");
		}
			
		handleErrors();
		return parser.getDocument();
	}

	// Helper: reset the error buffer to prepare for a new parse.
	private void prepareParse()
	{
		errorBuffer = new StringWriter();
		errorOut = new PrintWriter(errorBuffer);
	}

	// Helper: throw an exception with messages of any errors
	// accumulated during the parse.
	private void handleErrors()
		throws XOMException
	{
		errorOut.flush();
		String errorStr = errorBuffer.toString();
		if(errorStr.length() > 0)
			throw new XOMException("Document parse failed:"
								   + errorStr);
	}
}



// End XercesDOMParser.java
