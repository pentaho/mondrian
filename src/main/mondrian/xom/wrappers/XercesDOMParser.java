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

import mondrian.xom.DOMWrapper;
import mondrian.xom.XOMException;
import org.apache.xerces.dom.DocumentImpl;
import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

/**
 * This private helper class presents a GenericDOMParser using Xerces, with
 * simple error handling appropriate for a testing environment.
 */

public class XercesDOMParser extends GenericDOMParser {
	private DOMParser parser;

	/**
	 * Constructs a non-validating Xerces DOM Parser.
	 */
	public XercesDOMParser() throws XOMException {
		this(false);
	}

	/**
	 * Constructs a Xerces DOM Parser.
	 * @param validate whether to enable validation
	 */
	public XercesDOMParser(boolean validate) throws XOMException {
		parser = new DOMParser();
		try {
			if (!validate) {
				parser.setFeature(VALIDATION_FEATURE, false);
				parser.setFeature(LOAD_EXTERNAL_DTD_FEATURE, false);
			}
		} catch (SAXException e) {
			throw new XOMException(e, "Error setting up validation");
		}

		parser.setErrorHandler(this);
		document = new DocumentImpl();
	}

	// implement GenericDOMParser
	protected Document parseInputSource(InputSource in) throws XOMException {
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

	// implement Parser
	public DOMWrapper create(String tagName) {
		Node node = document.createElement(tagName);
		return new W3CDOMWrapper(node);
	}
}

// End XercesDOMParser.java
