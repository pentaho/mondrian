/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.xom.wrappers;

import mondrian.xom.XOMException;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * A <code>JaxpDOMParser</code> implements {@link mondrian.xom.Parser} using
 * a {@link DocumentBuilder JAXP-compliant parser}.
 *
 * @author jhyde
 * @since Aug 29, 2002
 * @version $Id$
 **/
public class JaxpDOMParser extends GenericDOMParser {
    private DocumentBuilder builder;

    /** Creates a non-validating parser. **/
    public JaxpDOMParser() throws XOMException {
        this(false);
    }

    /** Creates a parser. **/
    public JaxpDOMParser(boolean validating) throws XOMException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(validating);
            try {
                factory.setAttribute(VALIDATION_FEATURE, new Boolean(validating));
                factory.setAttribute(LOAD_EXTERNAL_DTD_FEATURE, new Boolean(validating));
            } catch (IllegalArgumentException e) {
                // Weblogic 6.1's parser complains 'No arguments are
                // implemented'
            }
            builder = factory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new XOMException(e, "Error creating parser");
        } catch (FactoryConfigurationError e) {
            throw new XOMException(e, "Error creating parser");
        }
        builder.setErrorHandler(this);
        document = builder.newDocument();
    }

    protected Document parseInputSource(InputSource in) throws XOMException {
        prepareParse();
        try {
            Document document = builder.parse(in);
            handleErrors();
            return document;
        } catch (SAXException e) {
            // Display any pending errors
            handleErrors();
            throw new XOMException(e, "Document parse failed");
        } catch (IOException e) {
            // Display any pending errors
            handleErrors();
            throw new XOMException(e, "Document parse failed");
        }
    }
}

// End JaxpDOMParser.java
