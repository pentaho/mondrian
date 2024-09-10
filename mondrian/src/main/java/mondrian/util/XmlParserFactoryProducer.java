/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara
// All Rights Reserved.
*/
package mondrian.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dom4j.io.SAXReader;
import org.xml.sax.EntityResolver;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

/**
 * Class was created to prevent XXE Security Vulnerabilities
 * http://jira.pentaho.com/browse/PPP-3506
 *
 * Created by Yury_Bakhmutski on 10/21/2016.
 */
public class XmlParserFactoryProducer {
    private static final Log logger =
        LogFactory.getLog(XmlParserFactoryProducer.class);

    /**
     * Creates an instance of {@link SAXParserFactory} class with enabled {@link XMLConstants#FEATURE_SECURE_PROCESSING} property.
     * Enabling this feature prevents from some XXE attacks (e.g. XML bomb)
     *
     * @throws ParserConfigurationException if a parser cannot
     *     be created which satisfies the requested configuration.
     *
     * @throws SAXNotRecognizedException When the underlying XMLReader does
     *            not recognize the property name.
     *
     * @throws SAXNotSupportedException When the underlying XMLReader
     *            recognizes the property name but doesn't support the
     *            property.
     */
    public static SAXParserFactory createSecureSAXParserFactory()
            throws SAXNotSupportedException, SAXNotRecognizedException,
            ParserConfigurationException
    {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        return factory;
    }

}

// End XmlParserFactoryProducer.java
