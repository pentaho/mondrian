/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Andreas Voss, 22 March, 2002
*/
package mondrian.web.taglib;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;
import java.io.InputStream;
import java.util.HashMap;

/**
 * holds compiled stylesheets
 */

public class ApplResources implements Listener.ApplicationContext {

    private static final String ATTRNAME = "mondrian.web.taglib.ApplResources";
    private ServletContext context;

    /**
     * Creates a <code>ApplResources</code>. Only {@link Listener} calls this;
     * you should probably call {@link #getInstance}.
     */
    public ApplResources() {
    }

    /**
     * Retrieves the one and only instance of <code>ApplResources</code> in
     * this servlet's context.
     */
    public static ApplResources getInstance(ServletContext context) {
        return (ApplResources)context.getAttribute(ATTRNAME);
    }

    private HashMap templatesCache = new HashMap();
    public Transformer getTransformer(String xsltURI, boolean useCache) {
        try {
            Templates templates = null;
            if (useCache)
                templates = (Templates)templatesCache.get(xsltURI);
            if (templates == null) {
                TransformerFactory tf = TransformerFactory.newInstance();
                InputStream input = context.getResourceAsStream(xsltURI);
                templates = tf.newTemplates(new StreamSource(input));
                if (useCache)
                    templatesCache.put(xsltURI, templates);
            }
            return templates.newTransformer();
        }
        catch (TransformerConfigurationException e) {
            e.printStackTrace();
            throw new RuntimeException(e.toString());
        }
    }

    // implement ApplicationContext
    public void init(ServletContextEvent event) {
        this.context = event.getServletContext();
        context.setAttribute(ATTRNAME, this);
    }

    public void destroy(ServletContextEvent ev) {
    }


}

// End ApplResources.java
