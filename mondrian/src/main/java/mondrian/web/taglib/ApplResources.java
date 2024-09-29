/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.web.taglib;

import java.io.InputStream;
import java.util.HashMap;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamSource;

/**
 * Holds compiled stylesheets.
 *
 * @author Andreas Voss, 22 March, 2002
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
            if (useCache) {
                templates = (Templates)templatesCache.get(xsltURI);
            }
            if (templates == null) {
                TransformerFactory tf = TransformerFactory.newInstance();
                InputStream input = context.getResourceAsStream(xsltURI);
                templates = tf.newTemplates(new StreamSource(input));
                if (useCache) {
                    templatesCache.put(xsltURI, templates);
                }
            }
            return templates.newTransformer();
        } catch (TransformerConfigurationException e) {
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
