/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/

package mondrian.web.taglib;

import org.w3c.dom.Document;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * A <code>TransformTag</code> renders the result of a {@link ResultCache}
 * object. Example:<blockquote>
 *
 * <pre>The current slicer is
 * &lt;transform query="query1"
 *    xsltURI="/WEB-INF/mdxslicer.xsl"
 *    xsltCache="true"/&gt;
 * &lt;br/&gt;
 * &lt;transform query="query1"
 *    xsltURI="/WEB-INF/mdxtable.xsl"
 *    xsltCache="false"/&gt;</pre>
 *
 * </blockquote>
 *
 * Attributes are
 * {@link #setQuery query},
 * {@link #setXsltURI xsltURI},
 * {@link #setXsltCache xsltCache}.
 *
 * @author Andreas Voss, 22 March, 2002
 */
public class TransformTag extends TagSupport {

    public TransformTag() {
    }

    public int doEndTag() throws javax.servlet.jsp.JspException {
        try {
            ApplResources ar =
                ApplResources.getInstance(pageContext.getServletContext());
            ResultCache rc =
                ResultCache.getInstance(
                    pageContext.getSession(),
                    pageContext.getServletContext(),
                    query);
            Document doc = rc.getDOM();
            // DomBuilder.debug(doc);
            Transformer transformer = ar.getTransformer(xsltURI, xsltCache);
            transformer.transform(
                new DOMSource(doc), new StreamResult(pageContext.getOut()));
        } catch (Exception e) {
            e.printStackTrace();
            throw new JspException(e);
        }
        return EVAL_PAGE;
    }

    /** Sets the string attribute <code>query</code>, which is the name of a
     * query declared using the {@link QueryTag &lt;query&gt;} tag. */
    public void setQuery(String newQuery) {
        query = newQuery;
    }
    public String getQuery() {
        return query;
    }

    /** Sets the string attribute <code>xsltURI</code>, which is the URI of an
     * XSL style-sheet to transform query output. */
    public void setXsltURI(String newXsltURI) {
        xsltURI = newXsltURI;
    }
    public String getXsltURI() {
        return xsltURI;
    }

    /** Sets the boolean attribute <code>xsltCache</code>, which determines
     * whether to cache the parsed representation of an XSL style-sheet. */
    public void setXsltCache(boolean newXsltCache) {
        xsltCache = newXsltCache;
    }
    public boolean isXsltCache() {
        return xsltCache;
    }

    private String query;
    private String xsltURI;
    private boolean xsltCache;


}

// End TransformTag.java
