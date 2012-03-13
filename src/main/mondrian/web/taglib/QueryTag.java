/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2010 Pentaho and others
// All Rights Reserved.
*/
package mondrian.web.taglib;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.BodyTagSupport;

/**
 * A <code>QueryTag</code> creates a {@link ResultCache} object and initializes
 * it with the MDX query. Example:<blockquote>
 *
 * <pre>&lt;query name="query1" resultCache="true"&gt;
 * select
 *   {[Measures].[Unit Sales], [Measures].[Store Cost]} on columns,
 *   CrossJoin(
 *     { [Promotion Media].[Radio],
 *       [Promotion Media].[TV],
 *       [Promotion Media].[Sunday Paper],
 *       [Promotion Media].[Street Handout] },
 *     [Product].[Drink].children) on rows
 * from Sales
 * where ([Time].[1997])
 * &lt;/query&gt;</pre>
 *
 * </blockquote>
 *
 * Attributes are
 * {@link #setName name},
 * {@link #setResultCache resultCache}.
 *
 * @author Andreas Voss, 22 March, 2002
 */
public class QueryTag extends BodyTagSupport {

    public QueryTag() {
    }

    public int doAfterBody() throws JspException {
        try {
            ApplResources ar =
                ApplResources.getInstance(pageContext.getServletContext());
            ResultCache rc =
                ResultCache.getInstance(
                    pageContext.getSession(),
                    pageContext.getServletContext(),
                    name);
            // if this is the first call, we have to parse the mdx query
            if (!resultCache || rc.getQuery() == null) {
                String mdx = getBodyContent().getString();
                rc.parse(mdx);
            }
            return SKIP_BODY;
        } catch (Exception e) {
            e.printStackTrace();
            throw new JspException(e);
        }
    }

    /** Sets string attribute <code>name</code>, which identifies this query
     * within its page. The {@link TransformTag#setQuery &lt;transform
     * query&gt;} attribute uses this. */
    public void setName(String newName) {
        name = newName;
    }
    public String getName() {
        return name;
    }
    /** Sets boolean attribute <code>resultCache</code>; if true, the query is
     * parsed, executed, and converted to an XML document at most once. This
     * improves performance and consistency, but the results may become out of
     * date. We also need a way to prevent the cache using too much memory. */
    public void setResultCache(boolean newResultCache) {
        resultCache = newResultCache;
    }
    public boolean isResultCache() {
        return resultCache;
    }
    private String name;
    private boolean resultCache;
}

// End QueryTag.java
