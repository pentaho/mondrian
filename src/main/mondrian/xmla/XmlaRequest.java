/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.Role;
import java.util.Map;
import java.util.List;

/**
 * XML/A request interface.
 *
 * @author Gang Chen
 * @version $Id$
 */
public interface XmlaRequest {

    /**
     * Indicate DISCOVER or EXECUTE method.
     */
    int getMethod();

    /**
     * Properties of XML/A request.
     */
    Map<String, String> getProperties();

    /**
     * Restrictions of DISCOVER method.
     *
     * <p>If the value is a list of strings, the restriction passes if the
     * column has one of the values.
     */
    Map<String, Object> getRestrictions();

    /**
     * Statement of EXECUTE method.
     */
    String getStatement();

    /**
     * Role name binds with this XML/A reqeust. Maybe null.
     */
    String getRoleName();

    /**
     * Role binds with this XML/A reqeust. Maybe null.
     */
    Role getRole();

    /**
     * Request type of DISCOVER method.
     */
    String getRequestType();

    /**
     * Indicate whether statement is a drill through statement of
     * EXECUTE method.
     */
    boolean isDrillThrough();

    /**
     * Drill through option: max returning rows of query.
     *
     * Value -1 means this option isn't provided.
     */
    int drillThroughMaxRows();

    /**
     * Drill through option: first returning row of query.
     *
     * Value -1 means this option isn't provided.
     */
    int drillThroughFirstRowset();

}

// End XmlaRequest.java
