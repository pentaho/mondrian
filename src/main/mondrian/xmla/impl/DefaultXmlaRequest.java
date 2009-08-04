/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import java.util.*;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap.Role;
import mondrian.xmla.XmlaConstants;
import mondrian.xmla.XmlaException;
import mondrian.xmla.XmlaRequest;
import mondrian.xmla.XmlaUtil;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.log4j.Logger;

/**
 * Default implementation of {@link mondrian.xmla.XmlaRequest} by DOM API.
 *
 * @author Gang Chen
 */
public class DefaultXmlaRequest
    implements XmlaRequest, XmlaConstants
{
    private static final Logger LOGGER =
        Logger.getLogger(DefaultXmlaRequest.class);

    private static final String MSG_INVALID_XMLA = "Invalid XML/A message";
    private static final String MSG_INVALID_DRILLTHROUGH =
        "Invalid DRILLTHROUGH statement";
    private static final String MSG_INVALID_MAXROWS =
        "MAXROWS is not positive integer";
    private static final String MSG_INVALID_FIRSTROWSET =
        "FIRSTROWSET isn't positive integer";

    /* common content */
    private int method;
    private Map<String, String> properties;
    private final String roleName;
    private final Role role;

    /* EXECUTE content */
    private String statement;
    private boolean drillthrough;
    private int maxRows;
    private int firstRowset;

    /* DISCOVER contnet */
    private String requestType;
    private Map<String, Object> restrictions;


    public DefaultXmlaRequest(final Element xmlaRoot) {
        this(xmlaRoot, null, null);
    }

    public DefaultXmlaRequest(final Element xmlaRoot, final String roleName)
        throws XmlaException
    {
        this(xmlaRoot, roleName, null);
    }

    public DefaultXmlaRequest(final Element xmlaRoot, final Role role)
        throws XmlaException
    {
        this(xmlaRoot, null, role);
    }

    protected DefaultXmlaRequest(
        final Element xmlaRoot,
        final String roleName,
        final Role role)
        throws XmlaException
    {
        init(xmlaRoot);
        this.roleName = roleName;
        this.role = role;
    }

    /* Interface implmentation */

    public int getMethod() {
        return method;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Map<String, Object> getRestrictions() {
        if (method != METHOD_DISCOVER) {
            throw new IllegalStateException(
                "Only METHOD_DISCOVER has restrictions");
        }
        return restrictions;
    }

    public String getStatement() {
        if (method != METHOD_EXECUTE) {
            throw new IllegalStateException(
                "Only METHOD_EXECUTE has statement");
        }
        return statement;
    }

    public String getRoleName() {
        return roleName;
    }

    public Role getRole() {
        return role;
    }

/*
    public void setRole(String roleName) {
        this.role = role;
    }
*/

    public String getRequestType() {
        if (method != METHOD_DISCOVER) {
            throw new IllegalStateException(
                "Only METHOD_DISCOVER has requestType");
        }
        return requestType;
    }

    public boolean isDrillThrough() {
        if (method != METHOD_EXECUTE) {
            throw new IllegalStateException(
                "Only METHOD_EXECUTE determines drillthrough");
        }
        return drillthrough;
    }

    public int drillThroughMaxRows() {
        if (method != METHOD_EXECUTE) {
            throw new IllegalStateException(
                "Only METHOD_EXECUTE determines drillthrough");
        }
        return maxRows;
    }

    public int drillThroughFirstRowset() {
        if (method != METHOD_EXECUTE) {
            throw new IllegalStateException(
                "Only METHOD_EXECUTE determines drillthrough");
        }
        return firstRowset;
    }


    protected final void init(Element xmlaRoot) throws XmlaException {
        if (NS_XMLA.equals(xmlaRoot.getNamespaceURI())) {
            String lname = xmlaRoot.getLocalName();
            if ("Discover".equals(lname)) {
                method = METHOD_DISCOVER;
                initDiscover(xmlaRoot);
            } else if ("Execute".equals(lname)) {
                method = METHOD_EXECUTE;
                initExecute(xmlaRoot);
            } else {
                // Note that is code will never be reached because
                // the error will be caught in
                // DefaultXmlaServlet.handleSoapBody first
                StringBuilder buf = new StringBuilder(100);
                buf.append(MSG_INVALID_XMLA);
                buf.append(": Bad method name \"");
                buf.append(lname);
                buf.append("\"");
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_BAD_METHOD_CODE,
                    HSB_BAD_METHOD_FAULT_FS,
                    Util.newError(buf.toString()));
            }
        } else {
            // Note that is code will never be reached because
            // the error will be caught in
            // DefaultXmlaServlet.handleSoapBody first
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Bad namespace url \"");
            buf.append(xmlaRoot.getNamespaceURI());
            buf.append("\"");
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_METHOD_NS_CODE,
                HSB_BAD_METHOD_NS_FAULT_FS,
                Util.newError(buf.toString()));
        }
    }

    private void initDiscover(Element discoverRoot) throws XmlaException {
        Element[] childElems =
            XmlaUtil.filterChildElements(
                discoverRoot,
                NS_XMLA,
                "RequestType");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of RequestType elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_REQUEST_TYPE_CODE,
                HSB_BAD_REQUEST_TYPE_FAULT_FS,
                Util.newError(buf.toString()));
        }
        requestType = XmlaUtil.textInElement(childElems[0]); // <RequestType>

        childElems =
            XmlaUtil.filterChildElements(
                discoverRoot,
                NS_XMLA,
                "Restrictions");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Restrictions elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_RESTRICTIONS_CODE,
                HSB_BAD_RESTRICTIONS_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initRestrictions(childElems[0]); // <Restriciotns><RestrictionList>

        childElems =
            XmlaUtil.filterChildElements(
                discoverRoot,
                NS_XMLA,
                "Properties");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Properties elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_PROPERTIES_CODE,
                HSB_BAD_PROPERTIES_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initProperties(childElems[0]); // <Properties><PropertyList>
    }

    private void initExecute(Element executeRoot) throws XmlaException {
        Element[] childElems =
            XmlaUtil.filterChildElements(
                executeRoot,
                NS_XMLA,
                "Command");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Command elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_COMMAND_CODE,
                HSB_BAD_COMMAND_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initCommand(childElems[0]); // <Command><Statement>

        childElems =
            XmlaUtil.filterChildElements(
                executeRoot,
                NS_XMLA,
                "Properties");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Properties elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_PROPERTIES_CODE,
                HSB_BAD_PROPERTIES_FAULT_FS,
                Util.newError(buf.toString()));
        }
        initProperties(childElems[0]); // <Properties><PropertyList>
    }

    private void initRestrictions(Element restrictionsRoot)
        throws XmlaException
    {
        Map<String, List<String>> restrictions =
            new HashMap<String, List<String>>();
        Element[] childElems =
            XmlaUtil.filterChildElements(
                restrictionsRoot,
                NS_XMLA,
                "RestrictionList");
        if (childElems.length == 1) {
            NodeList nlst = childElems[0].getChildNodes();
            for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;
                    if (NS_XMLA.equals(e.getNamespaceURI())) {
                        String key = e.getLocalName();
                        String value = XmlaUtil.textInElement(e);

                        List<String> values;
                        if (restrictions.containsKey(key)) {
                            values = restrictions.get(key);
                        } else {
                            values = new ArrayList<String>();
                            restrictions.put(key, values);
                        }

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "DefaultXmlaRequest.initRestrictions: "
                                + " key=\""
                                + key
                                + "\", value=\""
                                + value
                                + "\"");
                        }

                        values.add(value);
                    }
                }
            }
        } else if (childElems.length > 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of RestrictionList elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_RESTRICTION_LIST_CODE,
                HSB_BAD_RESTRICTION_LIST_FAULT_FS,
                Util.newError(buf.toString()));
        } else {
        }
        this.restrictions = (Map) Collections.unmodifiableMap(restrictions);
    }

    private void initProperties(Element propertiesRoot) throws XmlaException {
        Map<String, String> properties = new HashMap<String, String>();
        Element[] childElems =
            XmlaUtil.filterChildElements(
                propertiesRoot,
                NS_XMLA,
                "PropertyList");
        if (childElems.length == 1) {
            NodeList nlst = childElems[0].getChildNodes();
            for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;
                    if (NS_XMLA.equals(e.getNamespaceURI())) {
                        String key = e.getLocalName();
                        String value = XmlaUtil.textInElement(e);

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(
                                "DefaultXmlaRequest.initProperties: "
                                + " key=\""
                                + key
                                + "\", value=\""
                                + value
                                + "\"");
                        }

                        properties.put(key, value);
                    }
                }
            }
        } else if (childElems.length > 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of PropertyList elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_PROPERTIES_LIST_CODE,
                HSB_BAD_PROPERTIES_LIST_FAULT_FS,
                Util.newError(buf.toString()));
        } else {
        }
        this.properties = Collections.unmodifiableMap(properties);
    }


    private void initCommand(Element commandRoot) throws XmlaException {
        Element[] childElems =
            XmlaUtil.filterChildElements(
                commandRoot,
                NS_XMLA,
                "Statement");
        if (childElems.length != 1) {
            StringBuilder buf = new StringBuilder(100);
            buf.append(MSG_INVALID_XMLA);
            buf.append(": Wrong number of Statement elements: ");
            buf.append(childElems.length);
            throw new XmlaException(
                CLIENT_FAULT_FC,
                HSB_BAD_STATEMENT_CODE,
                HSB_BAD_STATEMENT_FAULT_FS,
                Util.newError(buf.toString()));
        }
        statement = XmlaUtil.textInElement(childElems[0]).replaceAll("\\r", "");

        String upperStatement = statement.toUpperCase();
        int dtOffset = upperStatement.indexOf("DRILLTHROUGH");
        int mrOffset = upperStatement.indexOf("MAXROWS");
        int frOffset = upperStatement.indexOf("FIRSTROWSET");
        int slOffset = upperStatement.indexOf("SELECT");

        if (dtOffset == -1) {
            drillthrough = false;
        } else {
            /*
             * <drillthrough> := DRILLTHROUGH
             *     [<Max_Rows>] [<First_Rowset>] <MDX select> [<Return_Columns>]
             * <Max_Rows> := MAXROWS <positive number>
             * <First_Rowset> := FIRSTROWSET <positive number>
             * <Return_Columns> := RETURN <member or attribute>
             *     [, <member or attribute>]
             */
            if (dtOffset < slOffset) {
                maxRows = firstRowset = -1;
                try {
                    if (mrOffset > dtOffset && mrOffset < slOffset) {
                        maxRows = parseIntValue(
                            statement.substring(mrOffset, slOffset));
                        if (maxRows <= 0) {
                            StringBuilder buf = new StringBuilder(100);
                            buf.append(MSG_INVALID_MAXROWS);
                            buf.append(": ");
                            buf.append(maxRows);
                            throw new XmlaException(
                                CLIENT_FAULT_FC,
                                HSB_DRILLDOWN_BAD_MAXROWS_CODE,
                                HSB_DRILLDOWN_BAD_MAXROWS_FAULT_FS,
                                Util.newError(buf.toString()));
                        }
                    }
                    if (frOffset > dtOffset
                        && frOffset > mrOffset
                        && frOffset < slOffset)
                    {
                        firstRowset =
                            parseIntValue(
                                statement.substring(frOffset, slOffset));
                        if (firstRowset <= 0) {
                            StringBuilder buf = new StringBuilder(100);
                            buf.append(MSG_INVALID_FIRSTROWSET);
                            buf.append(": ");
                            buf.append(firstRowset);
                            throw new XmlaException(
                                CLIENT_FAULT_FC,
                                HSB_DRILLDOWN_BAD_FIRST_ROWSET_CODE,
                                HSB_DRILLDOWN_BAD_FIRST_ROWSET_FAULT_FS,
                                Util.newError(buf.toString()));
                        }
                    }
                } catch (XmlaException xex) {
                    throw xex;
                } catch (Exception e) {
                    throw new XmlaException(
                        CLIENT_FAULT_FC,
                        HSB_DRILLDOWN_ERROR_CODE,
                        HSB_DRILLDOWN_ERROR_FAULT_FS,
                        Util.newError(e, MSG_INVALID_DRILLTHROUGH));
                }

                int configMaxRows = MondrianProperties.instance().MaxRows.get();
                if (configMaxRows > 0 && maxRows > configMaxRows) {
                    maxRows = configMaxRows;
                }

                StringBuilder dtStmtBuf = new StringBuilder();
                dtStmtBuf.append(statement.substring(0, dtOffset)); // formulas
                // select to end
                dtStmtBuf.append(statement.substring(slOffset));
                statement = dtStmtBuf.toString();

                drillthrough = true;
            } else {
                throw new XmlaException(
                    CLIENT_FAULT_FC,
                    HSB_DRILLDOWN_ERROR_CODE,
                    HSB_DRILLDOWN_ERROR_FAULT_FS,
                    Util.newError(MSG_INVALID_DRILLTHROUGH));
            }
        }
    }

    private int parseIntValue(String option) {
        String[] opts = option.split("[ \t\n]");
        return Integer.parseInt(opts[1]);
    }

}

// End DefaultXmlaRequest.java
