/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.xmla.XmlaConstants;
import mondrian.xmla.XmlaRequest;
import mondrian.xmla.XmlaUtil;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Default implementation of {@link mondrian.xmla.XmlaRequest} by DOM API.
 *
 * @author Gang Chen
 */
public class DefaultXmlaRequest implements XmlaRequest,
                                           XmlaConstants {

    private static final String MSG_INVALID_XMLA = "Invalid XML/A message";
    private static final String MSG_INVALID_DRILLTHROUGH = "Invalid DRILLTHROUGH statement";
    private static final String MSG_INVALID_MAXROWS = "MAXROWS isn't positive integer";
    private static final String MSG_INVALID_FIRSTROWSET = "FIRSTROWSET isn't positive integer";

    /* common content */
    private int method;
    private Map properties;
    private String role;

    /* EXECUTE content */
    private String statement;
    private boolean drillthrough;
    private int maxRows;
    private int firstRowset;

    /* DISCOVER contnet */
    private String requestType;
    private Map restrictions;


    public DefaultXmlaRequest(Element xmlaRoot) {
        this(xmlaRoot, null);
    }

    public DefaultXmlaRequest(Element xmlaRoot, String role) {
        init(xmlaRoot);
        this.role = role;
    }

    /* Interface implmentation */

    public int getMethod() {
        return method;
    }

    public Map getProperties() {
        return properties;
    }

    public Map getRestrictions() {
        if (method != METHOD_DISCOVER)
            throw new IllegalStateException("Only METHOD_DISCOVER has restrictions");
        return restrictions;
    }

    public String getStatement() {
        if (method != METHOD_EXECUTE)
            throw new IllegalStateException("Only METHOD_EXECUTE has statement");
        return statement;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getRequestType() {
        if (method != METHOD_DISCOVER)
            throw new IllegalStateException("Only METHOD_DISCOVER has requestType");
        return requestType;
    }

    public boolean isDrillThrough() {
        if (method != METHOD_EXECUTE)
            throw new IllegalStateException("Only METHOD_EXECUTE determines drillthrough");
        return drillthrough;
    }

    public int drillThroughMaxRows() {
        if (method != METHOD_EXECUTE)
            throw new IllegalStateException("Only METHOD_EXECUTE determines drillthrough");
        return maxRows;
    }

    public int drillThroughFirstRowset() {
        if (method != METHOD_EXECUTE)
            throw new IllegalStateException("Only METHOD_EXECUTE determines drillthrough");
        return firstRowset;
    }


    protected final void init(Element xmlaRoot) {
        if (NS_XMLA.equals(xmlaRoot.getNamespaceURI())) {
            String lname = xmlaRoot.getLocalName();
            if ("Discover".equals(lname)) {
                method = METHOD_DISCOVER;
                initDiscover(xmlaRoot);
            } else if ("Execute".equals(lname)) {
                method = METHOD_EXECUTE;
                initExecute(xmlaRoot);
            } else {
                throw Util.newError(MSG_INVALID_XMLA);
            }
        } else {
            throw Util.newError(MSG_INVALID_XMLA);
        }
    }

    private void initDiscover(Element discoverRoot) {
        Element[] childElems = XmlaUtil.filterChildElements(discoverRoot,
                                                            NS_XMLA,
                                                            "RequestType");
        if (childElems.length != 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        }
        requestType = XmlaUtil.textInElement(childElems[0]); // <RequestType>

        childElems = XmlaUtil.filterChildElements(discoverRoot,
                                                  NS_XMLA,
                                                  "Restrictions");
        if (childElems.length != 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        }
        initRestrictions(childElems[0]); // <Restriciotns><RestrictionList>

        childElems = XmlaUtil.filterChildElements(discoverRoot,
                                                  NS_XMLA,
                                                  "Properties");
        if (childElems.length != 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        }
        initProperties(childElems[0]); // <Properties><PropertyList>
    }

    private void initExecute(Element executeRoot) {
        Element[] childElems = XmlaUtil.filterChildElements(executeRoot,
                                                            NS_XMLA,
                                                            "Command");
        if (childElems.length != 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        }
        initCommand(childElems[0]); // <Command><Statement>

        childElems = XmlaUtil.filterChildElements(executeRoot,
                                                  NS_XMLA,
                                                  "Properties");
        if (childElems.length != 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        }
        initProperties(childElems[0]); // <Properties><PropertyList>
    }

    private void initRestrictions(Element restrictionsRoot) {
        Map restricions = new HashMap();
        Element[] childElems = XmlaUtil.filterChildElements(restrictionsRoot,
                                                            NS_XMLA,
                                                            "RestrictionList");
        if (childElems.length == 1) {
            NodeList nlst = childElems[0].getChildNodes();
            for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;
                    if (!NS_XMLA.equals(e.getNamespaceURI()))
                        continue;
                    String key = e.getLocalName();
                    List values;
                    if (restricions.containsKey(key)) {
                        values = (List) restricions.get(key);
                    } else {
                        values = new ArrayList();
                        restricions.put(key, values);
                    }
                    values.add(XmlaUtil.textInElement(e));
                }
            }
            String[] dummy = new String[0];
            for (Iterator it = restricions.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                List values = (List) entry.getValue();
                if (values.size() > 1) {
                    entry.setValue(values.toArray(dummy));
                } else {
                    entry.setValue(values.get(0));
                }
            }
        } else if (childElems.length > 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        } else {
        }
        restrictions = Collections.unmodifiableMap(restricions);
    }

    private void initProperties(Element propertiesRoot) {
        Map properties = new HashMap();
        Element[] childElems = XmlaUtil.filterChildElements(propertiesRoot,
                                                            NS_XMLA,
                                                            "PropertyList");
        if (childElems.length == 1) {
            NodeList nlst = childElems[0].getChildNodes();
            for (int i = 0, nlen = nlst.getLength(); i < nlen; i++) {
                Node n = nlst.item(i);
                if (n instanceof Element) {
                    Element e = (Element) n;
                    if (!NS_XMLA.equals(e.getNamespaceURI()))
                        continue;
                    properties.put(e.getLocalName(), XmlaUtil.textInElement(e));
                }
            }
        } else if (childElems.length > 1) {
            throw Util.newError(MSG_INVALID_XMLA);
        } else {
        }
        this.properties = Collections.unmodifiableMap(properties);
    }


    private void initCommand(Element commandRoot) {
        Element[] childElems = XmlaUtil.filterChildElements(commandRoot,
                                                            NS_XMLA,
                                                            "Statement");
        if (childElems.length != 1) {
            throw Util.newError(MSG_INVALID_XMLA);
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
            if (dtOffset < slOffset) {
                maxRows = firstRowset = -1;
                try {
                    if (mrOffset > dtOffset && mrOffset < slOffset) {
                        maxRows = parseIntValue(statement.substring(mrOffset, slOffset));
                        if (maxRows <= 0) {
                            new IllegalArgumentException(MSG_INVALID_MAXROWS);
                        }
                    }
                    if (frOffset > dtOffset && frOffset > mrOffset && frOffset < slOffset) {
                        firstRowset = parseIntValue(statement.substring(frOffset, slOffset));
                        if (firstRowset <= 0) {
                            new IllegalArgumentException(MSG_INVALID_FIRSTROWSET);
                        }
                    }
                } catch (Exception e) {
                    throw Util.newError(e, MSG_INVALID_DRILLTHROUGH);
                }

                int configMaxRows = MondrianProperties.instance().MaxRows.get();
                if (configMaxRows > 0 && maxRows > configMaxRows) {
                    maxRows = configMaxRows;
                }
                
                StringBuffer dtStmtBuf = new StringBuffer();
                dtStmtBuf.append(statement.substring(0, dtOffset)); // formulas
                dtStmtBuf.append(statement.substring(slOffset)); // select to end
                statement = dtStmtBuf.toString();

                drillthrough = true;
            } else {
                throw Util.newError(MSG_INVALID_DRILLTHROUGH);
            }
        }
    }

    private int parseIntValue(String option) {
        String[] opts = option.split("[ \t\n]");
        return Integer.parseInt(opts[1]);
    }

}

// End DefaultXmlaRequest.java
