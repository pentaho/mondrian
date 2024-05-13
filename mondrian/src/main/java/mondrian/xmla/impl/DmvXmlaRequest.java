/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2021 Sergei Semenkov.
 * Copyright (C) 2024 Hitachi Vantara.
 * All rights reserved.
 */

package mondrian.xmla.impl;

import java.util.HashMap;
import java.util.Map;

import mondrian.xmla.XmlaRequest;
import org.olap4j.metadata.XmlaConstants;
import org.olap4j.metadata.XmlaConstants.Method;

public class DmvXmlaRequest implements XmlaRequest {
  private final Map<String, Object> restrictions;
  private final Map<String, String> properties;
  private final String roleName;
  private final String requestType;
  private final String username;
  private final String password;
  private final String sessionId;

  public DmvXmlaRequest( Map<String, Object> restrictions, Map<String, String> properties, String roleName, String requestType, String username, String password, String sessionId ) {
    this.restrictions = new HashMap<>( restrictions );
    this.properties = new HashMap<>( properties );
    this.roleName = roleName;
    this.requestType = requestType;
    this.username = username;
    this.password = password;
    this.sessionId = sessionId;
  }

  public XmlaConstants.Method getMethod() {
    return Method.DISCOVER;
  }

  public Map<String, String> getProperties() {
    return this.properties;
  }

  public Map<String, Object> getRestrictions() {
    return this.restrictions;
  }

  public String getStatement() {
    return null;
  }

  public String getRoleName() {
    return this.roleName;
  }

  public String getRequestType() {
    return this.requestType;
  }

  public boolean isDrillThrough() {
    return false;
  }

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.password;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public String getAuthenticatedUser() {
    return null;
  }

  public String[] getAuthenticatedUserGroups() {
    return null;
  }
}
