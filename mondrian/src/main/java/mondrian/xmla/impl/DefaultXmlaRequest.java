/*
 * This software is subject to the terms of the Eclipse Public License v1.0
 * Agreement, available at the following URL:
 * http://www.eclipse.org/legal/epl-v10.html.
 * You must accept the terms of that agreement to use this software.
 *
 * Copyright (C) 2019 Topsoft
 * Copyright (C) 2021-2022 Sergei Semenkov
 * Copyright (C) 2002-2024 Hitachi Vantara
 * All rights reserved.
 */

package mondrian.xmla.impl;

import mondrian.olap.Util;
import mondrian.xmla.XmlaConstants;
import mondrian.xmla.XmlaException;
import mondrian.xmla.XmlaRequest;
import mondrian.xmla.XmlaUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.olap4j.metadata.XmlaConstants.Method;

/**
 * Default implementation of {@link mondrian.xmla.XmlaRequest} by DOM API.
 *
 * @author Gang Chen
 */
public class DefaultXmlaRequest implements XmlaRequest, XmlaConstants {
  private static final Logger LOGGER = LogManager.getLogger( DefaultXmlaRequest.class );

  private static final String MSG_INVALID_XMLA = "Invalid XML/A message";

  /* common content */
  private Method method;
  private Map<String, String> properties;
  private final String roleName;

  /* EXECUTE content */
  private String statement;
  private boolean drillThrough;
  private String command;

  /* DISCOVER content */
  private String requestType;
  private Map<String, Object> restrictions;

  private final String username;
  private final String password;
  private final String sessionId;
  private String authenticatedUser = null;
  private String[] authenticatedUserGroups = null;

  public DefaultXmlaRequest( final Element xmlaRoot, final String roleName, final String username,
                             final String password, final String sessionId ) throws XmlaException {
    init( xmlaRoot );
    this.roleName = roleName;
    this.username = username;
    this.password = password;
    this.sessionId = sessionId;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public Method getMethod() {
    return method;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public Map<String, Object> getRestrictions() {
    if ( method != Method.DISCOVER ) {
      throw new IllegalStateException( "Only METHOD_DISCOVER has restrictions" );
    }

    return restrictions;
  }

  public String getStatement() {
    if ( method != Method.EXECUTE ) {
      throw new IllegalStateException( "Only METHOD_EXECUTE has statement" );
    }

    return statement;
  }

  public String getRoleName() {
    return roleName;
  }

  public String getCommand() {
    return this.command;
  }

  public String getRequestType() {
    if ( method != Method.DISCOVER ) {
      throw new IllegalStateException( "Only METHOD_DISCOVER has requestType" );
    }

    return requestType;
  }

  public boolean isDrillThrough() {
    if ( method != Method.EXECUTE ) {
      throw new IllegalStateException( "Only METHOD_EXECUTE determines drill-through" );
    }

    return drillThrough;
  }

  protected final void init( Element xmlaRoot ) throws XmlaException {
    if ( NS_XMLA.equals( xmlaRoot.getNamespaceURI() ) ) {
      String lname = xmlaRoot.getLocalName();

      if ( "Discover".equals( lname ) ) {
        method = Method.DISCOVER;
        initDiscover( xmlaRoot );
      } else if ( "Execute".equals( lname ) ) {
        method = Method.EXECUTE;
        initExecute( xmlaRoot );
      } else {
        // Note that is code will never be reached because the error will be caught in DefaultXmlaServlet
        // .handleSoapBody first
        String buf = MSG_INVALID_XMLA + ": Bad method name \"" + lname + "\"";
        throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_METHOD_CODE, HSB_BAD_METHOD_FAULT_FS, Util.newError( buf ) );
      }
    } else {
      // Note that is code will never be reached because the error will be caught in DefaultXmlaServlet
      // .handleSoapBody first
      String buf = MSG_INVALID_XMLA + ": Bad namespace url \"" + xmlaRoot.getNamespaceURI() + "\"";
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_METHOD_NS_CODE, HSB_BAD_METHOD_NS_FAULT_FS,
        Util.newError( buf ) );
    }
  }

  private void initDiscover( Element discoverRoot ) throws XmlaException {
    Element[] childElems = XmlaUtil.filterChildElements( discoverRoot, NS_XMLA, "RequestType" );

    if ( childElems.length != 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of RequestType elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_REQUEST_TYPE_CODE, HSB_BAD_REQUEST_TYPE_FAULT_FS,
        Util.newError( buf ) );
    }

    requestType = XmlaUtil.textInElement( childElems[ 0 ] ); // <RequestType>

    childElems = XmlaUtil.filterChildElements( discoverRoot, NS_XMLA, "Properties" );

    if ( childElems.length != 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of Properties elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_PROPERTIES_CODE, HSB_BAD_PROPERTIES_FAULT_FS,
        Util.newError( buf ) );
    }

    initProperties( childElems[ 0 ] ); // <Properties><PropertyList>

    childElems = XmlaUtil.filterChildElements( discoverRoot, NS_XMLA, "Restrictions" );

    if ( childElems.length != 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of Restrictions elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_RESTRICTIONS_CODE, HSB_BAD_RESTRICTIONS_FAULT_FS,
        Util.newError( buf ) );
    }

    initRestrictions( childElems[ 0 ] ); // <Restrictions><RestrictionList>
  }

  private void initExecute( Element executeRoot ) throws XmlaException {
    Element[] childElems = XmlaUtil.filterChildElements( executeRoot, NS_XMLA, "Command" );

    if ( childElems.length != 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of Command elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_COMMAND_CODE, HSB_BAD_COMMAND_FAULT_FS, Util.newError( buf ) );
    }

    initCommand( childElems[ 0 ] ); // <Command><Statement>

    childElems = XmlaUtil.filterChildElements( executeRoot, NS_XMLA, "Properties" );

    if ( childElems.length != 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of Properties elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_PROPERTIES_CODE, HSB_BAD_PROPERTIES_FAULT_FS,
        Util.newError( buf ) );
    }

    initProperties( childElems[ 0 ] ); // <Properties><PropertyList>
  }

  private void initRestrictions( Element restrictionsRoot ) throws XmlaException {
    Map<String, List<String>> localRestrictions = new HashMap<>();
    Element[] childElems = XmlaUtil.filterChildElements( restrictionsRoot, NS_XMLA, "RestrictionList" );

    if ( childElems.length == 1 ) {
      NodeList nlst = childElems[ 0 ].getChildNodes();

      for ( int i = 0, nlen = nlst.getLength(); i < nlen; i++ ) {
        Node n = nlst.item( i );

        if ( n instanceof Element ) {
          Element e = (Element) n;

          if ( NS_XMLA.equals( e.getNamespaceURI() ) ) {
            String key = e.getLocalName();
            String value = XmlaUtil.textInElement( e );
            List<String> values;

            if ( localRestrictions.containsKey( key ) ) {
              values = localRestrictions.get( key );
            } else {
              values = new ArrayList<>();
              localRestrictions.put( key, values );
            }

            if ( LOGGER.isDebugEnabled() ) {
              LOGGER.debug( "DefaultXmlaRequest.initRestrictions: key=\"{}\", value=\"{}\"", key, value );
            }

            values.add( value );
          }
        }
      }
    } else if ( childElems.length > 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of RestrictionList elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_RESTRICTION_LIST_CODE, HSB_BAD_RESTRICTION_LIST_FAULT_FS,
        Util.newError( buf ) );
    }

    // If there is a Catalog property, we have to consider it a constraint as well.
    String key = org.olap4j.metadata.XmlaConstants.Literal.CATALOG_NAME.name();

    if ( this.properties.containsKey( key ) && !localRestrictions.containsKey( key ) ) {
      List<String> values = new ArrayList<>();
      localRestrictions.put( this.properties.get( key ), values );

      if ( LOGGER.isDebugEnabled() ) {
        LOGGER.debug( "DefaultXmlaRequest.initRestrictions: key=\"{}\", value=\"{}\"", key,
          this.properties.get( key ) );
      }
    }

    this.restrictions = Collections.unmodifiableMap( localRestrictions );
  }

  private void initProperties( Element propertiesRoot ) throws XmlaException {
    Map<String, String> localProperties = new HashMap<>();
    Element[] childElems = XmlaUtil.filterChildElements( propertiesRoot, NS_XMLA, "PropertyList" );

    if ( childElems.length == 1 ) {
      NodeList nlst = childElems[ 0 ].getChildNodes();

      for ( int i = 0, nlen = nlst.getLength(); i < nlen; i++ ) {
        Node n = nlst.item( i );

        if ( n instanceof Element ) {
          Element e = (Element) n;

          if ( NS_XMLA.equals( e.getNamespaceURI() ) ) {
            String key = e.getLocalName();
            String value = XmlaUtil.textInElement( e );

            if ( LOGGER.isDebugEnabled() ) {
              LOGGER.debug( "DefaultXmlaRequest.initProperties: " + " key=\"{}\", value=\"{}\"", key, value );
            }

            localProperties.put( key, value );
          }
        }
      }
    } else if ( childElems.length > 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of PropertyList elements: " + childElems.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_PROPERTIES_LIST_CODE, HSB_BAD_PROPERTIES_LIST_FAULT_FS,
        Util.newError( buf ) );
    }

    this.properties = Collections.unmodifiableMap( localProperties );
  }

  private void initCommand( Element commandRoot ) throws XmlaException {
    Element[] commandElements = XmlaUtil.filterChildElements( commandRoot, null, null );

    if ( commandElements.length != 1 ) {
      String buf = MSG_INVALID_XMLA + ": Wrong number of Command children elements: " + commandElements.length;
      throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_COMMAND_CODE, HSB_BAD_COMMAND_FAULT_FS, Util.newError( buf ) );
    } else {
      this.command = commandElements[ 0 ].getLocalName();

      if ( this.command == null || this.command.isEmpty() ) {
        throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_COMMAND_CODE, HSB_BAD_COMMAND_FAULT_FS,
          Util.newError( MSG_INVALID_XMLA + ": Command not found" ) );
      }

      if ( this.command.equalsIgnoreCase( "STATEMENT" ) ) {
        this.statement = XmlaUtil.textInElement( commandElements[ 0 ] ).replaceAll( "\\r", "" );
        this.drillThrough = this.statement.toUpperCase().contains( "DRILLTHROUGH" );
      } else if ( !this.command.equalsIgnoreCase( "CANCEL" ) ) {
        String buf = MSG_INVALID_XMLA + ": Wrong child of Command elements: " + this.command;
        throw new XmlaException( CLIENT_FAULT_FC, HSB_BAD_COMMAND_CODE, HSB_BAD_COMMAND_FAULT_FS,
          Util.newError( buf ) );
      }
    }
  }

  public void setProperty( String key, String value ) {
    HashMap<String, String> newProperties = new HashMap<>( this.properties );
    newProperties.put( key, value );
    this.properties = Collections.unmodifiableMap( newProperties );
  }

  public void setAuthenticatedUser( String authenticatedUser ) {
    this.authenticatedUser = authenticatedUser;
  }

  public String getAuthenticatedUser() {
    return this.authenticatedUser;
  }

  public void setAuthenticatedUserGroups( String[] authenticatedUserGroups ) {
    this.authenticatedUserGroups = authenticatedUserGroups;
  }

  public String[] getAuthenticatedUserGroups() {
    return this.authenticatedUserGroups;
  }
}
