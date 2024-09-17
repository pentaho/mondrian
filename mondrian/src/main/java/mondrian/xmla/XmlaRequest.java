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

package mondrian.xmla;

import org.olap4j.metadata.XmlaConstants;

import java.util.Map;

/**
 * XML/A request interface.
 *
 * @author Gang Chen
 */
public interface XmlaRequest {
  /**
   * Indicate DISCOVER or EXECUTE method.
   */
  XmlaConstants.Method getMethod();

  /**
   * Properties of XML/A request.
   */
  Map<String, String> getProperties();

  /**
   * Restrictions of DISCOVER method.
   *
   * <p>If the value is a list of strings, the restriction passes if the column has one of the values.
   */
  Map<String, Object> getRestrictions();

  /**
   * Statement of EXECUTE method.
   */
  String getStatement();

  /**
   * Role name binds with this XML/A request. Maybe null.
   */
  String getRoleName();

  /**
   * Request type of DISCOVER method.
   */
  String getRequestType();

  /**
   * Indicate whether statement is a drill through statement of EXECUTE method.
   */
  boolean isDrillThrough();

  /**
   * The username to use to open the underlying olap4j connection. Can be null.
   */
  String getUsername();

  /**
   * The password to use to open the underlying olap4j connection. Can be null.
   */
  String getPassword();

  /**
   * Returns the id of the session this request belongs to.
   *
   * <p>Not necessarily the same as the HTTP session: the SOAP request contains its own session information.</p>
   *
   * <p>The session id is used to retrieve existing olap connections. And username / password only need to be passed
   * on the first request in a session.</p>
   *
   * @return Id of the session
   */
  String getSessionId();

  String getAuthenticatedUser();

  String[] getAuthenticatedUserGroups();
}
