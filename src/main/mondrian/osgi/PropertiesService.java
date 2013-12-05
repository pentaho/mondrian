/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.osgi;

import mondrian.olap.MondrianProperties;

import java.util.Map;

/**
 * Recives an OSGI Configuration Admin managed map of properties. setProperties
 * will be call once on initialization and again for any change to the
 * "mondrian" OSGI configuration.
 *
 * User: nbaker
 * Date: 11/12/13
 */
public class PropertiesService {

  public void setProperties(Map<String, ?> properties) {
    MondrianProperties.instance().clear();
    MondrianProperties.instance().putAll(properties);
  }
}
// End PropertiesService.java