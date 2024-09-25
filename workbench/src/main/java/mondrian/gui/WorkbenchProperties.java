/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2020 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.gui;

import org.pentaho.di.core.encryption.Encr;

import java.util.Properties;

public class WorkbenchProperties extends Properties {

  private String jdbcPass = "jdbcPassword";

  @Override
  public synchronized Object setProperty(String key, String value) {
    if (jdbcPass.equals(key)) {
      value = Encr.encryptPasswordIfNotUsingVariables(value);
    }
    return super.setProperty(key, value);
  }

  @Override
  public String getProperty(String key) {
    if (jdbcPass.equals(key)) {
      return Encr.decryptPasswordOptionallyEncrypted(super.getProperty(key));
    }
    return super.getProperty(key);
  }
}
// End WorkbenchProperties.java