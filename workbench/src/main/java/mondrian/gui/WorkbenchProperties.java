/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

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