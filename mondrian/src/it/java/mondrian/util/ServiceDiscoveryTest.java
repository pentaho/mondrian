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
package mondrian.util;

import junit.framework.TestCase;
import mondrian.spi.UserDefinedFunction;
import junit.framework.Test;

/**
 * Created by nbaker on 8/2/16.
 */
public class ServiceDiscoveryTest extends TestCase {

  public void testGetImplementor() throws Exception {
    ServiceDiscovery<UserDefinedFunction> userDefinedFunctionServiceDiscovery =
        ServiceDiscovery.forClass( UserDefinedFunction.class );
    assertNotNull( userDefinedFunctionServiceDiscovery );
    assertTrue( userDefinedFunctionServiceDiscovery.getImplementor().size() > 0 );
  }

}
