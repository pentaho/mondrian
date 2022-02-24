package mondrian.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import mondrian.spi.UserDefinedFunction;

/**
 * Created by nbaker on 8/2/16.
 */
public class ServiceDiscoveryTest{

  @org.junit.jupiter.api.Test
  public void testGetImplementor() throws Exception {
    ServiceDiscovery<UserDefinedFunction> userDefinedFunctionServiceDiscovery =
        ServiceDiscovery.forClass( UserDefinedFunction.class );
    assertNotNull( userDefinedFunctionServiceDiscovery );
    assertTrue( userDefinedFunctionServiceDiscovery.getImplementor().size() > 0 );
  }

}
