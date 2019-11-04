/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2019 Hitachi Vantara..  All rights reserved.
*/

package mondrian.test;

import junit.framework.TestCase;
import mondrian.i18n.LocalizingDynamicSchemaProcessor;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.spi.DynamicSchemaProcessor;

/**
 * Unit test LocalizingDynamicSchemaProcessor. Tests availability of properties that LDSP's
 * are called, and used to modify the resulting Mondrian schema
 *
 * @author bcosta
 */
public class LocalizingDynamicSchemaProcessorTest
    extends TestCase
{
    /**
     * Tests to make sure that our LocalizingDynamicSchemaProcessor works, with
     * replacements. Does not test Mondrian is able to connect with the schema
     * definition.
     */
    public void testProcessingCatalog() {
        String variable = "%{translate}";
        String translation = "Translate";
        String catalog = "..." + variable + "...";
        MondrianProperties.instance().LocalePropFile.set("mondrian/i18n/LocalizingDynamicSchemaProcessor/resources.properties" );
        DynamicSchemaProcessor dsp = new LocalizingDynamicSchemaProcessor();
        Util.PropertyList connectInfo = new Util.PropertyList();

        try {
            String catalogProcessed = dsp.processCatalog(catalog, connectInfo);
            assertEquals( catalog.replace( variable, translation ), catalogProcessed );
        } catch (Exception e) {
            // TODO some other assert failure message
            assertEquals(0, 1);
        }

        //Reset property value
        MondrianProperties.instance().LocalePropFile.set( "" );
    }


}
// End LocalizingDynamicSchemaProcessorTest.java
