/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import junit.framework.TestCase;
import org.eigenbase.util.property.*;
import org.eigenbase.util.property.Property;

/**
 * Tests Mondrian Properties
 *
 * @author remberson
 */
public class MondrianPropertiesTest extends TestCase {

    public MondrianPropertiesTest(String name) {
        super(name);
    }

    private boolean triggerCalled;
    private String triggerValue;

    /** 
     * Test if the trigger is called after the value is changed 
     */
    public void changeValueTest() {
        String path= "test.mondrian.properties.change.value"; 
        BooleanProperty boolProp = new BooleanProperty(
                MondrianProperties.instance(),
                path,
                false);

        assertTrue("Check property value NOT false", 
            (! boolProp.get()));

        // now explicitly set the property
        MondrianProperties.instance().setProperty(path, "false");
        
        String v = MondrianProperties.instance().getProperty(path);
        assertTrue("Check property value is null", 
            (v != null));
        assertTrue("Check property value is true", 
            (! Boolean.valueOf(v).booleanValue()));
        

        triggerCalled = false;
        triggerValue = null;

        boolProp.addTrigger(
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.PRIMARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        triggerCalled = true;
                        triggerValue = value;
                    }
                }
        );

        String falseStr = "false";
        MondrianProperties.instance().setProperty(path, falseStr);
        assertTrue("Check trigger was called", 
            ! triggerCalled);

        String trueStr = "true";
        MondrianProperties.instance().setProperty(path, trueStr);

        assertTrue("Check trigger was NOT called", 
            triggerCalled);
        assertTrue("Check trigger value was null", 
            (triggerValue != null));
        assertTrue("Check trigger value is NOT correct", 
            triggerValue.equals(trueStr));
        
    }

    int callCounter;
    int primaryOne;
    int primaryTwo;
    int secondaryOne;
    int secondaryTwo;
    int tertiaryOne;
    int tertiaryTwo;

    /** 
     * Check that triggers are called in the correct order. 
     */
    public void triggerCallOrderTest() {
        String path= "test.mondrian.properties.call.order"; 
        BooleanProperty boolProp = new BooleanProperty(
                MondrianProperties.instance(),
                path,
                false);

        callCounter = 0;

        // now explicitly set the property
        MondrianProperties.instance().setProperty(path, "false");

        String v = MondrianProperties.instance().getProperty(path);
        assertTrue("Check property value is null", 
            (v != null));
        assertTrue("Check property value is true", 
            (! Boolean.valueOf(v).booleanValue()));

        // primaryOne
        Trigger primaryOneTrigger =
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.PRIMARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        primaryOne = callCounter++;
                    }
                };
        boolProp.addTrigger(primaryOneTrigger);

        // secondaryOne
        Trigger secondaryOneTrigger =
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.SECONDARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        secondaryOne = callCounter++;
                    }
                };
        boolProp.addTrigger(secondaryOneTrigger);

        // tertiaryOne
        Trigger tertiaryOneTrigger =
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.TERTIARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        tertiaryOne = callCounter++;
                    }
                };
        boolProp.addTrigger(tertiaryOneTrigger);

        // tertiaryTwo
        Trigger tertiaryTwoTrigger =
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.TERTIARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        tertiaryTwo = callCounter++;
                    }
                };
        boolProp.addTrigger(tertiaryTwoTrigger);

        // secondaryTwo
        Trigger secondaryTwoTrigger =
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.SECONDARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        secondaryTwo = callCounter++;
                    }
                };
        boolProp.addTrigger(secondaryTwoTrigger);

        // primaryTwo
        Trigger primaryTwoTrigger =
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.PRIMARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        primaryTwo = callCounter++;
                    }
                };
        boolProp.addTrigger(primaryTwoTrigger);

        String falseStr = "false";
        MondrianProperties.instance().setProperty(path, falseStr);
        assertTrue("Check trigger was called", 
            (callCounter == 0));


        String trueStr = "true";
        MondrianProperties.instance().setProperty(path, trueStr);

        assertTrue("Check trigger was NOT called", 
            (callCounter != 0 ));
        assertTrue("Check triggers was NOT called correct number of times", 
            (callCounter == 6 ));

        // now make sure that primary are called before secondary which are
        // before tertiary
        assertTrue("Check primaryOne > secondaryOne", 
            (primaryOne < secondaryOne));
        assertTrue("Check primaryOne > secondaryTwo", 
            (primaryOne < secondaryTwo));
        assertTrue("Check primaryOne > tertiaryOne", 
            (primaryOne < tertiaryOne));
        assertTrue("Check primaryOne > tertiaryTwo", 
            (primaryOne < tertiaryTwo));

        assertTrue("Check primaryTwo > secondaryOne", 
            (primaryTwo < secondaryOne));
        assertTrue("Check primaryTwo > secondaryTwo", 
            (primaryTwo < secondaryTwo));
        assertTrue("Check primaryTwo > tertiaryOne", 
            (primaryTwo < tertiaryOne));
        assertTrue("Check primaryTwo > tertiaryTwo", 
            (primaryTwo < tertiaryTwo));

        assertTrue("Check secondaryOne > tertiaryOne", 
            (secondaryOne < tertiaryOne));
        assertTrue("Check secondaryOne > tertiaryTwo", 
            (secondaryOne < tertiaryTwo));

        assertTrue("Check secondaryTwo > tertiaryOne", 
            (secondaryTwo < tertiaryOne));
        assertTrue("Check secondaryTwo > tertiaryTwo", 
            (secondaryTwo < tertiaryTwo));
        


        // remove some of the triggers
        boolProp.removeTrigger(primaryTwoTrigger);
        boolProp.removeTrigger(secondaryTwoTrigger);
        boolProp.removeTrigger(tertiaryTwoTrigger);

        // reset
        callCounter = 0;
        primaryOne = 0;
        primaryTwo = 0;
        secondaryOne = 0;
        secondaryTwo = 0;
        tertiaryOne = 0;
        tertiaryTwo = 0;

        MondrianProperties.instance().setProperty(path, falseStr);
        assertTrue("Check trigger was NOT called", 
            (callCounter != 0 ));
        assertTrue("Check triggers was NOT called correct number of times", 
            (callCounter == 3 ));

        // now make sure that primary are called before secondary which are
        // before tertiary
        assertTrue("Check primaryOne > secondaryOne", 
            (primaryOne < secondaryOne));
        assertTrue("Check primaryOne > tertiaryOne", 
            (primaryOne < tertiaryOne));

        assertTrue("Check secondaryOne > tertiaryOne", 
            (secondaryOne < tertiaryOne));

    }

    /** 
     * Check that one can veto a property change
     */
    public void vetoChangeValueTest() throws Exception{
        String path= "test.mondrian.properties.veto.change.value"; 
        IntegerProperty intProp = new IntegerProperty(
                MondrianProperties.instance(),
                path,
                -1);

        assertTrue("Check property value NOT false", 
            (intProp.get() == -1));

        // now explicitly set the property
        MondrianProperties.instance().setProperty(path, "-1");
        
        String v = MondrianProperties.instance().getProperty(path);
        assertTrue("Check property value is null", 
            (v != null));

        assertTrue("Check property value is -1", 
            (Integer.decode(v).intValue() == -1));
        

        callCounter = 0;

        intProp.addTrigger(
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.PRIMARY_PHASE;
                    }
                    public void execute(Property property, String value) {
                        triggerCalled = true;
                        triggerValue = value;
                    }
                }
        );
        intProp.addTrigger(
                new Trigger() {
                    public boolean isPersistent() {
                        return false;
                    }
                    public int phase() {
                        return Trigger.SECONDARY_PHASE;
                    }
                    public void execute(Property property, String value) 
                            throws Trigger.VetoRT {

                        // even numbers are rejected
                        callCounter++;
                        int ival = Integer.decode(value).intValue();
                        if ((ival % 2) == 0) {
                            // throw on even
                            throw new Trigger.VetoRT("have a nice day");
                        } else {
                            // ok
                        }
                    }
                }
        );

        String falseStr = "false";
        String trueStr = "true";

        for (int i = 0; i < 10; i++) {
            // reset values
            triggerCalled = false;
            triggerValue = null;

            boolean isEven = ((i % 2) == 0);
            
            try {
                MondrianProperties.instance().setProperty(path, 
                        Integer.toString(i));
            } catch (Trigger.VetoRT ex) {
                // Trigger rejects even numbers so if even its ok
                if (! isEven) {
                    fail("Did not reject even number: " +i);
                }
                int val = Integer.decode(triggerValue).intValue();

                // the property value was reset to the previous value of "i"
                // so we add "1" to it to get the current value.
                assertTrue("Even counter not value plus one", (i == val+1));
                continue;
            }
            // should only be here if odd
            if (isEven) {
                fail("Did not pass odd number: " +i);
            }
            int val = Integer.decode(triggerValue).intValue();

            assertTrue("Odd counter not value", (i == val));

        }
        
    }
}
