package mondrian.calc.impl;

import junit.framework.TestCase;
import mondrian.olap.type.NullType;
import mondrian.olap.fun.FunUtil;

/**
 * Created by IntelliJ IDEA.
 * User: Brightondev
 * Date: Apr 2, 2007
 * Time: 1:31:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class ConstantCalcTest extends TestCase {
    public void testNullEvaluatesToConstantDoubleNull() {
        ConstantCalc constantCalc = new ConstantCalc(new NullType(), null);
        assertEquals(FunUtil.DoubleNull,constantCalc.evaluateDouble(null));
    }

    public void testNullEvaluatesToConstantIntegerNull() {
        ConstantCalc constantCalc = new ConstantCalc(new NullType(), null);
        assertEquals(FunUtil.IntegerNull,constantCalc.evaluateInteger(null));
    }
}
