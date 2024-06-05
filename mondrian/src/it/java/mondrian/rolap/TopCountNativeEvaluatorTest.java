/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2015-2024 Hitachi Vantara and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.calc.DummyExp;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.Literal;
import mondrian.olap.type.EmptyType;
import mondrian.test.FoodMartTestCase;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * This class contains tests for some cases related to creating
 * native evaluator for {@code TOPCOUNT} function.
 *
 * @author Andrey Khayrutdinov
 * @see RolapNativeTopCount#createEvaluator(RolapEvaluator, FunDef, Exp[])
 */
public class TopCountNativeEvaluatorTest extends FoodMartTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        propSaver.set(propSaver.properties.EnableNativeTopCount, true);
    }


    public void testNonNative_WhenExplicitlyDisabled() throws Exception {
        propSaver.set(propSaver.properties.EnableNativeTopCount, false);
        RolapNativeTopCount nativeTopCount = new RolapNativeTopCount();

        assertNull(
            "Native evaluator should not be created when "
            + "'mondrian.native.topcount.enable' is 'false'",
            nativeTopCount.createEvaluator(null, null, null));
    }

    public void testNonNative_WhenContextIsInvalid() throws Exception {
        RolapNativeTopCount nativeTopCount = createTopCountSpy();
        doReturn(false).when(nativeTopCount)
            .isValidContext(any(RolapEvaluator.class));

        assertNull(
            "Native evaluator should not be created when "
            + "evaluation context is invalid",
            nativeTopCount.createEvaluator(null, null, null));
    }

    /**
     * For now, prohibit native evaluation of the function if has two
     * parameters. According to the specification, this means
     * the function should behave similarly to {@code HEAD} function.
     * However, native evaluation joins data with the fact table and if there
     * is no data there, then some records are ignored, what is not correct.
     *
     * @see <a href="http://jira.pentaho.com/browse/MONDRIAN-2394">MONDRIAN-2394</a>
     */
    public void testNonNative_WhenTwoParametersArePassed() throws Exception {
        RolapNativeTopCount nativeTopCount = createTopCountSpy();
        doReturn(true).when(nativeTopCount)
            .isValidContext(any(RolapEvaluator.class));

        Exp[] arguments = new Exp[] {
            new DummyExp(new EmptyType()),
            Literal.create(BigDecimal.ONE)
        };

        assertNull(
            "Native evaluator should not be created when "
            + "two parameters are passed",
            nativeTopCount.createEvaluator(
                null, mockFunctionDef(), arguments));
    }

    private RolapNativeTopCount createTopCountSpy() {
        RolapNativeTopCount nativeTopCount = new RolapNativeTopCount();
        nativeTopCount = spy(nativeTopCount);
        return nativeTopCount;
    }

    private FunDef mockFunctionDef() {
        FunDef topCountFunMock = mock(FunDef.class);
        when(topCountFunMock.getName()).thenReturn("TOPCOUNT");
        return topCountFunMock;
    }
}

// End TopCountNativeEvaluatorTest.java