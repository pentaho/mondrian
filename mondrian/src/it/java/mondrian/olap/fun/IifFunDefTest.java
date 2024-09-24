/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2024 Hitachi Vantara..  All rights reserved.
*/
package mondrian.olap.fun;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import junit.framework.TestCase;
import mondrian.calc.Calc;
import mondrian.calc.ExpCompiler;
import mondrian.calc.ResultStyle;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Exp;
import mondrian.olap.FunDef;
import mondrian.olap.fun.SetFunDef.SetListCalc;
import mondrian.olap.type.SetType;

public class IifFunDefTest extends TestCase {

  private Exp logicalParamMock = mock( Exp.class );
  private Exp trueCaseParamMock = mock( Exp.class );
  private Exp falseCaseParamMock = mock( Exp.class );
  private FunDef funDefMock = mock( FunDef.class );
  private ExpCompiler compilerMock = mock( ExpCompiler.class );
  private Exp[] args = new Exp[] { logicalParamMock, trueCaseParamMock, falseCaseParamMock };
  private SetType setTypeMock = mock( SetType.class );
  private SetListCalc setListCalc;
  ResolvedFunCall call;

  @Override
  protected void setUp() throws Exception {
    when( trueCaseParamMock.getType() ).thenReturn( setTypeMock );
    setListCalc = new SetListCalc( trueCaseParamMock, new Exp[] { args[1] }, compilerMock, ResultStyle.LIST_MUTABLELIST );
    call = new ResolvedFunCall( funDefMock, args, setTypeMock );
    when( compilerMock.compileAs( any(), any(), any() ) ).thenReturn( setListCalc );
  }

  public void testGetResultType() {
    ResultStyle actualResStyle = null;
    ResultStyle expectedResStyle = setListCalc.getResultStyle();
    // Compile calculation for IIf function for (<Logical Expression>, <SetType>, <SetType>) params
    Calc calc = IifFunDef.SET_INSTANCE.compileCall( call, compilerMock );
    try {
      actualResStyle = calc.getResultStyle();
    } catch ( Exception e ) {
      fail( "Should not have thrown any exception." );
    }
    assertNotNull( actualResStyle );
    assertEquals( expectedResStyle, actualResStyle );

  }

}
