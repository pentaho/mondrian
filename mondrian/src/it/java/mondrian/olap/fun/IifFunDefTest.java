/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
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
