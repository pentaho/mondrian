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

package mondrian.calc;

import mondrian.olap.Parameter;

/**
 * Extension to {@link mondrian.olap.Parameter} which allows compilation.
 *
 * @author jhyde
 * @since Jul 22, 2006
 */
public interface ParameterCompilable extends Parameter {
    Calc compile(ExpCompiler compiler);
}

// End ParameterCompilable.java
