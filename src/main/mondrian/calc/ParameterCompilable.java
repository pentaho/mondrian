/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.calc;

import mondrian.olap.Parameter;

/**
 * Extension to {@link mondrian.olap.Parameter} which allows compilation.
 *
 * @author jhyde
 * @version $Id$
 * @since Jul 22, 2006
 */
public interface ParameterCompilable extends Parameter {
    Calc compile(ExpCompiler compiler);
}

// End ParameterCompilable.java
