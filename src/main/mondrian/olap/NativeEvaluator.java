/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.calc.ResultStyle;

/**
 * Allows expressions to be evaluated native, e.g. in SQL.
 *
 * @author av
 * @since Nov 11, 2005
 * @version $Id$
 */

public interface NativeEvaluator {
    Object execute(ResultStyle resultStyle);
}

// End NativeEvaluator.java
