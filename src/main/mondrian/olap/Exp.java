/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;

import mondrian.olap.type.Type;
import java.io.PrintWriter;

/**
 * An <code>Exp</code> is an MDX expression.
 **/
public interface Exp {

    Object clone();

    /**
     * Returns the {@link Category} of the expression.
     * @post Category.instance().isValid(return)
     **/
    int getCategory();

    /**
     * @deprecated Use {@link #getCategory()}
     **/
    int getType();

    /**
     * Returns the type of this expression. Never null.
     */
    Type getTypeX();

    boolean isSet();

    boolean isMember();

    boolean isElement();

    boolean isEmptySet();

    void unparse(PrintWriter pw);

    Exp resolve(Validator resolver);

    /**
     * true means that the result of this expression will be different
     * for different members of <code>dimension</code> in the evaluation
     * context.
     * <p>
     * For example, the expression
     * <code>[Measures].[Unit Sales]</code> depends on all dimensions
     * except Measures. The boolean expression
     * <code>([Measures].[Unit Sales], [Time].[1997]) &gt; 1000</code>
     * depends on all dimensions except Measures and Time.
     */
    boolean dependsOn(Dimension dimension);

    /**
     * Adds 'exp' as the right child of the CrossJoin whose left child has
     * 'iPosition' hierarchies (hence 'iPosition' - 1 CrossJoins) under it.  If
     * added successfully, returns -1, else returns the number of hierarchies
     * under this node.
     **/
    int addAtPosition(Exp e, int iPosition);

    Object evaluate(Evaluator evaluator);

    Object evaluateScalar(Evaluator evaluator);
}

// End Exp.java
