/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2006 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.olap.FunDef;
import mondrian.olap.Syntax;

/**
 * <code>ResolverBase</code> provides a skeleton implementation of
 * <code>interface {@link Resolver}</code>
 *
 * @author jhyde
 * @since 3 March, 2002
 */
abstract class ResolverBase extends FunUtil implements Resolver {
    private final String name;
    private final String signature;
    private final String description;
    private final Syntax syntax;

    ResolverBase(
        String name,
        String signature,
        String description,
        Syntax syntax)
    {
        this.name = name;
        this.signature = signature;
        this.description = description;
        this.syntax = syntax;
    }

    public String getName() {
        return name;
    }

    public String getSignature() {
        return signature;
    }

    public FunDef getFunDef() {
        return null;
    }

    public String getDescription() {
        return description;
    }

    public Syntax getSyntax() {
        return syntax;
    }

    public boolean requiresExpression(int k) {
        return false;
    }

    public String[] getReservedWords() {
        return emptyStringArray;
    }
}

// End ResolverBase.java
