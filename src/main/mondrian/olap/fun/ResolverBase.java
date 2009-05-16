/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.Syntax;
import mondrian.olap.FunDef;

/**
 * <code>ResolverBase</code> provides a skeleton implementation of
 * <code>interface {@link Resolver}</code>
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 */
abstract class ResolverBase extends FunUtil implements Resolver {
    private final String name;
    private final String signature;
    private final String description;
    private final Syntax syntax;

    ResolverBase(String name,
                 String signature,
                 String description,
                 Syntax syntax) {
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
