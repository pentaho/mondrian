/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 March, 2002
*/
package mondrian.olap.fun;

import mondrian.olap.Syntax;

/**
 * <code>ResolverBase</code> provides a skeleton implementation of
 * <code>interface {@link Resolver}</code>
 *
 * @author jhyde
 * @since 3 March, 2002
 * @version $Id$
 **/
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
    public String getSignature () {
        return signature;
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
