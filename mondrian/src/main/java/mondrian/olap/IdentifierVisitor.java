/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2017 Hitachi Vantara and others
// All Rights Reserved.
 */
package mondrian.olap;

import mondrian.mdx.*;

import java.util.*;

public class IdentifierVisitor extends MdxVisitorImpl {
    private final Set<Id> identifiers;

    public IdentifierVisitor(Set<Id> identifiers) {
        this.identifiers = identifiers;
    }

    public Object visit(Id id) {
        identifiers.add(id);
        return null;
    }
}
// End IdentifierVisitor.java
