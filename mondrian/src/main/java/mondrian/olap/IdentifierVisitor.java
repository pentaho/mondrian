/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


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
