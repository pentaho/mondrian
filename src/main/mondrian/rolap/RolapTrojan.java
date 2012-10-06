/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

/**
 * Gives access to protected members, for testing purposes.
 *
 * <p>Methods in this class are subject to change without notice.
 *
 * @author jhyde
 */
public class RolapTrojan {
    public static final RolapTrojan INSTANCE = new RolapTrojan();

    private RolapTrojan() {
    }

    public RolapSchema.PhysExpr getAttributeNameExpr(RolapAttribute attribute) {
        return attribute.getNameExp();
    }
}

// End RolapTrojan.java
