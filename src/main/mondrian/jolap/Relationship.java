/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

/**
 * Defines an association between two classes.
 *
 * <p>Instances of a relationship are held in {@link RelationshipList} or
 * {@link OrderedRelationshipList}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
class Relationship {
    Class fromClass;
    Class toClass;

    /** Constructs a two-way relationship. */
    public Relationship(Class fromClass, String name, Class toClass, String inverseName) {
        this.fromClass = fromClass;
        this.toClass = toClass;
    }
    /** Constructs a one-way relationship. */
    public Relationship(Class fromClass, String name, Class toClass) {
        this(fromClass, name, toClass, null);
    }
}

// End Relationship.java
