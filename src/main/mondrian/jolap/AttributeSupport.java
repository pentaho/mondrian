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

import org.omg.java.cwm.objectmodel.core.*;

import java.util.Collection;

/**
 * Abstract implementation of {@link Attribute}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
abstract class AttributeSupport extends RefObjectSupport implements Attribute {
    public Expression getInitialValue() {
        throw new UnsupportedOperationException();
    }

    public void setInitialValue(Expression input) {
        throw new UnsupportedOperationException();
    }

    public ChangeableKind getChangeability() {
        throw new UnsupportedOperationException();
    }

    public void setChangeability(ChangeableKind value) {
        throw new UnsupportedOperationException();
    }

    public Multiplicity getMultiplicity() {
        throw new UnsupportedOperationException();
    }

    public void setMultiplicity(Multiplicity input) {
        throw new UnsupportedOperationException();
    }

    public OrderingKind getOrdering() {
        throw new UnsupportedOperationException();
    }

    public void setOrdering(OrderingKind value) {
        throw new UnsupportedOperationException();
    }

    public ScopeKind getTargetScope() {
        throw new UnsupportedOperationException();
    }

    public void setTargetScope(ScopeKind value) {
        throw new UnsupportedOperationException();
    }

    public void setType(Classifier input) {
        throw new UnsupportedOperationException();
    }

    public Classifier getType() {
        throw new UnsupportedOperationException();
    }

    public ScopeKind getOwnerScope() {
        throw new UnsupportedOperationException();
    }

    public void setOwnerScope(ScopeKind value) {
        throw new UnsupportedOperationException();
    }

    public void setOwner(Classifier input) {
        throw new UnsupportedOperationException();
    }

    public Classifier getOwner() {
        throw new UnsupportedOperationException();
    }

    public abstract String getName();

    public void setName(String input) {
        throw new UnsupportedOperationException();
    }

    public VisibilityKind getVisibility() {
        throw new UnsupportedOperationException();
    }

    public void setVisibility(VisibilityKind value) {
        throw new UnsupportedOperationException();
    }

    public Collection getClientDependency() {
        throw new UnsupportedOperationException();
    }

    public Collection getConstraint() {
        throw new UnsupportedOperationException();
    }

    public Collection getImporter() {
        throw new UnsupportedOperationException();
    }

    public void setNamespace(Namespace input) {
        throw new UnsupportedOperationException();
    }

    public Namespace getNamespace() {
        throw new UnsupportedOperationException();
    }
}

// End AttributeSupport.java
