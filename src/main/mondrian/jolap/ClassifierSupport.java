/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import org.omg.java.cwm.objectmodel.core.Classifier;
import org.omg.java.cwm.objectmodel.core.Feature;
import org.omg.java.cwm.objectmodel.core.Namespace;
import org.omg.java.cwm.objectmodel.core.VisibilityKind;

import java.util.Collection;
import java.util.List;

/**
 * Abstract implementation of {@link Classifier}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class ClassifierSupport extends RefObjectSupport implements Classifier {
    protected OrderedRelationshipList feature = new OrderedRelationshipList(Meta.feature);

    static class Meta {
        static final Relationship feature = new Relationship(Classifier.class, "feature", Feature.class);
    }

    public boolean isAbstract() {
        throw new UnsupportedOperationException();
    }

    public void setAbstract(boolean input) {
        throw new UnsupportedOperationException();
    }

    public List getFeature() {
        return feature;
    }

    public Collection getOwnedElement() {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        throw new UnsupportedOperationException();
    }

    public void setName(String input) {
        throw new UnsupportedOperationException();
    }

    public VisibilityKind getVisibility() {
        throw new UnsupportedOperationException();
    }

    public void setVisibility(VisibilityKind input) {
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

// End ClassifierSupport.java
