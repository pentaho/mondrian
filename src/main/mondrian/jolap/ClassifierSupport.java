/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 24, 2002
*/
package mondrian.jolap;

import org.omg.cwm.objectmodel.core.*;
import org.omg.cwm.objectmodel.core.Package;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

/**
 * A <code>ClassifierSupport</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class ClassifierSupport implements Classifier {
	protected OrderedRelationshipList feature = new OrderedRelationshipList(Meta.feature);

	static class Meta {
		static final Relationship feature = new Relationship(Classifier.class, "feature", Feature.class);
	}

	public Boolean getIsAbstract() {
		throw new UnsupportedOperationException();
	}

	public void setIsAbstract(Boolean input) {
		throw new UnsupportedOperationException();
	}

	public void setFeature(Collection input) {
		feature.set(input);
	}

	public List getFeature() {
		return feature.get();
	}

	public void removeFeature(Feature input) {
		feature.remove(input);
	}

	public void moveFeatureBefore(Feature before, Feature input) {
		feature.moveBefore(before, input);
	}

	public void moveFeatureAfter(Feature before, Feature input) {
		feature.moveAfter(before, input);
	}

	public void setOwnedElement(Collection input) {
		throw new UnsupportedOperationException();
	}

	public Collection getOwnedElement() {
		throw new UnsupportedOperationException();
	}

	public void removeOwnedElement(ModelElement input) {
		throw new UnsupportedOperationException();
	}

	public String getName() {
		throw new UnsupportedOperationException();
	}

	public void setName(String input) {
		throw new UnsupportedOperationException();
	}

	public String getVisibility() {
		throw new UnsupportedOperationException();
	}

	public void setVisibility(String input) {
		throw new UnsupportedOperationException();
	}

	public void setClientDependency(Collection input) {
		throw new UnsupportedOperationException();
	}

	public Collection getClientDependency() {
		throw new UnsupportedOperationException();
	}

	public void addClientDependency(Dependency input) {
		throw new UnsupportedOperationException();
	}

	public void removeClientDependency(Dependency input) {
		throw new UnsupportedOperationException();
	}

	public void setConstraint(Collection input) {
		throw new UnsupportedOperationException();
	}

	public Collection getConstraint() {
		throw new UnsupportedOperationException();
	}

	public void addConstraint(Constraint input) {
		throw new UnsupportedOperationException();
	}

	public void removeConstraint(Constraint input) {
		throw new UnsupportedOperationException();
	}

	public void setImporter(Collection input) {
		throw new UnsupportedOperationException();
	}

	public Collection getImporter() {
		throw new UnsupportedOperationException();
	}

	public void addImporter(Package input) {
		throw new UnsupportedOperationException();
	}

	public void removeImporter(Package input) {
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
