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

/**
 * A <code>AttributeSupport</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
abstract class AttributeSupport implements Attribute {
	public Expression getInitialValue() {
		throw new UnsupportedOperationException();
	}

	public void setInitialValue(Expression input) {
		throw new UnsupportedOperationException();
	}

	public String getChangeability() {
		throw new UnsupportedOperationException();
	}

	public void setChangeability(String input) {
		throw new UnsupportedOperationException();
	}

	public Multiplicity getMultiplicity() {
		throw new UnsupportedOperationException();
	}

	public void setMultiplicity(Multiplicity input) {
		throw new UnsupportedOperationException();
	}

	public String getOrdering() {
		throw new UnsupportedOperationException();
	}

	public void setOrdering(String input) {
		throw new UnsupportedOperationException();
	}

	public String getTargetScope() {
		throw new UnsupportedOperationException();
	}

	public void setTargetScope(String input) {
		throw new UnsupportedOperationException();
	}

	public void setType(Classifier input) {
		throw new UnsupportedOperationException();
	}

	public Classifier getType() {
		throw new UnsupportedOperationException();
	}

	public String getOwnerScope() {
		throw new UnsupportedOperationException();
	}

	public void setOwnerScope(String input) {
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

// End AttributeSupport.java