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

import org.omg.cwm.objectmodel.instance.Slot;
import org.omg.cwm.objectmodel.core.Classifier;

import javax.olap.metadata.Dimension;
import javax.olap.metadata.Member;
import java.util.Collection;

/**
 * A <code>MondrianJolapMember</code> is ...
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 **/
class MondrianJolapMember extends ClassifierSupport implements Member {
	mondrian.olap.Member member;
	
	public MondrianJolapMember(Dimension dimension) {
	}

	// Object methods

	public void setSlot(Collection input) {
		throw new UnsupportedOperationException();
	}

	public Collection getSlot() {
		throw new UnsupportedOperationException();
	}

	public void removeSlot(Slot input) {
		throw new UnsupportedOperationException();
	}

	// Instance methods

	public void setClassifier(Classifier input) {
		throw new UnsupportedOperationException();
	}

	public Classifier getClassifier() {
		throw new UnsupportedOperationException();
	}
}

// End MondrianJolapMember.java