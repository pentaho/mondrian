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

import org.omg.java.cwm.objectmodel.core.Classifier;

import javax.olap.metadata.Dimension;
import javax.olap.metadata.Member;
import java.util.Collection;

/**
 * Implementation of {@link Member JOLAP Member} based upon a
 * {@link mondrian.olap.Member Mondrian Member}.
 *
 * @author jhyde
 * @since Dec 24, 2002
 * @version $Id$
 */
class MondrianJolapMember extends ClassifierSupport implements Member {
    mondrian.olap.Member member;

    public MondrianJolapMember(Dimension dimension) {
    }

    // Object methods

    public Collection getSlot() {
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
