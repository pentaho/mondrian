/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap;
import mondrian.olap.*;
import mondrian.olap.fun.MondrianEvaluationException;
import mondrian.rolap.agg.AggregationManager;
import mondrian.rolap.agg.CellRequest;

import java.util.*;

class RolapPosition extends Position
{
    // override Object
    public boolean equals(Object o)
    {
        if (o instanceof RolapPosition) {
            RolapPosition other = (RolapPosition) o;
            if (other.members.length == this.members.length) {
                for (int i = 0; i < this.members.length; i++) {
                    if (this.members[i] != other.members[i]) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }
    // override Object
    public int hashCode()
    {
        int h = 0;
        for (int i = 0; i < members.length; i++) {
            h = (h << 4) ^ members[i].hashCode();
        }
        return h;
    }
}

// End RolapPosition.java
