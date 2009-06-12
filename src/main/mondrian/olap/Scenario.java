/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2009-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * Context for a set of writeback operations.
 *
 * <p>An analyst performing a what-if analysis would first create a scenario,
 * or open an existing scenario, then modify a sequence of cell values.
 *
 * <p>Some OLAP engines allow scenarios to be saved (to a file, or perhaps to
 * the database) and restored in a future session.
 *
 * <p>Multiple scenarios may be open at the same time, by different users of
 * the OLAP engine.
 *
 * <p>TODO: move to olap4j
 *
 * @author jhyde
 * @since 24 April, 2009
 * @version $Id$
 */
public interface Scenario {
    /**
     * Returns the unique identifier of the scenario.
     */
    int getId();
}

// End Scenario.java
