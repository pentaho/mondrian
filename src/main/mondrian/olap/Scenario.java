/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
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
 * @see Connection#createScenario()
 * @see Connection#setScenario(Scenario)
 * @see Connection#getScenario()
 * @see Cell#setValue(Object, mondrian.olap.Cell.AllocationPolicy, Object[])
 * @see mondrian.olap.Cell.AllocationPolicy
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
