/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

/**
 * This interface provides a user exit to format
 * the cell value to be displayed. The user registers the CellFormatter's
 * full class name as an attribute of a Measure in the schema file.
 * A single instance of the CellFormatter is created for the Measure.
 * <p>
 * It is important that different CellFormatter's, CellFormatter that will
 * be used to format different Measures in different ways, implement
 * the <code>equals</code> and <code>hashCode</code> methods so that
 * the different CellFormatter are not treated as being the same in
 * a java.util.Collection.
 */
public interface CellFormatter {

    /**
     * user provided cell formatting function
     * @param value
     * @return the formatted value
     */
    String formatCell(Object value);

}

// End CellFormatter.java

