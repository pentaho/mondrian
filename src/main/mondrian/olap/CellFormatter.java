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
 * this interface provides a user exit to format
 * the cell value beeing beeing displayed.
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

