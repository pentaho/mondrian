/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package mondrian.olap.type;

/**
 * The type of a symbolic expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class SymbolType extends ScalarType {

    /**
     * Creates a symbol type.
     */
    public SymbolType() {
        super("SYMBOL");
    }
}

// End SymbolType.java
