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


package mondrian.gui;

import java.awt.*;
import javax.swing.*;

/**
 * <code>ListRenderer</code> ...
 */
class ListRenderer implements ListCellRenderer {
    // The original ListCellRenderer we want to override
    ListCellRenderer std;

    public ListRenderer(ListCellRenderer override) {
        if (override == null) {
            throw new NullPointerException(
                "ListRenderer constructor: default renderer is null");
        }
        std = override;
    }

    // Override of getListCellRendererComponent.
    // This is called by the AWT event thread to paint components.
    public Component getListCellRendererComponent(
        JList list,
        Object value,
        int index,
        boolean isSelected,
        boolean cellHasFocus)
    {
        // Ask the standard renderer for what it thinks is right
        Component c =
            std.getListCellRendererComponent(
                list,
                value,
                index,
                isSelected,
                cellHasFocus);
        if (!isSelected) {
            // Set the background of the returned component to Aqua
            // striped background, but only for unselected cells;
            // The standard renderer functions as desired for
            // highlighted cells.
            c.setBackground((Color)UIManager.get("ComboBox.background"));
        }
        return c;
    }
}

// End ListRenderer.java
