/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import javax.swing.*;
import java.awt.*;

/**
 * <code>ListRenderer</code> ...
 *
 * @version $Id$
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
    public Component getListCellRendererComponent(JList list,
            Object value,
            int index,
            boolean isSelected,
            boolean cellHasFocus) {
        // Ask the standard renderer for what it thinks is right
        Component c = std.getListCellRendererComponent(list,
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
