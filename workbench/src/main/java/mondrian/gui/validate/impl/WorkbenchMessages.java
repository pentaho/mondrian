/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.gui.validate.impl;

import mondrian.gui.I18n;
import mondrian.gui.validate.Messages;

/**
 * Implementation of <code>Messages</code> for Workbench.
 *
 * @author mlowery
 */
public class WorkbenchMessages implements Messages {

    private final I18n i18n;

    /**
     * Creates a WorkbenchMessages.
     *
     * @param i18n Resources
     */
    public WorkbenchMessages(I18n i18n) {
        super();
        this.i18n = i18n;
    }

    public String getFormattedString(
        String stringId,
        String defaultValue,
        Object... args)
    {
        return i18n.getFormattedString(stringId, defaultValue, args);
    }

    public String getString(String stringID, String defaultValue) {
        return i18n.getString(stringID, defaultValue);
    }
}

// End WorkbenchMessages.java
