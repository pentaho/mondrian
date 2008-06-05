package mondrian.gui.validate.impl;

import mondrian.gui.I18n;
import mondrian.gui.validate.Messages;

/**
 * Implementation of <code>Messages</code> for Workbench.
 * 
 * @author mlowery
 */
public class WorkbenchMessages implements Messages {

    private I18n i18n;

    public WorkbenchMessages(I18n i18n) {
        super();
        this.i18n = i18n;
    }

    public String getFormattedString(String stringID, String defaultValue,
                    Object[] args) {
        return i18n.getFormattedString(stringID, defaultValue, args);
    }

    public String getString(String stringID, String defaultValue) {
        return i18n.getString(stringID, defaultValue);
    }

}
