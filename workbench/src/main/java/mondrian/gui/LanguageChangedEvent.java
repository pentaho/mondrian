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

public class LanguageChangedEvent {

    private java.util.Locale locale;

    /** Creates a new instance of LanguageChangedEvent */
    public LanguageChangedEvent(java.util.Locale locale) {
        this.locale = locale;
    }

    /** Getter for property locale.
     * @return Value of property locale.
     *
     */
    public java.util.Locale getLocale() {
        return locale;
    }

    /** Setter for property locale.
     * @param locale New value of property locale.
     *
     */
    public void setLocale(java.util.Locale locale) {
        this.locale = locale;
    }

}

// End LanguageChangedEvent.java
