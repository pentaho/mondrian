// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007 JasperSoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.

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
