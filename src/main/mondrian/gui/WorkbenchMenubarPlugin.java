/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.gui;

import javax.swing.*;

public interface WorkbenchMenubarPlugin {
    public void addItemsToMenubar(JMenuBar menubar);
    public void setWorkbench(Workbench workbench);
}

// End WorkbenchMenubarPlugin.java
