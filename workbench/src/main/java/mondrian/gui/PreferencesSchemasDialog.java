/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
*/

package mondrian.gui;

import java.awt.*;
import java.awt.event.*;
import java.util.Arrays;
import java.util.List;
import javax.swing.*;
import javax.swing.table.TableModel;

/**
 *
 * @author swood
 */
public class PreferencesSchemasDialog extends JDialog {

    PreferencesDialog preferences = null;
    JdbcMetaData jdbcMetadata = null;
    String selectedSchemaString = null;
    boolean accepted = false;

    public PreferencesSchemasDialog() {
        initComponents();
    }

    public PreferencesSchemasDialog(
        PreferencesDialog preferences,
        JdbcMetaData jdbcMetadata)
    {
        this.preferences = preferences;
        this.jdbcMetadata = jdbcMetadata;
        initComponents();
    }

    private void initComponents() {
        jScrollPane1 = new javax.swing.JScrollPane();
        jTable1 = new javax.swing.JTable();
        cancelButton = new javax.swing.JButton();
        okButton = new javax.swing.JButton();

        setLayout(new java.awt.GridBagLayout());
        addWindowListener(
            new WindowAdapter() {
                public void windowClosing(WindowEvent evt) {
                    closeDialog(evt);
                }
            });

        jTable1.setModel(getSchemaTableModel());
        jTable1.setRowSelectionAllowed(false);
        jScrollPane1.setViewportView(jTable1);

        GridBagConstraints gridBagConstraints = new GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 0;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.NORTH;
        gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);

        add(jScrollPane1, gridBagConstraints);

        cancelButton.setText(
            getResourceConverter().getString(
                "preferences.cancelButton.title",
                "Cancel"));
        cancelButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    closeDialog(evt);
                }
            });

        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 1;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.SOUTHEAST;
        gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);
        add(cancelButton, gridBagConstraints);

        cancelButton.setText(
            getResourceConverter().getString(
                "preferences.okButton.title",
                "OK"));
        okButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    acceptButtonActionPerformed(evt);
                }
            });

        gridBagConstraints = new java.awt.GridBagConstraints();
        gridBagConstraints.gridx = 0;
        gridBagConstraints.gridy = 1;
        gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
        gridBagConstraints.anchor = java.awt.GridBagConstraints.SOUTHWEST;
        gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);
        add(okButton, gridBagConstraints);
        pack();
    }

    private void acceptButtonActionPerformed(ActionEvent evt) {
        TableModel tm = jTable1.getModel();

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < tm.getRowCount(); i++) {
            Boolean selected = (Boolean) tm.getValueAt(i, 0);

            if (selected) {
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append((String) tm.getValueAt(i, 1));
            }
        }

        selectedSchemaString = sb.toString();

        accepted = true;
        setVisible(false);
        dispose();
    }

    private void closeDialog(WindowEvent evt) {
        setVisible(false);
        dispose();
    }

    private void closeDialog(ActionEvent evt) {
        setVisible(false);
        dispose();
    }

    public boolean isAccepted() {
        return accepted;
    }

    public TableModel getSchemaTableModel() {
        List<String> allSchemaNames = jdbcMetadata.listAllSchemas();

        String currentSchemas = preferences.getDatabaseSchema();

        boolean selectedSchemas[] = new boolean[allSchemaNames.size()];

        Arrays.fill(selectedSchemas, false);

        // Validate entered schemas
        if (currentSchemas != null && currentSchemas.trim().length() > 0) {
            String schemasArray[] = currentSchemas.trim().split("[,;]");

            for (int i = 0; i < schemasArray.length; i++) {
                // trim the names, removing empties
                String enteredSchemaName = schemasArray[i].trim();

                if (enteredSchemaName.length() > 0) {
                    for (int j = 0; j < allSchemaNames.size(); j++) {
                        String actualSchemaName = allSchemaNames.get(j);
                        if (actualSchemaName.equalsIgnoreCase(
                                enteredSchemaName))
                        {
                            selectedSchemas[j] = true;
                            break;
                        }
                    }
                }
            }
        }

        Object [][] table = new Object [allSchemaNames.size()][2];

        for (int i = 0; i < allSchemaNames.size(); i++) {
            table[i][0] = new Boolean(selectedSchemas[i]);
            table[i][1] = allSchemaNames.get(i);
        }

        return new javax.swing.table.DefaultTableModel(
            table,
            new String [] {
                "Select", "Schema"
            })
        {
            Class[] types = {Boolean.class, String.class};
            boolean[] canEdit = {true, false};

            public Class getColumnClass(int columnIndex) {
                return types[columnIndex];
            }

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit[columnIndex];
            }
        };
    }

    /**
     * @return the workbench i18n converter
     */
    public I18n getResourceConverter() {
        return preferences.getResourceConverter();
    }

    private javax.swing.JButton cancelButton;
    private javax.swing.JButton okButton;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JTable jTable1;
}

// End PreferencesSchemasDialog.java
