/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2017 Hitachi Vantara and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
*/

package mondrian.gui;

import java.awt.*;
import java.io.StringReader;
import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.table.TableCellRenderer;

/**
 * @author sean
 */
public class SchemaPropertyCellRenderer
    extends javax.swing.table.DefaultTableCellRenderer
{
    Workbench workbench;

    JLabel stringRenderer;
    JCheckBox booleanRenderer;
    JLabel integerRenderer;
    JTable tableRenderer;
    JComboBox listRenderer;

    JScrollPane jScrollPaneCDATA;
    // JEditorPane jEditorPaneCDATA;
    JTextArea cdataTextArea;

    JComboBox relationList;  // Join, Table
    JTable relationTable;
    JPanel relationRenderer, rlPanel;

    JScrollPane jScrollPaneT;

    // All objects of this class will use this color value to render attribute
    // column this value is initialized by SchemaExplorer to the scrollpane
    // background color value.
    public static Color attributeBackground;

    /**
     * Creates a new instance of SchemaPropertyCellRenderer
     */
    public SchemaPropertyCellRenderer(Workbench wb) {
        workbench = wb;

        super.setBackground(attributeBackground);

        stringRenderer = new JLabel();
        stringRenderer.setFont(Font.decode("Dialog"));

        // cdata multi-line

        cdataTextArea = new JTextArea();
        cdataTextArea.setLineWrap(true);
        cdataTextArea.setWrapStyleWord(true);
        cdataTextArea.setLayout(new java.awt.BorderLayout());
        cdataTextArea.setEditable(true);
        cdataTextArea.setPreferredSize(new java.awt.Dimension(100, 300));
        cdataTextArea.setMinimumSize(new java.awt.Dimension(100, 100));

        jScrollPaneCDATA = new JScrollPane(cdataTextArea);
        jScrollPaneCDATA.setMaximumSize(cdataTextArea.getPreferredSize());

        booleanRenderer = new JCheckBox();
        booleanRenderer.setBackground(Color.white);
        integerRenderer = new JLabel();
        integerRenderer.setHorizontalAlignment(JTextField.RIGHT);
        integerRenderer.setFont(Font.decode("Courier"));


        listRenderer = new JComboBox(MondrianGuiDef.Measure._aggregator_values);
        listRenderer.setMaximumSize(stringRenderer.getMaximumSize());
        listRenderer.setFont(Font.decode("Dialog"));
        listRenderer.setBackground(Color.white);
        //listRenderer.setModel(new ComboBoxModel());
        listRenderer.setBorder(
            new EmptyBorder(
                0, 0, 0, 0)); //super.noFocusBorder);
        listRenderer.setRenderer(new ListRenderer(listRenderer.getRenderer()));

        /*
        relationListRenderer = new JComboBox(new String[] {"Join", "Table"});
        relationListRenderer.setMaximumSize(stringRenderer.getMaximumSize());
        relationListRenderer.setFont(Font.decode("Dialog"));
        relationListRenderer.setBackground(Color.white);
         */
        relationRenderer = new JPanel();

        rlPanel = new JPanel();
        relationList = new JComboBox(
            new String[]{
                workbench.getResourceConverter().getString(
                    "schemaPropertyCellRenderer.join", "Join"),
                workbench.getResourceConverter().getString(
                    "schemaPropertyCellRenderer.table", "Table")
            });
        relationList.setMaximumSize(new Dimension(55, 22));
        relationList.setPreferredSize(new Dimension(55, 22));
        relationList.setMinimumSize(new Dimension(55, 22));
        relationList.setFont(Font.decode("Dialog"));
        relationList.setBackground(Color.white);

        relationTable = new JTable();
        relationTable.setBackground(new java.awt.Color(255, 204, 204));

        // to remove table headers 'Property', 'Value''
        relationTable.setTableHeader(null);

        jScrollPaneT = new JScrollPane();
        jScrollPaneT.setViewportBorder(
            javax.swing.BorderFactory.createLineBorder(
                new java.awt.Color(
                    255, 0, 255), 2));
        jScrollPaneT.setVerticalScrollBarPolicy(
            ScrollPaneConstants.VERTICAL_SCROLLBAR_NEVER);
        jScrollPaneT.setViewportView(relationTable);

        relationRenderer.setLayout(new BorderLayout());
        rlPanel.add(relationList);
        relationRenderer.add(rlPanel, java.awt.BorderLayout.WEST);
        relationRenderer.add(jScrollPaneT, java.awt.BorderLayout.CENTER);


        relationRenderer.setBackground(Color.white);

        //relationRenderer.add(jScrollPaneT,java.awt.BorderLayout.CENTER);

        //JPanel relPanel = new JPanel();  // default flowlayout
        //relPanel.add(relationList);
        //relPanel.add(jScrollPaneT);
        //relationRenderer.add(relationTable);
        //relationRenderer.add(relPanel,java.awt.BorderLayout.CENTER);
        //relationRenderer.add(jScrollPaneT);

        tableRenderer = new JTable();
    }

    public JCheckBox getBooleanRenderer() {
        return booleanRenderer;
    }

    public Component getTableCellRendererComponent(
        JTable table,
        Object value,
        boolean isSelected,
        boolean hasFocus,
        int row,
        int column)
    {
        if (column == 1) {
            PropertyTableModel tableModel =
                (PropertyTableModel) table.getModel();
            Class targetClassz = tableModel.target.getClass();
            String propertyName = tableModel.getRowName(row);

            stringRenderer.setOpaque(false);
            stringRenderer.setToolTipText(null);
            stringRenderer.setBackground(Color.white);

            //targetClassz == MondrianGuiDef.Formula.class &&
            if (propertyName.equals("cdata")) {
                try {
                    cdataTextArea.read(new StringReader((String) value), null);
                } catch (Exception ex) {
                }
                return jScrollPaneCDATA;
            } else if (value instanceof String) {
                stringRenderer.setText((String) value);
                return stringRenderer;
            } else if (value instanceof Boolean) {
                booleanRenderer.setSelected((Boolean) value);
                return booleanRenderer;
            } else if (value instanceof Integer) {
                integerRenderer.setText(value.toString());
                return integerRenderer;
            } else if (value == null) {
                return null;
            } else if (value.getClass() == MondrianGuiDef.Join.class) {
                stringRenderer.setText(generateJoinStr(value));

                stringRenderer.setToolTipText(
                    workbench.getResourceConverter().getString(
                        "schemaPropertyCellRenderer.selectJoinObject",
                        "Select the Join/Table object from Schema tree to "
                        + "edit."));
                stringRenderer.setOpaque(true);
                stringRenderer.setBackground(Color.LIGHT_GRAY);
                return stringRenderer;

                /* 2: Displaying Join in nested pink boxes
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer();
                relationTable.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm =
                    new PropertyTableModel(value,SchemaExplorer.DEF_JOIN);
                relationTable.setModel(ptm);
                relationTable.getColumnModel().getColumn(0).setMaxWidth(100);
                relationTable.getColumnModel().getColumn(0).setMinWidth(100);
                setTableRendererHeight(relationTable, relationRenderer);
                return relationRenderer;
                 */
            } else if (value.getClass()
                       == MondrianGuiDef.OrdinalExpression.class)
            {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench,
                    ((MondrianGuiDef.OrdinalExpression) value).expressions[0],
                    SchemaExplorer.DEF_SQL);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                return tableRenderer;
            } else if (value.getClass()
                    == MondrianGuiDef.OrdinalExpression.class)
            {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench,
                    ((MondrianGuiDef.CaptionExpression) value).expressions[0],
                    SchemaExplorer.DEF_SQL);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                return tableRenderer;
            } else if (value.getClass() == MondrianGuiDef.Formula.class) {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_FORMULA);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                return tableRenderer;
            } else if (value.getClass()
                       == MondrianGuiDef.CalculatedMemberProperty.class)
            {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench,
                    value,
                    SchemaExplorer.DEF_CALCULATED_MEMBER_PROPERTY);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                return tableRenderer;
            } else if (value.getClass() == MondrianGuiDef.Table.class) {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_TABLE);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                return tableRenderer;
            } else if (value.getClass()
                       == MondrianGuiDef.RelationOrJoin.class)
            {
                // REVIEW: Covers View and InlineTable, since Table and Join are
                // managed above.
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_RELATION);
                tableRenderer.setModel(ptm);
                return tableRenderer;
            } else if (value.getClass() == MondrianGuiDef.AggFactCount.class) {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_AGG_FACT_COUNT);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                return tableRenderer;
            } else if (value.getClass() == MondrianGuiDef.Closure.class) {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_CLOSURE);
                tableRenderer.setModel(ptm);
                tableRenderer.getColumnModel().getColumn(0).setMaxWidth(100);
                tableRenderer.getColumnModel().getColumn(0).setMinWidth(100);
                setTableRendererHeight(tableRenderer, null);
                return tableRenderer;
            } else if (value.getClass() == MondrianGuiDef.Property.class) {
                SchemaPropertyCellRenderer spcr =
                    new SchemaPropertyCellRenderer(workbench);
                tableRenderer.setDefaultRenderer(Object.class, spcr);
                PropertyTableModel ptm = new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_PROPERTY);
                tableRenderer.setModel(ptm);
                return tableRenderer;
            } else {
                return null;
            }

        } else {
            if (value instanceof String) {
                // Use data from workbenchInfo.properties as tooltip when
                // available.
                PropertyTableModel tableModel =
                    (PropertyTableModel) table.getModel();
                String className = (tableModel.target.getClass()).getName();
                int pos = className.lastIndexOf("$");
                String tooltip = null;
                if (pos > 0) {
                    String tipName = (className.substring(pos + 1))
                                     + ","
                                     + tableModel.getRowName(row);
                    tooltip = workbench.getTooltip(tipName);
                }
                stringRenderer.setToolTipText(tooltip);
                stringRenderer.setText((String) value);
                stringRenderer.setOpaque(true);
                stringRenderer.setBackground(new java.awt.Color(221, 221, 221));
                if (isSelected && hasFocus) {
                    table.editCellAt(row, 1);
                }
                return stringRenderer;
            }
        }
        return super.getTableCellRendererComponent(
            table, value, isSelected, hasFocus, row, column);
    }

    private String generateJoinStr(Object value) {
        MondrianGuiDef.Join currentJoin = (MondrianGuiDef.Join) value;
        String joinStr = "<html>"
            + generateLeftRightStr(currentJoin.left)
            + " <b>JOIN</b> "
            + generateLeftRightStr(currentJoin.right)
            + "</html>";
        return joinStr;
    }

    private String generateLeftRightStr(Object value) {
        MondrianGuiDef.RelationOrJoin currentObj =
            (MondrianGuiDef.RelationOrJoin) value;
        if (currentObj instanceof MondrianGuiDef.Table) {
            return (((MondrianGuiDef.Table) currentObj).alias == null
                    || ((MondrianGuiDef.Table) currentObj).alias.equals("")
                ? ((MondrianGuiDef.Table) currentObj).name
                : ((MondrianGuiDef.Table) currentObj).alias);
        }
        MondrianGuiDef.Join currentJoin = (MondrianGuiDef.Join) currentObj;
        String joinStr = "("
            + generateLeftRightStr(currentJoin.left)
            + " <b>JOIN</b> "
            + generateLeftRightStr(currentJoin.right)
            + ")";
        return joinStr;
    }

    void setTableRendererHeight(JTable relationTable, JPanel relationRenderer) {
        int tableH = 0;
        int tableW = 0;
        Object value = null;
        for (int i = 0; i < relationTable.getRowCount(); i++) {
            TableCellRenderer renderer = relationTable.getCellRenderer(i, 1);
            Component comp = renderer.getTableCellRendererComponent(
                relationTable,
                relationTable.getValueAt(i, 1),
                false,
                false,
                i,
                1);
            try {
                int height = 0;
                int width = 0;
                if (comp != null) {
                    height = comp.getMaximumSize().height;
                    width = comp.getMaximumSize().width;
                    relationTable.setRowHeight(i, height);
                }

                value = relationTable.getValueAt(i, 1);
                if (value instanceof MondrianGuiDef.RelationOrJoin) {
                    tableH += comp.getPreferredSize().height;
                    tableW = Math.max(
                        tableW, comp.getPreferredSize().width + stringRenderer
                            .getMaximumSize().width);
                } else if (value == null) {
                    tableH += stringRenderer.getMaximumSize().height;
                    tableW = Math.max(
                        tableW, stringRenderer.getMaximumSize().width * 2);
                } else {
                    tableH += height;
                    tableW = Math.max(tableW, width * 2);
                }
            } catch (Exception ea) {
            }
        }
        // table height  changes
        relationTable.setPreferredSize(
            new Dimension(
                tableW, tableH));
        // scrollpane adjusts to new table height, it also changes scrollpanes'
        // preferred size values
        relationTable.setPreferredScrollableViewportSize(
            relationTable.getPreferredSize());
        if (relationRenderer != null) {
            relationRenderer.setPreferredSize(
                jScrollPaneT.getPreferredSize());
            relationRenderer.setMaximumSize(
                jScrollPaneT.getPreferredSize());
        }
    }
}

// End SchemaPropertyCellRenderer.java
