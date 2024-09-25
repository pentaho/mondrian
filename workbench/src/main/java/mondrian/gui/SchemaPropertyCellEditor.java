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

import mondrian.gui.MondrianGuiDef.Hierarchy;
import mondrian.olap.Id;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;

import org.eigenbase.xom.NodeDef;

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;
import java.util.List;
import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.text.JTextComponent;

/**
 * @author sean
 */
public class SchemaPropertyCellEditor
    implements javax.swing.table.TableCellEditor
{
    Workbench workbench;

    final List<CellEditorListener> listeners =
        new ArrayList<CellEditorListener>();

    JTextField stringEditor;
    JTextArea cdataTextArea;
    JScrollPane jScrollPaneCDATA;
    // JEditorPane jEditorPaneCDATA;
    JCheckBox booleanEditor;
    JTextField integerEditor;
    JTable tableEditor;
    Component activeEditor;
    JComboBox listEditor;
    JTable relationTable;
    JPanel relationRenderer;

    JdbcMetaData jdbcMetaData;
    ComboBoxModel allOptions, selOptions;
    String listEditorValue;
    MouseListener ml;
    ItemListener il;
    ActionListener al;

    String noSelect = "-- No Selection --";
    FocusAdapter editorFocus;

    Object originalValue;

    public SchemaPropertyCellEditor(
        Workbench workbench)
    {
        this(workbench, null);
    }

    /**
     * Creates a new instance of SchemaPropertyCellEditor
     */
    public SchemaPropertyCellEditor(
        Workbench workbench,
        JdbcMetaData jdbcMetaData)
    {
        this.workbench = workbench;
        this.jdbcMetaData = jdbcMetaData;

        noSelect = getResourceConverter().getString(
            "schemaPropertyCellEditor.noSelection", noSelect);

        stringEditor = new JTextField();
        stringEditor.setFont(Font.decode("Dialog"));
        stringEditor.setBorder(null);

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

        booleanEditor = new JCheckBox();
        booleanEditor.setBackground(Color.white);

        integerEditor = new JTextField();
        integerEditor.setBorder(null);
        integerEditor.setHorizontalAlignment(JTextField.RIGHT);
        integerEditor.setFont(Font.decode("Courier"));

        tableEditor = new JTable();

        listEditor = new JComboBox();
        listEditor.setEditable(true);
        listEditor.setMaximumSize(stringEditor.getMaximumSize());
        listEditor.setFont(Font.decode("Dialog"));
        listEditor.setBackground(Color.white);
        listEditor.setBorder(
            new EmptyBorder(
                0, 0, 0, 0)); //super.noFocusBorder);

        al = new ActionListener() {
            boolean all = true;

            public void actionPerformed(ActionEvent e) {
                if (e.getActionCommand().equals("comboBoxChanged")
                    && listEditor.getSelectedIndex() == 0)
                {   // 0 index refers to less or more options
                    if (all) {
                        listEditor.setModel(allOptions);
                    } else {
                        listEditor.setModel(selOptions);
                    }
                    listEditor.setSelectedIndex(-1);
                    all = !all;
                }
                // Must invoke later on the GUI thread since trying
                // now will fail. The component is already marked
                // as 'dirty'.
                SwingUtilities.invokeLater(
                    new Runnable() {
                        public void run() {
                            if (listEditor.isDisplayable()) {
                                listEditor.setPopupVisible(true);
                            }
                        }
                    });
            }
        };

        JTextComponent editor =
            (JTextComponent) listEditor.getEditor().getEditorComponent();

        editor.addMouseListener(
            new MouseAdapter() {
                public void mousePressed(MouseEvent e) {
                    if (listEditor.isDisplayable()) {
                        listEditor.setPopupVisible(true);
                    }
                }
            });

        editor.addKeyListener(
            new KeyAdapter() {
                public void keyPressed(KeyEvent e) {
                    if (listEditor.isDisplayable()) {
                        listEditor.setPopupVisible(true);
                    }
                }

                public void keyReleased(KeyEvent e) {
                    // listEditor.setSelectedItem(
                    //   ((JTextComponent) e.getSource()).getText());
                    if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
                        listEditor.setSelectedItem(listEditorValue);
                        listEditor.getEditor().setItem(listEditorValue);
                    }
                }
            });

        // Not used

//        relationRenderer = new JPanel();
//
//        relationList = new JComboBox(
//            new String[] {
//                getResourceConverter().getString(
//                    "schemaPropertyCellEditor.join","Join"),
//                 getResourceConverter().getString(
//                     "schemaPropertyCellEditor.table","Table")});
//        relationList.setMaximumSize(stringEditor.getMaximumSize());
//        relationTable = new JTable();
//        relationRenderer.add(relationList);
//        relationRenderer.add(relationTable);
    }


    public MondrianGuiDef.RelationOrJoin getRelation(
        final JTable table,
        final int row,
        final int column)
    {
        PropertyTableModel tableModel = (PropertyTableModel) table.getModel();
        Object value = tableModel.getValue();
        Class<?> targetClassz = tableModel.target.getClass();
        Object parent = this.getParentObject();

        MondrianGuiDef.RelationOrJoin relation = null;

        if (targetClassz == MondrianGuiDef.Table.class) {
            relation = (MondrianGuiDef.Table) value;
        } else if (targetClassz == MondrianGuiDef.View.class) {
            relation = (MondrianGuiDef.View) value;
        } else if (targetClassz == MondrianGuiDef.Join.class) {
            relation = (MondrianGuiDef.Join) value;
        } else if (targetClassz == MondrianGuiDef.Hierarchy.class) {
            MondrianGuiDef.Hierarchy hProps = (MondrianGuiDef.Hierarchy) value;
            relation = hProps.relation;
        } else if (targetClassz == MondrianGuiDef.Level.class) {
            MondrianGuiDef.Hierarchy hProps = (MondrianGuiDef.Hierarchy) parent;
            relation = hProps.relation;
        } else if (targetClassz == MondrianGuiDef.Cube.class) {
            MondrianGuiDef.Cube hProps = (MondrianGuiDef.Cube) value;
            relation = hProps.fact;
        }

        return relation;
    }

    public Component getTableCellEditorComponent(
        final JTable table,
        Object value,
        boolean isSelected,
        final int row,
        final int column)
    {
        PropertyTableModel tableModel = (PropertyTableModel) table.getModel();
        Class<?> parentClassz = null;
        if (tableModel.getParentTarget() != null) {
            parentClassz = tableModel.getParentTarget().getClass();
        }
        Class<?> targetClassz = tableModel.target.getClass();
        String propertyName = tableModel.getRowName(row);
        String selectedFactTable = tableModel.getFactTable();
        String selectedFactTableSchema = tableModel.getFactTableSchema();
        listEditorValue = null;  // reset value of combo-box
        Object parent = this.getParentObject();

        MondrianGuiDef.RelationOrJoin currentRelation =
            getRelation(table, row, column);

        boolean nonTableRelation =
            currentRelation != null
            && !(currentRelation instanceof MondrianGuiDef.Table
                 || currentRelation instanceof MondrianGuiDef.Join);

        if (targetClassz == MondrianGuiDef.UserDefinedFunction.class
            && propertyName.equals("className"))
        {
            List<String> udfs = getUdfs();
            ComboBoxModel cAlludfs =
                new DefaultComboBoxModel(new Vector<String>(udfs));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAlludfs);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.Measure.class
                   && propertyName.equals("formatString"))
        {
            List<String> formatStrs = getFormatStrings();
            ComboBoxModel cAllformatStrs =
                new DefaultComboBoxModel(new Vector<String>(formatStrs));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllformatStrs);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.Measure.class
                   && propertyName.equals("aggregator"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Measure._aggregator_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Measure.class
                   && propertyName.equals("datatype"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Measure._datatype_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Parameter.class
                   && propertyName.equals("parameter"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Parameter._type_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.SQL.class
                   && propertyName.equals("dialect"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            String[] dialects = MondrianGuiDef.SQL._dialect_values.clone();
            Arrays.sort(dialects);
            listEditor.setModel(
                new DefaultComboBoxModel(dialects));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class
                   && propertyName.equals("hideMemberIf"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Level._hideMemberIf_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class
                   && propertyName.equals("levelType"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Level._levelType_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class
                   && propertyName.equals("type"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(MondrianGuiDef.Level._type_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class
            && propertyName.equals("internalType"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Level._internalType_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Dimension.class
                   && propertyName.equals("type"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(
                    MondrianGuiDef.Dimension._type_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.DimensionUsage.class
                   && propertyName.equals("source"))
        {
            List<String> source = getSource();
            ComboBoxModel cAllsource =
                new DefaultComboBoxModel(new Vector<String>(source));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;

        } else if ((tableModel.target instanceof MondrianGuiDef.Grant
            || tableModel.target instanceof MondrianGuiDef.MemberGrant)
            && propertyName.equals("access"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            ComboBoxModel cAccess =
                new DefaultComboBoxModel(
                    new String[]{"all", "none"});

            if (targetClassz == MondrianGuiDef.SchemaGrant.class) {
                cAccess = new DefaultComboBoxModel(
                    new String[]{
                        "all", "custom", "none", "all_dimensions"
                    });
            } else if (targetClassz == MondrianGuiDef.CubeGrant.class
                       || targetClassz == MondrianGuiDef.DimensionGrant.class
                       || targetClassz == MondrianGuiDef.MemberGrant.class)
            {
                cAccess =
                    new DefaultComboBoxModel(
                        new String[]{"all", "custom", "none"});

            } else if (targetClassz == MondrianGuiDef.HierarchyGrant.class
                || targetClassz == MondrianGuiDef.DimensionGrant.class)
            {
                cAccess = new DefaultComboBoxModel(
                    new String[]{
                        "all", "custom", "none"
                    });
            }
            listEditor.setModel(cAccess);

            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.HierarchyGrant.class
            && propertyName.equals("rollupPolicy"))
        {
            ComboBoxModel cRollupPolicy =
                new DefaultComboBoxModel(
                    new String[]{"full", "partial", "hidden"});
            listEditor.setModel(cRollupPolicy);
            listEditor.setSelectedItem(value);
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.DimensionGrant.class
                    && propertyName.equals("dimension"))
        {
            List<String> source = getDimensions();
            ComboBoxModel cAllsource =
                new DefaultComboBoxModel(new Vector<String>(source));

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.HierarchyGrant.class
                       && propertyName.equals("hierarchy"))
        {
            List<String> source = getHierarchies();
            ComboBoxModel cAllsource =
                new DefaultComboBoxModel(new Vector<String>(source));

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;

        } else if ((targetClassz == MondrianGuiDef.HierarchyGrant.class
                    && (propertyName.equals("topLevel")
                        || propertyName.equals("bottomLevel"))))
        {
            List<String> source = getLevels(
                ((MondrianGuiDef.HierarchyGrant) tableModel.target).hierarchy);
            ComboBoxModel cAllsource =
                new DefaultComboBoxModel(new Vector<String>(source));

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;

        } else if (((targetClassz == MondrianGuiDef.VirtualCubeDimension.class
                     || targetClassz == MondrianGuiDef.VirtualCubeMeasure.class)
                    && propertyName.equals("cubeName"))
                   || (targetClassz == MondrianGuiDef.CubeGrant.class
                       && propertyName.equals("cube")))
        {
            List<String> source = getCubes();
            ComboBoxModel cAllsource =
                new DefaultComboBoxModel(new Vector<String>(source));

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;
        } else if ((targetClassz == MondrianGuiDef.Dimension.class
                    && propertyName.equals("foreignKey"))
                   || (targetClassz == MondrianGuiDef.DimensionUsage.class
                       && propertyName.equals("foreignKey"))
                   || (targetClassz == MondrianGuiDef.Measure.class
                       && propertyName.equals("column")))
        {
            Vector<String> fks = new Vector<String>(
                jdbcMetaData.getFactTableFKs(
                    selectedFactTableSchema, selectedFactTable));
            fks.add(
                0, getResourceConverter().getString(
                    "schemaPropertyCellEditor.allColumns",
                    "<< All Columns >>"));
            Vector<String> allcols = new Vector<String>(
                jdbcMetaData.getAllColumns(
                    selectedFactTableSchema, selectedFactTable));
            ComboBoxModel cFks = new DefaultComboBoxModel(fks);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            if ((fks.size() > 1) && propertyName.equals("foreignKey")) {
                allcols.add(
                    0,
                    getResourceConverter().getString(
                        "schemaPropertyCellEditor.foreignKeys",
                        "<< Foreign keys >>"));
                ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);
                listEditor.setModel(cFks);
                selOptions = cFks;
                allOptions = cAllcols;
                listEditor.addActionListener(al);
            } else {
                ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);
                listEditor.setModel(cAllcols);
            }

            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.Hierarchy.class
                   && propertyName.equals("primaryKey"))
        {
            MondrianGuiDef.Hierarchy hProps =
                (MondrianGuiDef.Hierarchy) tableModel.getValue();
            String pkTable = hProps.primaryKeyTable;

            String schemaName = null;

            String pk = "";
            List<String> allcols;

            MondrianGuiDef.RelationOrJoin relation = hProps.relation;
            if (relation instanceof MondrianGuiDef.Table) {
                pkTable = ((MondrianGuiDef.Table) relation).name;
                schemaName = ((MondrianGuiDef.Table) relation).schema;
                pk = jdbcMetaData.getTablePK(schemaName, pkTable);
            } else if (relation instanceof MondrianGuiDef.Join) {
                String[] schemaAndTable =
                    SchemaExplorer.getTableNameForAlias(
                        hProps.relation, pkTable);
                schemaName = schemaAndTable[0];
                pkTable = schemaAndTable[1];
            }

            if (relation instanceof MondrianGuiDef.Table
                || relation instanceof MondrianGuiDef.Join)
            {
                allcols = jdbcMetaData.getAllColumns(schemaName, pkTable);
                pk = jdbcMetaData.getTablePK(schemaName, pkTable);
            } else {
                allcols = Collections.emptyList();
            }

            ComboBoxModel cAllcols =
                new DefaultComboBoxModel(new Vector<String>(allcols));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllcols);
            if (value == null || ((String) value).equals("")) {
                listEditor.setSelectedItem(pk);
            } else {
                listEditor.setSelectedItem((String) value);
                listEditorValue = (String) value;
            }
            activeEditor = listEditor;
        } else if ((targetClassz == MondrianGuiDef.Level.class
                    && propertyName.equals("column"))
                   || (targetClassz == MondrianGuiDef.Level.class
                       && propertyName.equals("nameColumn"))
                   || (targetClassz == MondrianGuiDef.Level.class
                       && propertyName.equals("parentColumn"))
                   || (targetClassz == MondrianGuiDef.Level.class
                       && propertyName.equals("ordinalColumn"))
                   || (targetClassz == MondrianGuiDef.Level.class
                       && propertyName.equals("captionColumn"))
                   || (targetClassz == MondrianGuiDef.Closure.class
                       && propertyName.equals("parentColumn"))
                   || (targetClassz == MondrianGuiDef.Closure.class
                       && propertyName.equals("childColumn"))
                   || (targetClassz == MondrianGuiDef.Property.class
                       && propertyName.equals("column")))
        {
            MondrianGuiDef.Level lProps;
            if (targetClassz == MondrianGuiDef.Level.class) {
                lProps = (MondrianGuiDef.Level) tableModel.getValue();
            } else {
                lProps = (MondrianGuiDef.Level) this.getParentObject();
            }

            String schemaName = null;
            String lTable = lProps.table;
            List<String> allcols;

            // Sets the corresponding columns on the selection dropdown for the
            // specified table.
            if (targetClassz == MondrianGuiDef.Level.class && parent != null) {
                if (parent instanceof MondrianGuiDef.Hierarchy) {
                    MondrianGuiDef.RelationOrJoin relation =
                        ((MondrianGuiDef.Hierarchy) parent).relation;
                    if (relation instanceof MondrianGuiDef.Table) {
                        lTable = ((MondrianGuiDef.Table) relation).name;
                        schemaName = ((MondrianGuiDef.Table) relation).schema;
                    } else if (relation instanceof MondrianGuiDef.Join) {
                        String[] schemaAndTable =
                            SchemaExplorer.getTableNameForAlias(
                                relation, lTable);
                        schemaName = schemaAndTable[0];
                        lTable = schemaAndTable[1];
                    }
                }
            }
            if (lTable != null) {
                allcols = jdbcMetaData.getAllColumns(schemaName, lTable);
            } else {
                allcols = Collections.emptyList();
            }
            ComboBoxModel cAllcols =
                new DefaultComboBoxModel(new Vector<String>(allcols));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllcols);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Property.class
                   && propertyName.equals("type"))
        {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(
                new DefaultComboBoxModel(MondrianGuiDef.Property._type_values));
            listEditor.setSelectedItem((String) value);
            activeEditor = listEditor;

        } else if ((targetClassz == MondrianGuiDef.AggFactCount.class
            && propertyName.equals("column"))
            || (targetClassz == MondrianGuiDef.AggMeasureFactCount.class
            && propertyName.equals("column"))
            || (targetClassz == MondrianGuiDef.AggIgnoreColumn.class
            && propertyName.equals("column"))
            || (targetClassz == MondrianGuiDef.AggLevelProperty.class
            && propertyName.equals("column"))
            || (targetClassz == MondrianGuiDef.AggLevel.class
            && Arrays.asList(
                "column", "ordinalColumn",
                "captionColumn", "nameColumn").contains(propertyName))
            || (targetClassz == MondrianGuiDef.AggMeasure.class
            && propertyName.equals("column"))
            || (targetClassz == MondrianGuiDef.AggForeignKey.class
            && propertyName.equals("factColumn"))
            || (targetClassz == MondrianGuiDef.AggForeignKey.class
            && propertyName.equals("aggColumn")))
        {
            List<String> allcols = jdbcMetaData.getAllColumns(null, null);

            ComboBoxModel cAllcols =
                new DefaultComboBoxModel(new Vector<String>(allcols));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllcols);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Table.class && propertyName
            .equals("schema"))
        {
            List<String> allschemas = jdbcMetaData.getAllSchemas();
            ComboBoxModel cAllschemas =
                new DefaultComboBoxModel(new Vector<String>(allschemas));

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllschemas);
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;
        } else if (currentRelation != null
                   && nonTableRelation
                   && ((targetClassz == MondrianGuiDef.Hierarchy.class
                        && propertyName.equals("primaryKeyTable"))
                       || (targetClassz == MondrianGuiDef.Level.class
                           && propertyName.equals("table"))))
        {
            // Can't set a table on a non table relation
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            activeEditor = listEditor;
        } else if ((targetClassz == MondrianGuiDef.Table.class
                    && propertyName.equals("name"))
                   || (targetClassz == MondrianGuiDef.Hierarchy.class
                       && propertyName.equals("primaryKeyTable"))
                   || (targetClassz == MondrianGuiDef.Level.class
                       && propertyName.equals("table")))
        {
            String schema = "";
            if (targetClassz == MondrianGuiDef.Table.class) {
                MondrianGuiDef.Table tProps =
                    (MondrianGuiDef.Table) tableModel.getValue();
                schema = tProps.schema;
            }
            Vector<String> factTables =
                new Vector<String>(jdbcMetaData.getFactTables(schema));
            Vector<String> allTablesMinusFact =
                new Vector<String>(
                    jdbcMetaData.getAllTables(
                        schema, selectedFactTable));
            Vector<String> allTables =
                new Vector<String>(jdbcMetaData.getAllTables(schema));
            Vector<String> dimeTables =
                new Vector<String>(
                    jdbcMetaData.getDimensionTables(
                        schema, selectedFactTable));

            // suggestive fact tables
            ComboBoxModel cFactTables =
                new DefaultComboBoxModel(factTables);

            // all tables of selected schema
            ComboBoxModel cAllTables = new DefaultComboBoxModel(
                (allTablesMinusFact.size() > 0)
                    ? allTablesMinusFact
                    : allTables);

            // suggestive dimension tables based on selected fact table
            ComboBoxModel cDimeTables =
                new DefaultComboBoxModel(dimeTables);

            // Sets the corresponding join tables on selection dropdown when
            // using joins.
            if (targetClassz == MondrianGuiDef.Level.class
                || targetClassz == MondrianGuiDef.Hierarchy.class)
            {
                MondrianGuiDef.RelationOrJoin relation = null;
                if (parent != null
                    && parent instanceof MondrianGuiDef.Hierarchy)
                {
                    relation = ((MondrianGuiDef.Hierarchy) parent).relation;
                } else {
                    relation =
                        ((MondrianGuiDef.Hierarchy) tableModel.target).relation;
                }
                if (relation instanceof MondrianGuiDef.Join) {
                    TreeSet<String> joinTables = new TreeSet<String>();
                    // getTableNamesForJoin calls itself recursively and
                    // collects table names in joinTables.
                    SchemaExplorer.getTableNamesForJoin(relation, joinTables);
                    cAllTables =
                        new DefaultComboBoxModel(
                            new Vector<String>(
                                joinTables));
                }
            }

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(cAllTables);
            allOptions = cAllTables;
            boolean toggleModel = false;
            if (parentClassz == MondrianGuiDef.Cube.class) {
                cAllTables = new DefaultComboBoxModel(allTables);
                allOptions = cAllTables;
                if (factTables.size() > 0) {
                    ((DefaultComboBoxModel) cFactTables).insertElementAt(
                        workbench.getResourceConverter().getString(
                            "schemaPropertyCellEditor.allTables",
                            "<< All Tables >>"), 0);
                    ((DefaultComboBoxModel) cAllTables).insertElementAt(
                        workbench.getResourceConverter().getString(
                            "schemaPropertyCellEditor.factTables",
                            "<< Fact Tables >>"), 0);
                    listEditor.setModel(cFactTables);
                    selOptions = cFactTables;
                    toggleModel = true;
                }
            } else {
                if (dimeTables.size() > 0) {
                    ((DefaultComboBoxModel) cDimeTables).insertElementAt(
                        workbench.getResourceConverter().getString(
                            "schemaPropertyCellEditor.allTables",
                            "<< All Tables >>"), 0);
                    ((DefaultComboBoxModel) cAllTables).insertElementAt(
                        workbench.getResourceConverter().getString(
                            "schemaPropertyCellEditor.dimensionTables",
                            "<< Dimension Tables >>"), 0);
                    listEditor.setModel(cDimeTables);
                    selOptions = cDimeTables;
                    toggleModel = true;
                }
            }

            if (toggleModel) {
                listEditor.addActionListener(al);
            }
            listEditor.setSelectedItem((String) value);
            listEditorValue = (String) value;
            activeEditor = listEditor;
            // Disables table selection when not using joins.
            if ((targetClassz == MondrianGuiDef.Level.class
                 && propertyName.equals(SchemaExplorer.DEF_LEVEL[1])
                 && parent != null)
                || (targetClassz == MondrianGuiDef.Hierarchy.class
                    && propertyName.equals(SchemaExplorer.DEF_HIERARCHY[7])
                    && parent != null))
            {
                MondrianGuiDef.RelationOrJoin relation = null;
                if (parent instanceof MondrianGuiDef.Hierarchy) {
                    relation = ((MondrianGuiDef.Hierarchy) parent).relation;
                } else if (parent instanceof MondrianGuiDef.Dimension) {
                    relation =
                        ((MondrianGuiDef.Hierarchy) tableModel.target).relation;
                }
                if (relation instanceof MondrianGuiDef.Table) {
                    activeEditor = stringEditor;
                    stringEditor.setText((String) value);
                }
            }
        } else if (propertyName.equals("cdata")) {
            try {
                cdataTextArea.read(new StringReader((String) value), null);
            } catch (Exception ex) {
            }

            activeEditor = jScrollPaneCDATA;
        } else if (value instanceof String) {
            activeEditor = stringEditor;
            stringEditor.setText((String) value);
        } else if (value instanceof Boolean) {
            activeEditor = booleanEditor;
            booleanEditor.setSelected((Boolean) value);
        } else if (value instanceof Integer) {
            activeEditor = integerEditor;
            integerEditor.setText((String) value);
        } else if (value == null) {
            value = "";
            activeEditor = stringEditor;
            stringEditor.setText((String) value);
        } else if (value.getClass() == MondrianGuiDef.Join.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_JOIN);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.NameExpression.class) {
            return null;
        } else if (value.getClass() == MondrianGuiDef.RelationOrJoin.class) {
            // REVIEW: I don't think this code will ever be executed, because
            // class RelationOrJoin is abstract.
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_RELATION);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
            return null;
        } else if (value.getClass() == MondrianGuiDef.OrdinalExpression.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench,
                    ((MondrianGuiDef.OrdinalExpression) value).expressions[0],
                    SchemaExplorer.DEF_SQL);
            ptm.setParentTarget(((PropertyTableModel) table.getModel()).target);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.CaptionExpression.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench,
                    ((MondrianGuiDef.CaptionExpression) value).expressions[0],
                    SchemaExplorer.DEF_SQL);
            ptm.setParentTarget(((PropertyTableModel) table.getModel()).target);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Formula.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(
                    workbench, jdbcMetaData);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_FORMULA);
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass()
                   == MondrianGuiDef.CalculatedMemberProperty.class)
        {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench,
                    value,
                    SchemaExplorer.DEF_CALCULATED_MEMBER_PROPERTY);
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Table.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            // Adding cell editing stopped listeners to nested property of type
            // table so that any change in value of table fields are reflected
            // in tree.
            for (int i = listeners.size() - 1; i >= 0; i--) {
                spce.addCellEditorListener(listeners.get(i));
            }
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(
                workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(
                workbench, value, SchemaExplorer.DEF_TABLE);
            ptm.setFactTable(selectedFactTable);
            if (targetClassz == MondrianGuiDef.Cube.class) {
                ptm.setParentTarget(
                    ((PropertyTableModel) table.getModel()).target);
            }
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.AggFactCount.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(
                workbench, jdbcMetaData);
            // Adding cell editing stopped listeners to nested property of type
            // table so that any change in value of table fields are reflected
            // in tree.
            for (int i = listeners.size() - 1; i >= 0; i--) {
                spce.addCellEditorListener(listeners.get(i));
            }
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(
                workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(
                workbench, value, SchemaExplorer.DEF_AGG_FACT_COUNT);
            ptm.setFactTable(selectedFactTable);
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Closure.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            // Adding cell editing stopped listeners to nested property of type
            // table so that any change in value of table fields are reflected
            // in tree.
            for (int i = listeners.size() - 1; i >= 0; i--) {
                spce.addCellEditorListener(listeners.get(i));
            }
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_CLOSURE);
            ptm.setFactTable(selectedFactTable);
            if (targetClassz == MondrianGuiDef.Level.class) {
                ptm.setParentTarget(
                    ((PropertyTableModel) table.getModel()).target);
            }
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            spcr.setTableRendererHeight(tableEditor, null);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Property.class) {
            SchemaPropertyCellEditor spce =
                new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr =
                new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm =
                new PropertyTableModel(
                    workbench, value, SchemaExplorer.DEF_PROPERTY);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else {
            value = "";
            activeEditor = stringEditor;
            stringEditor.setText((String) value);
        }
        activeEditor.setVisible(true);

        setOriginalValue();

        table.changeSelection(row, column, false, false);
        activeEditor.setBackground(new java.awt.Color(224, 249, 255));
        activeEditor.requestFocusInWindow();
        return activeEditor;
    }

    /**
     * save original value to see whether it changed
     */
    private void setOriginalValue() {
        if (activeEditor == stringEditor) {
            originalValue = stringEditor.getText();
        } else if (activeEditor == booleanEditor) {
            originalValue = booleanEditor.isSelected();
        } else if (activeEditor == listEditor) {
            if (listEditor.isEditable()) {
                // returns the edited value from combox box
                originalValue = listEditor.getEditor().getItem();
            } else {
                if (listEditor.getSelectedItem() == noSelect) {
                    originalValue = null;  // blank selection
                }
                // returns the selected value from combox box
                originalValue = listEditor.getSelectedItem();
            }
        } else if (activeEditor == tableEditor) {
            originalValue =
                ((PropertyTableModel) tableEditor.getModel()).getValue();
        } else if (activeEditor == jScrollPaneCDATA) {
            Writer cdataTextAreaStr = new StringWriter();
            try {
                cdataTextArea.write(cdataTextAreaStr);
            } catch (Exception ex) {
            }
            originalValue = cdataTextAreaStr.toString();
        }
    }

    /**
     * Adds a listener to the list that's notified when the editor
     * stops, or cancels editing.
     *
     * @param l the CellEditorListener
     */
    public void addCellEditorListener(CellEditorListener l) {
        listeners.add(l);
    }

    /**
     * Tells the editor to cancel editing and not accept any partially
     * edited value.
     */
    public void cancelCellEditing() {
        if (activeEditor != null) {
            activeEditor.setVisible(false);
            fireEditingCancelled();
        }
    }

    /**
     * Returns the value contained in the editor.
     *
     * @return the value contained in the editor
     */
    public Object getCellEditorValue() {
        if (activeEditor == stringEditor) {
            return stringEditor.getText();
        } else if (activeEditor == booleanEditor) {
            return booleanEditor.isSelected();
        } else if (activeEditor == listEditor) {
            if (listEditor.isEditable()) {
                // returns the edited value from combox box
                return listEditor.getEditor().getItem();
            } else {
                if (listEditor.getSelectedItem() == noSelect) {
                    return null;  // blank selection
                }
                // returns the selected value from combox box
                return listEditor.getSelectedItem();
            }
        } else if (activeEditor == tableEditor) {
            return ((PropertyTableModel) tableEditor.getModel()).getValue();
        } else if (activeEditor == jScrollPaneCDATA) {
            Writer cdataTextAreaStr = new StringWriter();
            try {
                cdataTextArea.write(cdataTextAreaStr);
            } catch (Exception ex) {
            }
            return cdataTextAreaStr.toString();
        }

        return null;
    }

    /**
     * Asks the editor if it can start editing using <code>anEvent</code>.
     * <code>anEvent</code> is in the invoking component coordinate system.
     * The editor can not assume the Component returned by
     * <code>getCellEditorComponent</code> is installed.  This method
     * is intended for the use of client to avoid the cost of setting up
     * and installing the editor component if editing is not possible.
     * If editing can be started this method returns true.
     *
     * @param anEvent the event the editor should use to consider
     *                whether to begin editing or not
     * @return true if editing can be started
     * @see #shouldSelectCell
     */
    public boolean isCellEditable(EventObject anEvent) {
        return true;
    }

    /**
     * Removes a listener from the list that's notified
     *
     * @param l the CellEditorListener
     */
    public void removeCellEditorListener(CellEditorListener l) {
        listeners.remove(l);
    }

    /**
     * Returns true if the editing cell should be selected, false otherwise.
     * Typically, the return value is true, because is most cases the editing
     * cell should be selected.  However, it is useful to return false to
     * keep the selection from changing for some types of edits.
     * eg. A table that contains a column of check boxes, the user might
     * want to be able to change those checkboxes without altering the
     * selection.  (See Netscape Communicator for just such an example)
     * Of course, it is up to the client of the editor to use the return
     * value, but it doesn't need to if it doesn't want to.
     *
     * @param anEvent the event the editor should use to start
     *                editing
     * @return true if the editor would like the editing cell to be selected;
     *         otherwise returns false
     * @see #isCellEditable
     */
    public boolean shouldSelectCell(EventObject anEvent) {
        return true;
    }

    /**
     * Tells the editor to stop editing and accept any partially edited
     * value as the value of the editor.  The editor returns false if
     * editing was not stopped; this is useful for editors that validate
     * and can not accept invalid entries.
     *
     * @return true if editing was stopped; false otherwise
     */
    public boolean stopCellEditing() {
        if (activeEditor != null) {
            /* save the nested table as well */
            if (activeEditor == tableEditor) {
                if (tableEditor.isEditing()) {
                    List<JTable> nestedTableEditors = new ArrayList<JTable>();
                    JTable nestedTableEditor = tableEditor;
                    // Get the list of nested tables from outer->inner sequence,
                    // descending towards innermost nested table
                    // so that we can stop the editing in this order.
                    while (nestedTableEditor != null) {
                        nestedTableEditors.add(nestedTableEditor);
                        SchemaPropertyCellEditor sce =
                            (SchemaPropertyCellEditor) nestedTableEditor
                                .getCellEditor();
                        if (sce != null
                            && sce.activeEditor == sce.tableEditor
                            && sce.tableEditor.isEditing())
                        {
                            nestedTableEditor = sce.tableEditor; //
                            //tableEditor.editingStopped(null);
                        } else {
                            nestedTableEditor = null;
                        }
                    }
                    for (int i = nestedTableEditors.size() - 1; i >= 0; i--) {
                        nestedTableEditors.get(i).editingStopped(null);
                    }
                }
            }
            activeEditor.setVisible(false);
            fireEditingStopped();
        }
        return true;
    }

    protected void fireEditingStopped() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
            listeners.get(i).editingStopped(ce);
        }
    }

    protected void fireEditingCancelled() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
            listeners.get(i).editingCanceled(ce);
        }
    }

    private List<String> getUdfs() {
        List<String> udfs = new ArrayList<String>();
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.UserDefinedFunction[] u = s.userDefinedFunctions;
            for (int i = 0; i < u.length; i++) {
                if (!(u[i].className == null
                      || udfs.contains(u[i].className)))
                {
                    udfs.add(u[i].className);
                }
            }
        }

        return udfs;
    }

    private List<String> getFormatStrings() {
        List<String> fs = new ArrayList<String>();
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.Cube[] c = s.cubes;
            for (int i = 0; i < c.length; i++) {
                MondrianGuiDef.Measure[] m = c[i].measures;
                for (int j = 0; j < m.length; j++) {
                    if (!(m[j].formatString == null
                          || fs.contains(m[j].formatString)))
                    {
                        fs.add(m[j].formatString);
                    }
                }
            }
        }
        return fs;
    }

    private MondrianGuiDef.Schema getSchema() {
        SchemaExplorer se = this.getSchemaExplorer();
        return (se == null)
            ? null
            : se.getSchema();
    }

    private Object getParentObject() {
        SchemaExplorer se = this.getSchemaExplorer();
        if (se != null) {
            Object po = se.getParentObject();
            return po;
        }
        return null;
    }

    private SchemaExplorer getSchemaExplorer() {
        for (int i = listeners.size() - 1; i >= 0; i--) {
            CellEditorListener cel = listeners.get(i);
            if (cel instanceof SchemaExplorer) {
                return (SchemaExplorer) cel;
            }
        }
        return null;
    }

    // shared dimensions in schema
    private List<String> getSource() {
        List<String> source = new ArrayList<String>();
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.Dimension[] u = s.dimensions;
            for (int i = 0; i < u.length; i++) {
                source.add(u[i].name);
            }
        }
        return source;
    }

    private List<String> getCubes() {
        List<String> source = new ArrayList<String>();
        //===source.add(noSelect);
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.Cube[] u = s.cubes;
            for (int i = 0; i < u.length; i++) {
                source.add(u[i].name);
            }
        }
        return source;
    }

    private void generatePrimaryKeyTables(Object relation, List<String> v) {
        if (relation == null) {
            return;
        }
        if (relation instanceof MondrianGuiDef.Table) {
            String sname = ((MondrianGuiDef.Table) relation).schema;
            v.add(
                ((sname == null || sname.equals(""))
                    ? ""
                    : sname + "->") + ((MondrianGuiDef.Table) relation).name);
            return;
        }
        MondrianGuiDef.Join currentJoin = (MondrianGuiDef.Join) relation;
        generatePrimaryKeyTables(currentJoin.left, v);
        generatePrimaryKeyTables(currentJoin.right, v);
        return;
    }

    private List<String> getDimensions() {
        List<String> dims = new ArrayList<String>();
        Object po = getParentObject(); // cubegrant
        if (po != null) {
            MondrianGuiDef.CubeGrant parent = (MondrianGuiDef.CubeGrant) po;
            if (!(parent.cube == null || parent.cube.equals(""))) {
                MondrianGuiDef.Schema s = getSchema();
                if (s != null) {
                    for (int i = 0; i < s.cubes.length; i++) {
                        if (s.cubes[i].name.equals(parent.cube)) {
                            dims.add("Measures");
                            for (int j = 0; j < s.cubes[i].dimensions.length;
                                j++)
                            {
                                dims.add(s.cubes[i].dimensions[j].name);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return dims;
    }

    private List<String> getHierarchies() {
        List<String> hiers = new ArrayList<String>();
        Object po = getParentObject(); // cubegrant
        if (po != null) {
            MondrianGuiDef.CubeGrant parent = (MondrianGuiDef.CubeGrant) po;
            if (!(parent.cube == null || parent.cube.equals(""))) {
                MondrianGuiDef.Schema s = getSchema();
                if (s != null) {
                    for (int i = 0; i < s.cubes.length; i++) {
                        if (s.cubes[i].name.equals(parent.cube)) {
                            for (int j = 0; j < s.cubes[i].dimensions.length;
                                j++)
                            {
                                MondrianGuiDef.Dimension sharedDim =
                                    lookupDimension(
                                        s, s.cubes[i].dimensions[j]);
                                NodeDef[] children = sharedDim.getChildren();
                                for (int k = 0; k < children.length; k++) {
                                    if (children[k] instanceof Hierarchy) {
                                        String hname =
                                            ((Hierarchy) children[k]).name;
                                        if (hname != null) {
                                            if (MondrianProperties.instance()
                                                .SsasCompatibleNaming.get())
                                            {
                                                hiers.add(
                                                    Util.quoteMdxIdentifier(
                                                        s.cubes[i].dimensions[j]
                                                            .name)
                                                        + "."
                                                        + Util
                                                            .quoteMdxIdentifier(
                                                                hname));
                                            } else {
                                                hiers.add(
                                                    Util.quoteMdxIdentifier(
                                                        s.cubes[i].dimensions[j]
                                                            .name
                                                        + "."
                                                        + hname));
                                            }
                                        } else {
                                            hiers.add(Util.quoteMdxIdentifier(
                                                s.cubes[i].dimensions[j].name));
                                        }
                                    }
                                }
                            }

                            break;
                        }
                    }
                }
            }
        }
        return hiers;
    }

    private String cacheCube = "";
    private String cacheHierarchy = "";
    private List<String> hlevels = new ArrayList<String>();

    private List<String> getLevels(String hierarchy) {
        if (hierarchy == null || hierarchy.equals("")) {
            return hlevels;
        }
        List<Id.Segment> segments = Util.parseIdentifier(hierarchy);
        if (segments == null || segments.size() == 0) {
            return hlevels;
        }
        if (!MondrianProperties.instance().SsasCompatibleNaming.get()) {
            String data = ((Id.NameSegment)segments.get(0)).getName();
            // if segment contains a hierarchy
            if (data.indexOf(".") >= 0) {
                Id.Segment segment = segments.get(0);
                // split the segment
                segments.clear();
                segments.add(new Id.NameSegment(
                    data.substring(0, data.indexOf(".")),
                    segment.getQuoting()));
                segments.add(new Id.NameSegment(
                    data.substring(data.indexOf(".") + 1),
                    segment.getQuoting()));
            }
        }
        Object po = getParentObject(); // cubegrant
        if (po == null) {
            return hlevels;
        }
        MondrianGuiDef.CubeGrant parent = (MondrianGuiDef.CubeGrant) po;
        if (parent.cube == null || parent.cube.equals("")) {
            return hlevels;
        }
        if (cacheCube.equals(parent.cube) && cacheHierarchy.equals(hierarchy)) {
            return hlevels;
        }
        hlevels = new ArrayList<String>();
        cacheCube = parent.cube;
        cacheHierarchy = hierarchy;
        MondrianGuiDef.Schema s = getSchema();
        if (s == null) {
            return hlevels;
        }
        for (int i = 0; i < s.cubes.length; i++) {
            final MondrianGuiDef.Cube cube = s.cubes[i];
            if (!cube.name.equals(parent.cube)) {
                continue;
            }
            for (int j = 0; j < cube.dimensions.length; j++) {
                final MondrianGuiDef.CubeDimension dimension =
                    cube.dimensions[j];
                if (!segments.get(0).matches(dimension.name)) {
                    continue;
                }
                MondrianGuiDef.Dimension d = lookupDimension(s, dimension);
                NodeDef[] children = d.getChildren();
                MondrianGuiDef.Hierarchy hierarchyObj = null;
                for (int k = 0; k < children.length; k++) {
                    if (children[k] instanceof Hierarchy) {
                        if ((segments.size() == 1
                                && ((Hierarchy) children[k]).name == null)
                            || (segments.size() != 0
                                && segments.get(1).matches(
                                    ((Hierarchy) children[k]).name)))
                        {
                            hierarchyObj = (Hierarchy)children[k];
                            break;
                        }
                    }
                }
                if (hierarchyObj != null) {
                    for (int k = 0; k < hierarchyObj.levels.length; k++) {
                        hlevels.add(
                            hierarchy + "."
                            + Util.quoteMdxIdentifier(
                                hierarchyObj.levels[k].name));
                    }
                }
            }
            break;
        }
        return hlevels;
    }

    private static MondrianGuiDef.Dimension lookupDimension(
        MondrianGuiDef.Schema schema,
        MondrianGuiDef.CubeDimension cubeDimension)
    {
        if (cubeDimension instanceof MondrianGuiDef.Dimension) {
            return (MondrianGuiDef.Dimension) cubeDimension;
        } else {
            MondrianGuiDef.DimensionUsage dimensionUsage =
                (MondrianGuiDef.DimensionUsage) cubeDimension;
            for (int m = 0; m < schema.dimensions.length; m++) {
                final MondrianGuiDef.Dimension dimension =
                    schema.dimensions[m];
                if (dimension.name.equals(dimensionUsage.source)) {
                    return dimension;
                }
            }
            return null;
        }
    }

    private I18n getResourceConverter() {
        return workbench.getResourceConverter();
    }

    public void setMetaData(JdbcMetaData jdbcMetaData) {
        // Called from the SchemaExplorer.resetMetadata(). A call to the
        // updateUI() should be made on the owning SchemaFrame to reflect the
        // use of the JdbcMetaData being set.
        this.jdbcMetaData = jdbcMetaData;
    }
}

// End SchemaPropertyCellEditor.java
