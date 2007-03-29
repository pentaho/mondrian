/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Created on September 26, 2002, 11:28 AM
// Modified on 15-Jun-2003 by ebengtso
//
 */

package mondrian.gui;


import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.util.Iterator;
import java.util.Map;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.filechooser.FileSystemView;
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;

import javax.swing.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.util.Vector;
import javax.swing.event.InternalFrameEvent;
import javax.swing.plaf.basic.BasicArrowButton;
import javax.swing.text.DefaultEditorKit;

import org.eigenbase.xom.XMLOutput;

/**
 *
 * @author  sean
 */
public class Workbench extends javax.swing.JFrame {

    private static final String LAST_USED1 = "lastUsed1";
    private static final String LAST_USED1_URL = "lastUsedUrl1";
    private static final String LAST_USED2 = "lastUsed2";
    private static final String LAST_USED2_URL = "lastUsedUrl2";
    private static final String LAST_USED3 = "lastUsed3";
    private static final String LAST_USED3_URL = "lastUsedUrl3";
    private static final String LAST_USED4 = "lastUsed4";
    private static final String LAST_USED4_URL = "lastUsedUrl4";

    private Connection connection;
    private String jdbcDriverClassName;
    private String jdbcConnectionUrl;
    private String jdbcUsername;
    private String jdbcPassword;

    private JDBCMetaData jdbcMetaData;

    private final ClassLoader myClassLoader;
    private final ResourceBundle resources;
    private Properties workbenchProperties;
    private static int newSchema = 1;

    private String openFile=null;

    private Map schemaWindowMap = new HashMap();    // map of schema frames and its menu items (JInternalFrame -> JMenuItem)
    private Vector mdxWindows = new Vector();
    private int windowMenuMapIndex = 1;

    private static ResourceBundle resBundle = null;

    /** Creates new form Workbench */
    public Workbench() {
        myClassLoader = this.getClass().getClassLoader();

        loadWorkbenchProperties();
        initDataSource();
        initComponents();

        //ResourceBundle resources = ResourceBundle.getBundle("mondrian.gui.resources.gui", Locale.getDefault(), myClassLoader);
        resources = ResourceBundle.getBundle("mondrian.gui.resources.gui", Locale.getDefault(), myClassLoader);

        ImageIcon icon = new javax.swing.ImageIcon(myClassLoader.getResource(resources.getString("cube")));

        this.setIconImage(icon.getImage());
        //openSchemaFrame(new File("C:/Documents and Settings/sarora/My Documents/Inventory.xml"), false); //===
    }

    /**
     * load properties
     */
    private void loadWorkbenchProperties() {
        workbenchProperties = new Properties();
        try {
            workbenchProperties.load(new FileInputStream(new File("workbench.properties")));
            String resourceName = "mondrian.gui.resources.workbenchInfo";
            resBundle = ResourceBundle.getBundle(resourceName);
        } catch (Exception e) {
            //e.printStackTrace();

            // TODO deal with exception
        }
    }

    /**
     * save properties
     */
    private void storeWorkbenchProperties() {
        //save properties to file
        OutputStream out = null;
        try {
            out = (OutputStream) new FileOutputStream(new File("workbench.properties"));
            workbenchProperties.store(out, "Workbench configuration");
        } catch (Exception e) {
            //TODO deal with exception
        } finally {
            try {
                out.close();
            } catch (IOException eIO) {
                //TODO deal with exception
            }
        }
    }

    /**
     * Initialize the data source from a property file
     */
    private void initDataSource() {
        jdbcDriverClassName = workbenchProperties.getProperty("jdbcDriverClassName");
        jdbcConnectionUrl = workbenchProperties.getProperty("jdbcConnectionUrl");
        jdbcUsername = workbenchProperties.getProperty("jdbcUsername");
        jdbcPassword = workbenchProperties.getProperty("jdbcPassword");
    }

    /** This method is called from within the constructor to
     * initialize the form.
     */
    private void initComponents() {
        desktopPane = new javax.swing.JDesktopPane();
        jToolBar1 = new javax.swing.JToolBar();
        jToolBar2 = new javax.swing.JToolBar();
        toolbarNewPopupMenu = new JPopupMenu();
        toolbarNewButton = new javax.swing.JButton();
        toolbarOpenButton = new javax.swing.JButton();
        toolbarSaveButton = new javax.swing.JButton();
        toolbarSaveAsButton = new javax.swing.JButton();
        jPanel1 = new javax.swing.JPanel();
        jPanel2 = new javax.swing.JPanel();
        toolbarPreferencesButton = new javax.swing.JButton();
        menuBar = new javax.swing.JMenuBar();
        fileMenu = new javax.swing.JMenu();
        newMenu = new javax.swing.JMenu();
        newSchemaMenuItem = new javax.swing.JMenuItem();
        newQueryMenuItem = new javax.swing.JMenuItem();
        newJDBCExplorerMenuItem = new javax.swing.JMenuItem();
        newSchemaMenuItem2 = new javax.swing.JMenuItem();
        newQueryMenuItem2 = new javax.swing.JMenuItem();
        newJDBCExplorerMenuItem2 = new javax.swing.JMenuItem();
        openMenuItem = new javax.swing.JMenuItem();
        preferencesMenuItem = new javax.swing.JMenuItem();
        lastUsed1MenuItem = new javax.swing.JMenuItem();
        lastUsed2MenuItem = new javax.swing.JMenuItem();
        lastUsed3MenuItem = new javax.swing.JMenuItem();
        lastUsed4MenuItem = new javax.swing.JMenuItem();
        saveMenuItem = new javax.swing.JMenuItem();
        saveAsMenuItem = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JSeparator();
        jSeparator2 = new javax.swing.JSeparator();
        jSeparator3 = new javax.swing.JSeparator();
        jSeparator4 = new javax.swing.JSeparator();
        exitMenuItem = new javax.swing.JMenuItem();
        windowMenu = new javax.swing.JMenu();
        helpMenu = new javax.swing.JMenu();
        editMenu = new javax.swing.JMenu();
        cutMenuItem = new javax.swing.JMenuItem(new DefaultEditorKit.CutAction());
        copyMenuItem = new javax.swing.JMenuItem(new DefaultEditorKit.CopyAction());
        pasteMenuItem = new javax.swing.JMenuItem(new DefaultEditorKit.PasteAction());
        deleteMenuItem = new javax.swing.JMenuItem();
        aboutMenuItem = new javax.swing.JMenuItem();
        toolsMenu = new javax.swing.JMenu();
        viewMenu = new javax.swing.JMenu();
        viewDimensionsMenuItem = new javax.swing.JCheckBoxMenuItem();
        viewMeasuresMenuItem = new javax.swing.JCheckBoxMenuItem();
        viewCubesMenuItem = new javax.swing.JCheckBoxMenuItem();
        viewXMLMenuItem = new javax.swing.JCheckBoxMenuItem();

        setTitle("Schema Workbench");
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                closeAllSchemaFrames(true);
            }
        });

        getContentPane().add(desktopPane, java.awt.BorderLayout.CENTER);


        newSchemaMenuItem2.setText("Schema");
        newSchemaMenuItem2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newSchemaMenuItemActionPerformed(evt);
            }
        });


        newQueryMenuItem2.setText("MDX Query");
        newQueryMenuItem2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newQueryMenuItemActionPerformed(evt);
            }
        });


        newJDBCExplorerMenuItem2.setText("JDBC Explorer");
        newJDBCExplorerMenuItem2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newJDBCExplorerMenuItemActionPerformed(evt);
            }
        });


        toolbarNewPopupMenu.add(newSchemaMenuItem2);
        toolbarNewPopupMenu.add(newQueryMenuItem2);
        toolbarNewPopupMenu.add(newJDBCExplorerMenuItem2);


        jPanel2.setLayout(new java.awt.BorderLayout());
        jPanel2.setBorder(javax.swing.BorderFactory.createEtchedBorder());
        jPanel2.setMaximumSize(new java.awt.Dimension(50, 28) ); // old width=18

        toolbarNewButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/New16.gif")));
        toolbarNewButton.setToolTipText("New");
        toolbarNewButton.setBorderPainted(false);
        toolbarNewButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                toolbarNewPopupMenu.show(jPanel2,0,jPanel2.getSize().height);
            }
        });

        jToolBar2.setFloatable(false);
        jToolBar2.add(toolbarNewButton);

        jPanel2.add(jToolBar2,java.awt.BorderLayout.CENTER);

        toolbarNewArrowButton = new BasicArrowButton(SwingConstants.SOUTH);
        toolbarNewArrowButton.setToolTipText("New");
        toolbarNewArrowButton.setBorderPainted(false);
        toolbarNewArrowButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                toolbarNewPopupMenu.show(jPanel2,0,jPanel2.getSize().height);
            }
        });

        jPanel2.add(toolbarNewArrowButton,java.awt.BorderLayout.EAST);

        // toolbarNewButton.add(select); // error:none of the toolbar buttons are displayed
        // jToolBar1.add(select,1);    //error: arrow button is so wide it takes all remaining space on toolbar
        jToolBar1.add(jPanel2,0);




        toolbarOpenButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Open16.gif")));
        toolbarOpenButton.setToolTipText("Open");
        toolbarOpenButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                openMenuItemActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarOpenButton);

        toolbarSaveButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Save16.gif")));
        toolbarSaveButton.setToolTipText("Save");
        toolbarSaveButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveMenuItemActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarSaveButton);

        toolbarSaveAsButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/SaveAs16.gif")));
        toolbarSaveAsButton.setToolTipText("Save As");
        toolbarSaveAsButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveAsMenuItemActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarSaveAsButton);

        jPanel1.setMaximumSize(new java.awt.Dimension(8, 8)); //8, 32767
        jToolBar1.add(jPanel1);

        toolbarPreferencesButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Preferences16.gif")));
        toolbarPreferencesButton.setToolTipText("Preferences");
        toolbarPreferencesButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                toolbarPreferencesButtonActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarPreferencesButton);


        getContentPane().add(jToolBar1, java.awt.BorderLayout.NORTH);

        fileMenu.setText("File");
        newMenu.setText("New");

        newSchemaMenuItem.setText("Schema");
        newSchemaMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newSchemaMenuItemActionPerformed(evt);
            }
        });

        newMenu.add(newSchemaMenuItem);

        newQueryMenuItem.setText("MDX Query");
        newQueryMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newQueryMenuItemActionPerformed(evt);
            }
        });

        newMenu.add(newQueryMenuItem);

        newJDBCExplorerMenuItem.setText("JDBC Explorer");
        newJDBCExplorerMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newJDBCExplorerMenuItemActionPerformed(evt);
            }
        });

        newMenu.add(newJDBCExplorerMenuItem);

        fileMenu.add(newMenu);

        openMenuItem.setText("Open");
        openMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                openMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(openMenuItem);

        saveMenuItem.setText("Save");
        saveMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(saveMenuItem);

        saveAsMenuItem.setText("Save As ...");
        saveAsMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveAsMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(saveAsMenuItem);

        //add last used
        fileMenu.add(jSeparator2);

        lastUsed1MenuItem.setText(workbenchProperties.getProperty("lastUsed1"));
        lastUsed1MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed1MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed1MenuItem);

        lastUsed2MenuItem.setText(workbenchProperties.getProperty("lastUsed2"));
        lastUsed2MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed2MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed2MenuItem);

        lastUsed3MenuItem.setText(workbenchProperties.getProperty("lastUsed3"));
        lastUsed3MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed3MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed3MenuItem);

        lastUsed4MenuItem.setText(workbenchProperties.getProperty("lastUsed4"));
        lastUsed4MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed4MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed4MenuItem);

        updateLastUsedMenu();
        fileMenu.add(jSeparator1);

        exitMenuItem.setText("Exit");
        exitMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                exitMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(exitMenuItem);

        menuBar.add(fileMenu);

        editMenu.setText("Edit");
        cutMenuItem.setText("Cut");
        editMenu.add(cutMenuItem);

        copyMenuItem.setText("Copy");
        editMenu.add(copyMenuItem);

        pasteMenuItem.setText("Paste");
        editMenu.add(pasteMenuItem);

        deleteMenuItem.setText("Delete");
        editMenu.add(deleteMenuItem);

        menuBar.add(editMenu);
        editMenu.add(pasteMenuItem);

        viewMenu.setText("View");
        viewXMLMenuItem.setText("View XML");
        viewXMLMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                viewXMLMenuItemActionPerformed(evt);
            }
        });
        viewMenu.add(viewXMLMenuItem);
        menuBar.add(viewMenu);

        toolsMenu.setText("Tools");
        preferencesMenuItem.setText("Preferences");
        preferencesMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                toolbarPreferencesButtonActionPerformed(evt);
            }
        });
        toolsMenu.add(preferencesMenuItem);
        menuBar.add(toolsMenu);


        windowMenu.setText("Windows");

        cascadeMenuItem = new javax.swing.JMenuItem();
        cascadeMenuItem.setText("Cascade Windows");
        cascadeMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cascadeMenuItemActionPerformed(evt);
            }
        });

        tileMenuItem = new javax.swing.JMenuItem();
        tileMenuItem.setText("Tile Windows");
        tileMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tileMenuItemActionPerformed(evt);
            }
        });

        closeAllMenuItem = new javax.swing.JMenuItem();
        closeAllMenuItem.setText("Close All");
        closeAllMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                closeAllMenuItemActionPerformed(evt);
            }
        });

        minimizeMenuItem = new javax.swing.JMenuItem();
        minimizeMenuItem.setText("Minimize All");
        minimizeMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                minimizeMenuItemActionPerformed(evt);
            }
        });

        maximizeMenuItem = new javax.swing.JMenuItem();
        maximizeMenuItem.setText("Maximize All");
        maximizeMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                maximizeMenuItemActionPerformed(evt);
            }
        });

        menuBar.add(windowMenu);

        aboutMenuItem.setText("About");
        aboutMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                aboutMenuItemActionPerformed(evt);
            }
        });

        helpMenu.add(aboutMenuItem);

        helpMenu.setText("Help");
        menuBar.add(helpMenu);

        setJMenuBar(menuBar);

        pack();
    }

    private void tileMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Dimension dsize = desktopPane.getSize();
        int desktopW = (int) dsize.getWidth();
        int desktopH = (int) dsize.getHeight();
        int darea =  (int) (desktopW*desktopH);

        double eacharea= darea/(schemaWindowMap.size()+mdxWindows.size());
        int wh = (int) Math.sqrt(eacharea);

        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // keys = schemaframes
        its[1] = mdxWindows.iterator();

        JInternalFrame sf=null;
        int x=0, y=0;

        try {
            for (int i=0; i<its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        if (sf.isIcon()) {
                            //sf.setIcon(false);
                        } else {
                            sf.setMaximum(false);
                            sf.moveToFront();
                            if ((x >= desktopW) || (((desktopW-x)*wh) < (eacharea/2))) {
                                // move to next row of windows
                                y+=wh;
                                x=0;
                            }
                            int sfwidth  = ((x+wh) < desktopW ? wh : desktopW-x);
                            int sfheight = ((y+wh) < desktopH ? wh : desktopH-y);
                            sf.setBounds(x, y, sfwidth, sfheight);
                            x += sfwidth;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            //do nothing
        }
    }
    // cascade all the indows open in schema workbench
    private void cascadeMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // keys = schemaframes
        its[1] = mdxWindows.iterator();
        int sfi = 1;
        JInternalFrame sf=null;

        try {
            for (int i=0; i<its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        if (sf.isIcon()) {
                            //sf.setIcon(false);
                        } else {
                            sf.setMaximum(false);
                            sf.setLocation(30*sfi, 30*sfi);
                            sf.moveToFront();
                            sf.setSelected(true);
                            sfi++;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            //do nothing
        }
    }

    // close all the windows open in schema workbench
    private void closeAllMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        closeAllSchemaFrames(false);
    }

    private void closeAllSchemaFrames(boolean exitAfterClose) {
        Object [][] its = new Object[2][];  // initialize row dimension
        its[0] = schemaWindowMap.keySet().toArray();   // keys = schemaframes
        its[1] = mdxWindows.toArray();
        JInternalFrame sf=null;

        try {
            for (int i=0; i<its.length; i++) {
                for (int j=0; j<its[i].length; j++) {
                    sf = (JInternalFrame) its[i][j];
                    if (sf != null) {
                        if (sf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                            SchemaExplorer se = (SchemaExplorer) sf.getContentPane().getComponent(0);
                            sf.setSelected(true);
                            int response = confirmFrameClose(sf, se);
                            if (response == 2) {    // cancel
                                return;
                            }
                            if (response == 3) {    // not dirty
                                sf.setClosed(true);
                            }
                        }
                    }
                }
            }
            // exit Schema Workbench if no files are open
            if (((schemaWindowMap.keySet().size()) == 0) && exitAfterClose) {
                System.exit(0);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private int confirmFrameClose(JInternalFrame schemaFrame, SchemaExplorer se) {
        if (se.isDirty()) {
            JMenuItem schemaMenuItem = (JMenuItem) schemaWindowMap.get(desktopPane.getSelectedFrame());
            int answer = JOptionPane.showConfirmDialog(null,
                                "Save changes to "+se.getSchemaFile()+"?" ,
                                "Schema", JOptionPane.YES_NO_CANCEL_OPTION);
            switch(answer) { //   yes=0 ;no=1 ;cancel=2
            case 0:
                saveMenuItemActionPerformed(null);
                schemaWindowMap.remove(schemaFrame); //schemaWindowMap.remove(se.getSchemaFile());
                updateMDXCatalogList();
                schemaFrame.dispose();
                windowMenu.remove(schemaMenuItem);  // follow this by removing file from schemaWindowMap
                break;
            case 1:
                schemaFrame.dispose();
                schemaWindowMap.remove(schemaFrame);
                windowMenu.remove(schemaMenuItem);
                break;
            case 2:
                try {
                    schemaFrame.setClosed(false);
                    schemaFrame.show();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            return answer;
        }
        return 3;
    }

    private void minimizeMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // values = schemaframes
        its[1] = mdxWindows.iterator();
        JInternalFrame sf;

        try {
            for (int i=0; i<its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        if (sf.isIcon()) {
                            //sf.setIcon(false);
                        } else {
                            sf.setIcon(true);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            //do nothing
        }
    }

    private void maximizeMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // values = schemaframes
        its[1] = mdxWindows.iterator();
        JInternalFrame sf;

        try {
            for (int i=0; i<its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        sf.setIcon(false);
                        sf.setMaximum(true);
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            //do nothing
        }
    }

    private void aboutMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            JEditorPane jEditorPane = new JEditorPane(myClassLoader.getResource(resources.getString("version")).toString());
            jEditorPane.setEditable(false);
            JScrollPane jScrollPane = new JScrollPane(jEditorPane);
            JPanel jPanel = new JPanel();
            jPanel.setLayout(new java.awt.BorderLayout());
            jPanel.add(jScrollPane, java.awt.BorderLayout.CENTER);

            JInternalFrame jf = new JInternalFrame();
            jf.setTitle("About");
            jf.getContentPane().add(jPanel);

            Dimension screenSize = this.getSize();
            int aboutW = 400;
            int aboutH = 300;
            int width = (screenSize.width / 2) - (aboutW/2);
            int height = (screenSize.height / 2) - (aboutH/2) - 100; ;
            jf.setBounds(width, height, aboutW, aboutH);
            jf.setClosable(true);

            desktopPane.add(jf);

            jf.setVisible(true);
            jf.show();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    private void newJDBCExplorerMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            if (this.jdbcDriverClassName==null || this.jdbcConnectionUrl==null) {
                throw new Exception("Driver="+this.jdbcDriverClassName+"\nConn Url="+this.jdbcConnectionUrl);
            }

            JInternalFrame jf = new JInternalFrame();
            jf.setTitle("JDBC Explorer - " + this.jdbcConnectionUrl);

            Class.forName(this.jdbcDriverClassName);
            java.sql.Connection con = java.sql.DriverManager.getConnection(this.jdbcConnectionUrl, this.jdbcUsername, this.jdbcPassword);

            JDBCExplorer jdbce = new JDBCExplorer(con);

            jf.getContentPane().add(jdbce);
            jf.setBounds(0, 0, 500, 480);
            jf.setClosable(true);
            jf.setIconifiable(true);
            jf.setMaximizable(true);
            jf.setResizable(true);
            jf.setVisible(true);

            desktopPane.add(jf);

            jf.show();
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Database connection could not be done.\n"+ex.getMessage(), "Database Connection Error" , JOptionPane.ERROR_MESSAGE);
            //ex.printStackTrace();
        }
    }

    private void toolbarPreferencesButtonActionPerformed(java.awt.event.ActionEvent evt) {
        PreferencesDialog pd = new PreferencesDialog(this, true);
        pd.setJDBCConnectionUrl(jdbcConnectionUrl);
        pd.setJDBCDriverClassName(jdbcDriverClassName);
        pd.setJDBCUsername(jdbcUsername);
        pd.setJDBCPassword(jdbcPassword);

        pd.show();

        if (pd.accepted()) {
            jdbcConnectionUrl = pd.getJDBCConnectionUrl();
            jdbcDriverClassName = pd.getJDBCDriverClassName();
	        jdbcUsername = pd.getJDBCUsername();
	        jdbcPassword = pd.getJDBCPassword();

            workbenchProperties.setProperty("jdbcDriverClassName", jdbcDriverClassName);
            workbenchProperties.setProperty("jdbcConnectionUrl", jdbcConnectionUrl);
            workbenchProperties.setProperty("jdbcUsername", jdbcUsername);
            workbenchProperties.setProperty("jdbcPassword", jdbcPassword);
        }
    }


    private void newSchemaMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        MondrianProperties.instance();
        /* user's default directory. This default depends on the operating system.
         * It is typically the "My Documents" folder on Windows, and the
         * user's home directory on Unix.
         */
        File defaultDir = FileSystemView.getFileSystemView().getDefaultDirectory();
        File outputFile  ; //=new File(defaultDir, "Schema"+ newSchema +".xml");
        do  {
            outputFile = new File(defaultDir, "Schema"+ newSchema++ +".xml");
        } while (outputFile.exists());
        /*
        try {

            FileWriter out = new FileWriter(outputFile);
            out.write("<?xml version=\"1.0\"?>\n\r");
            out.write("<Schema name=\"New Schema "+newSchema+"\">\n\r");  //name=\"FoodMart\"
            out.write("</Schema>\n\r");
            out.close();
            newSchema++;
        } catch (IOException e) {
            e.printStackTrace();
        }
         */
        openSchemaFrame(outputFile, true);
    }

    private void newQueryMenuItemActionPerformed(java.awt.event.ActionEvent evt) {

        /*
        System.out.println("connection.getCatalogName()==="+connection.getCatalogName());
        System.out.println("connection.getProperty(catalog)==="+connection.getProperty("catalog"));
        System.out.println("connection.getSchema().getName()==="+connection.getSchema().getName());
         */
        JMenuItem schemaMenuItem = (JMenuItem) schemaWindowMap.get(desktopPane.getSelectedFrame());

        final JInternalFrame jf = new JInternalFrame();
        jf.setTitle("MDX Query");
        QueryPanel qp = new QueryPanel();

        jf.getContentPane().add(qp);
        jf.setBounds(0, 0, 500, 480);
        jf.setClosable(true);
        jf.setIconifiable(true);
        jf.setMaximizable(true);
        jf.setResizable(true);
        jf.setVisible(true);

        desktopPane.add(jf);
        jf.show();
        try {
            jf.setSelected(true);
        } catch(Exception ex) {
            // do nothing
        }

        // add the mdx frame to this set of mdx frames for cascading method
        mdxWindows.add(jf);

        // create mdx menu item
        final javax.swing.JMenuItem queryMenuItem = new javax.swing.JMenuItem();
        queryMenuItem.setText(windowMenuMapIndex + " MDX"); //file.getName()
        queryMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                try {
                    if (jf.isIcon()) {
                        jf.setIcon(false);
                    } else {
                        jf.setSelected(true);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        // disable mdx frame close operation to provide our handler
        // to remove frame object from mdxframeset before closing
        jf.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

        jf.addInternalFrameListener(new InternalFrameAdapter() {
            public void internalFrameClosing(InternalFrameEvent e) {
                mdxWindows.remove(jf);
                jf.dispose();
                windowMenu.remove(queryMenuItem);  // follow this by removing file from schemaWindowMap
                return;
            }
        });

        windowMenu.add(queryMenuItem,-1);
        windowMenu.add(jSeparator3,-1);
        windowMenu.add(cascadeMenuItem,-1);
        windowMenu.add(tileMenuItem,-1);
        windowMenu.add(minimizeMenuItem, -1);
        windowMenu.add(maximizeMenuItem, -1);
        windowMenu.add(closeAllMenuItem,-1);

        qp.setMenuItem(queryMenuItem);
        qp.setSchemaWindowMap(schemaWindowMap);
        //===qp.setConnection(connection);
        qp.setWindowMenuIndex(windowMenuMapIndex++);
        if (schemaMenuItem != null) {
            qp.initConnection(schemaMenuItem.getText());
        } else {
            JOptionPane.showMessageDialog(this,"No Mondrian connection. Select a Schema to connect.", "Alert", JOptionPane.WARNING_MESSAGE);
        }
    }

    // inform all opened mdx query windows about the list of opened schema files
    private void updateMDXCatalogList() {
        Iterator it = mdxWindows.iterator();
        while (it.hasNext()) {
            JInternalFrame elem = (JInternalFrame)  it.next();
            QueryPanel qp = (QueryPanel) elem.getContentPane().getComponent(0);
            qp.setSchemaWindowMap(schemaWindowMap);
        }
    }


    private void saveAsMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();
        if (jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);
            java.io.File schemaFile = se.getSchemaFile();
            java.io.File oldSchemaFile = schemaFile;
            java.io.File suggSchemaFile = new File(schemaFile, se.getSchema().name.trim()+".xml");
            MondrianGuiDef.Schema schema = se.getSchema();
            JFileChooser jfc = new JFileChooser();
            MondrianProperties.instance();
            //===jfc.setSelectedFile(schemaFile);
            jfc.setSelectedFile(suggSchemaFile);
            if (jfc.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
                try {
                    schemaFile = jfc.getSelectedFile();
                    if (!oldSchemaFile.equals(schemaFile) && schemaFile.exists()) {  //new file already exists, check for overwrite
                        int answer = JOptionPane.showConfirmDialog(null, schemaFile.getAbsolutePath() + " file already exists. Do you want to replace it?", "Save As", JOptionPane.YES_NO_OPTION);
                        if (answer == 1) { //  no=1 ; yes=0
                            return;
                        }
                    }

                    if (se.isNewFile() && !oldSchemaFile.equals(schemaFile)) {
                        oldSchemaFile.delete();
                    }

                    if (se.isNewFile()) {
                        se.setNewFile(false);
                    }
                    se.setDirty(false);
                    se.setDirtyFlag(false);

                    XMLOutput out = new XMLOutput(new java.io.FileWriter(jfc.getSelectedFile()));
                    schema.displayXML(out);
                    se.setSchemaFile(schemaFile);
                    se.setTitle();  //sets title of iframe
                    setLastUsed(jfc.getSelectedFile().getName(), jfc.getSelectedFile().toURI().toURL().toString());
                    // update menu item with new file name, then update catalog list for mdx queries
                    JMenuItem sMenuItem = (JMenuItem) schemaWindowMap.get(jf);
                    String mtexttokens[] = sMenuItem.getText().split(" ");
                    sMenuItem.setText(mtexttokens[0]+" "+se.getSchemaFile().getName());
                    updateMDXCatalogList(); // schema menu item updated, now update mdx query windows with updated catallog list
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void viewXMLMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();
        boolean oldValue = viewXMLMenuItem.getState();
        if (jf!=null && jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);
            // call schema explorer's view xml event and update the workbench's view menu accordingly'
            ((JCheckBoxMenuItem) evt.getSource()).setSelected(se.editMode(evt));
            return;
        }
        viewXMLMenuItem.setSelected(! oldValue);
    }

    private void saveMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();
        if (jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);

            java.io.File schemaFile = se.getSchemaFile();

            if (se.isNewFile()) {
                saveAsMenuItemActionPerformed(evt);
                return;
            }

            se.setDirty(false);
            se.setDirtyFlag(false);
            se.setTitle();  //sets title of iframe

            MondrianGuiDef.Schema schema = se.getSchema();
            MondrianProperties.instance();
            try {
                XMLOutput out = new XMLOutput(new FileWriter(schemaFile));
                schema.displayXML(out);
                setLastUsed(schemaFile.getName(), schemaFile.toURI().toURL().toString());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    /**
     * Set last used in properties file
     *
     * @param name
     * @param url
     */
    private void setLastUsed(String name, String url) {
        int match = 4;
        String luName = null;
        String propname = null;
        String lastUsed = "lastUsed";
        String lastUsedUrl = "lastUsedUrl";
        for( int i=1; i<=4; i++) {
            propname = lastUsed+i;
            luName = workbenchProperties.getProperty(propname) ;

            if ( luName != null && luName.equals(name)) {
                match=i;
                break;
            }
        }

        for(int i=match; i>1; i--) {
            if (workbenchProperties.getProperty(lastUsed+(i-1)) != null) {
                workbenchProperties.setProperty(lastUsed+i, workbenchProperties.getProperty(lastUsed+(i-1)));
                workbenchProperties.setProperty(lastUsedUrl+i, workbenchProperties.getProperty(lastUsedUrl+(i-1)));
            }
        }
        /*
        if ( workbenchProperties.getProperty(LAST_USED1) == null ||
                (! name.equals(workbenchProperties.getProperty(LAST_USED1))) ) {

            if (workbenchProperties.getProperty(LAST_USED3) != null) {
                workbenchProperties.setProperty(LAST_USED4, workbenchProperties.getProperty(LAST_USED3));
                workbenchProperties.setProperty(LAST_USED4_URL, workbenchProperties.getProperty(LAST_USED3_URL));
            }
            if (workbenchProperties.getProperty(LAST_USED2) != null) {
                workbenchProperties.setProperty(LAST_USED3, workbenchProperties.getProperty(LAST_USED2));
                workbenchProperties.setProperty(LAST_USED3_URL, workbenchProperties.getProperty(LAST_USED2_URL));
            }
            if (workbenchProperties.getProperty(LAST_USED1) != null) {
                workbenchProperties.setProperty(LAST_USED2, workbenchProperties.getProperty(LAST_USED1));
                workbenchProperties.setProperty(LAST_USED2_URL, workbenchProperties.getProperty(LAST_USED1_URL));
            }
        }
         */
        workbenchProperties.setProperty(LAST_USED1, name);
        workbenchProperties.setProperty(LAST_USED1_URL, url);
        updateLastUsedMenu();
        storeWorkbenchProperties();
    }

    private void updateLastUsedMenu() {
        if (workbenchProperties.getProperty(LAST_USED1) == null) {
            jSeparator2.setVisible(false); } else {
            jSeparator2.setVisible(true); }

        if (workbenchProperties.getProperty(LAST_USED1) != null) {
            lastUsed1MenuItem.setVisible(true);} else {
            lastUsed1MenuItem.setVisible(false); }
        if (workbenchProperties.getProperty(LAST_USED2) != null) {
            lastUsed2MenuItem.setVisible(true); } else {
            lastUsed2MenuItem.setVisible(false); }
        if (workbenchProperties.getProperty(LAST_USED3) != null) {
            lastUsed3MenuItem.setVisible(true); } else {
            lastUsed3MenuItem.setVisible(false); }
        if (workbenchProperties.getProperty(LAST_USED4) != null) {
            lastUsed4MenuItem.setVisible(true); } else {
            lastUsed4MenuItem.setVisible(false); }

        lastUsed1MenuItem.setText(workbenchProperties.getProperty(LAST_USED1));
        lastUsed2MenuItem.setText(workbenchProperties.getProperty(LAST_USED2));
        lastUsed3MenuItem.setText(workbenchProperties.getProperty(LAST_USED3));
        lastUsed4MenuItem.setText(workbenchProperties.getProperty(LAST_USED4));
    }

    private void lastUsed1MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED1_URL))), false);
        } catch (Exception e) //catch (URISyntaxException e)
        {
            e.printStackTrace();
        }
    }

    private void lastUsed2MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            /*
                workbenchProperties.setProperty(LAST_USED2, workbenchProperties.getProperty(LAST_USED1));
                workbenchProperties.setProperty(LAST_USED2_URL, workbenchProperties.getProperty(LAST_USED1_URL));
             */
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED2_URL))), false);
            setLastUsed(workbenchProperties.getProperty(LAST_USED2), workbenchProperties.getProperty(LAST_USED2_URL));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void lastUsed3MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED3_URL))), false);
            setLastUsed(workbenchProperties.getProperty(LAST_USED3), workbenchProperties.getProperty(LAST_USED3_URL));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    private void lastUsed4MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED4_URL))), false);
            setLastUsed(workbenchProperties.getProperty(LAST_USED4), workbenchProperties.getProperty(LAST_USED4_URL));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param file
     */
    private void openSchemaFrame(File file, boolean newFile) {
        try {

            if (! newFile) {
                // check if file not already open
                if (checkFileOpen(file)) {
                    return;
                }
                // check if schema file exists
                if (! file.exists()) {
                    JOptionPane.showMessageDialog(this, file.getAbsolutePath() + " File not found.", "Alert", JOptionPane.WARNING_MESSAGE);
                    return;
                }
                // check if file is writable
                if (! file.canWrite()) {
                    JOptionPane.showMessageDialog(this, file.getAbsolutePath() + " File is locked by another application.", "Alert", JOptionPane.WARNING_MESSAGE);
                    return;
                }
                // check if schema file is valid by initiating a mondrian connection
                try {
                    // this connection parses the catalog file which if invalid will throw exception
                    String connectString = "Provider=mondrian;" +
							"Jdbc=" + jdbcConnectionUrl + ";" +
								"Catalog=" + file.toURL().toString() + ";";

					if (jdbcUsername != null && jdbcUsername.length() > 0) {
						connectString = connectString + "JdbcUser=" + jdbcUsername + ";";
					}
					if (jdbcPassword != null && jdbcPassword.length() > 0) {
						connectString = connectString + "JdbcPassword=" + jdbcPassword + ";";
					}

					connection = DriverManager.getConnection(connectString, null, false);
				} catch (Exception ex) {
					System.out.println("Exception  : Schema file is invalid."+ex.getMessage());
					ex.printStackTrace(); //====
				} catch (Error err) {
					System.out.println("Error : Schema file is invalid."+err.getMessage());
					err.printStackTrace(); //====
				}
			}

			final JInternalFrame schemaFrame = new JInternalFrame();
			schemaFrame.setTitle("Schema - " + file.getName());
			//===Class.forName(jdbcDriverClassName);
			jdbcMetaData = new JDBCMetaData(jdbcDriverClassName, jdbcConnectionUrl, jdbcUsername, jdbcPassword);

            schemaFrame.getContentPane().add(new SchemaExplorer(file, jdbcMetaData, newFile, schemaFrame));

            String errorOpening = ((SchemaExplorer) schemaFrame.getContentPane().getComponent(0)).getErrMsg() ;
            if (errorOpening != null) {
                JOptionPane.showMessageDialog(this,errorOpening, "Error", JOptionPane.ERROR_MESSAGE);
                schemaFrame.setClosed(true);
                return;
            }

            schemaFrame.setBounds(0, 0, 1000, 650);
            schemaFrame.setClosable(true);
            schemaFrame.setIconifiable(true);
            schemaFrame.setMaximizable(true);
            schemaFrame.setResizable(true);
            schemaFrame.setVisible(true);

            desktopPane.add(schemaFrame, javax.swing.JLayeredPane.DEFAULT_LAYER);
            schemaFrame.show();
            schemaFrame.setMaximum(true);

            // display jdbc connection status warning, if connection is uncsuccessful
            if (jdbcMetaData.getErrMsg() != null) {
                JOptionPane.showMessageDialog(this, "Database connection could not be done.\n"+jdbcMetaData.getErrMsg()+"\n All validations related to database will be ignored.", "Alert" , JOptionPane.WARNING_MESSAGE);
            }

            final javax.swing.JMenuItem schemaMenuItem = new javax.swing.JMenuItem();
            schemaMenuItem.setText(windowMenuMapIndex++ + " " +file.getName());
            schemaMenuItem.addActionListener(new java.awt.event.ActionListener() {
                public void actionPerformed(java.awt.event.ActionEvent evt) {
                    try {
                        if (schemaFrame.isIcon()) {
                            schemaFrame.setIcon(false);
                        } else {
                            schemaFrame.setSelected(true);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });

            windowMenu.add(schemaMenuItem,0);
            windowMenu.setEnabled(true);

            windowMenu.add(jSeparator3,-1);
            windowMenu.add(cascadeMenuItem,-1);
            windowMenu.add(tileMenuItem,-1);
            windowMenu.add(minimizeMenuItem, -1);
            windowMenu.add(maximizeMenuItem, -1);
            windowMenu.add(closeAllMenuItem,-1);

            // add the file details in menu map
            schemaWindowMap.put(schemaFrame, schemaMenuItem);
            updateMDXCatalogList();

            schemaFrame.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

            schemaFrame.addInternalFrameListener(new InternalFrameAdapter() {
                public void internalFrameClosing(InternalFrameEvent e) {
                    if (schemaFrame.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                        SchemaExplorer se = (SchemaExplorer) schemaFrame.getContentPane().getComponent(0);
                        int response = confirmFrameClose(schemaFrame, se);
                        if (response == 3) {    // not dirty
                            if (se.isNewFile()) {
                            //System.out.println("File ==="+se.getSchemaFile()+"  deleted=="+se.getSchemaFile().delete());
                                se.getSchemaFile().delete();
                            }
                            // default case for no save and not dirty
                            schemaWindowMap.remove(schemaFrame); //schemaWindowMap.remove(se.getSchemaFile());
                            updateMDXCatalogList();
                            schemaFrame.dispose();
                            windowMenu.remove(schemaMenuItem);
                        }
                    }
                }
            });

            schemaFrame.setFocusable(true);
            schemaFrame.addFocusListener(new FocusAdapter() {
                public void focusGained(FocusEvent e) {
                    if (schemaFrame.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                        SchemaExplorer se = (SchemaExplorer) schemaFrame.getContentPane().getComponent(0);
                        viewXMLMenuItem.setSelected(se.isEditModeXML());    // update view menu based on schemaframe who gained focus
                    }
                }

                public void focusLost(FocusEvent e) {
                    if (schemaFrame.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                        SchemaExplorer se = (SchemaExplorer) schemaFrame.getContentPane().getComponent(0);
                        viewXMLMenuItem.setSelected(se.isEditModeXML());  // update view menu based on
                    }
                }

            });
            viewXMLMenuItem.setSelected(false);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    private void openMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JFileChooser jfc = new JFileChooser();
        try {
            jfc.setFileSelectionMode(JFileChooser.FILES_ONLY) ;
            jfc.setFileFilter(new javax.swing.filechooser.FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getName().toLowerCase().endsWith(".xml")
                    || pathname.isDirectory();
                }
                public String getDescription() {
                    return "Mondrian Schema files (*.xml)";
                }

            });
            jfc.setCurrentDirectory(new File(new URI(workbenchProperties.getProperty(LAST_USED1_URL))));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("==Could not set file chooser. last used file does not exist");
        }
        MondrianProperties.instance();
        if (jfc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            try {
                //setLastUsed(jfc.getSelectedFile().getName(), jfc.getSelectedFile().toURL().toString());
                setLastUsed(jfc.getSelectedFile().getName(), jfc.getSelectedFile().toURI().toURL().toString());
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }

            openSchemaFrame(jfc.getSelectedFile(), false);
        }
    }

    // checks if file already open in schema explorer
    private boolean checkFileOpen(File file) {

        Iterator it = schemaWindowMap.keySet().iterator();  // keys=schemaframes
        while (it.hasNext()) {
            JInternalFrame elem = (JInternalFrame) it.next();
            File f = ((SchemaExplorer) elem.getContentPane().getComponent(0)).getSchemaFile();
            if (f.equals(file)) {
                try {
                    elem.setSelected(true); // make the schema file active
                    return true;
                } catch(Exception ex) {
                    schemaWindowMap.remove(elem); // remove file from map as schema frame does not exist
                    break;
                }
            }
        }
        return false;


        /* // file->schema frame
        JInternalFrame elem = (JInternalFrame) schemaWindowMap.get(file);
        if (elem != null) {
            try {
                elem.setSelected(true); // make the schema file active
                return true;
            } catch(Exception ex) {
                schemaWindowMap.remove(file); // remove file from map as schema frame does not exist
            }
        }
        return false;
         */
        /*
        Iterator it = schemaWindowMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry elem = (Map.Entry) it.next();
            File value = (File)elem.getValue();
            JInternalFrame key = (JInternalFrame)elem.getKey() ;

            if (  value.equals(file)  &&  (key.getContentPane().getComponent(0) instanceof SchemaExplorer) ) {
                try {
                    key.setSelected(true); // make the schema file active
                    return true;
                } catch(Exception ex) {
                    schemaWindowMap.remove(key); // remove file from map as schema frame does not exist
                    break;
                }
            }
        }
        return false;
         */
    }


    private void exitMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        storeWorkbenchProperties();
        closeAllSchemaFrames(true);
    }

    /**
     * parseArgs - parse arguments passed into Workbench.
     *
     * @param args the command line arguments
     *
     * Right now, it's very simple.  Just search through the list
     * of arguments.  If it begins with -f=, then the rest is a file name.
     * Ignore any others.  We can make this more complicated later if we
     * need to.
     */
    private void parseArgs(String args[]) {
        for (int argNum=0; argNum < args.length; argNum++) {
            if (args[argNum].startsWith("-f=")) {
                openFile = args[argNum].substring(3);
            }
        }
    }

    public static String getTooltip(String titleName) {
        try {
            return resBundle.getString(titleName);
        } catch (MissingResourceException e) {
            return "No help available for '" + titleName + "'";
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
    	try {
	        Workbench w = new Workbench();
	        w.parseArgs(args);
	        w.setSize(800, 600);
	        // if user specified a file to open, do so now.
	        if (w.openFile != null) {
	            File f= new File(w.openFile);
	            if (f.canRead()) {
	                //w.openSchemaFrame(f.getAbsoluteFile());
	                w.openSchemaFrame(f.getAbsoluteFile(), false); // parameter to indicate this is a new or existing catalog file
	            }
	        }
	        w.show();
    	} catch (Throwable ex) {
			ex.printStackTrace();
    	}
    }

// Variables declaration - do not modify
    private javax.swing.JButton toolbarSaveAsButton;
    private javax.swing.JMenuItem openMenuItem;
    private javax.swing.JMenuItem lastUsed1MenuItem;
    private javax.swing.JMenuItem lastUsed2MenuItem;
    private javax.swing.JMenuItem lastUsed3MenuItem;
    private javax.swing.JMenuItem lastUsed4MenuItem;
    private javax.swing.JMenu fileMenu;
    private javax.swing.JMenuItem newQueryMenuItem;
    private javax.swing.JMenuItem newQueryMenuItem2;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JButton toolbarOpenButton;
    private javax.swing.JButton toolbarNewButton;
    private javax.swing.JButton toolbarNewArrowButton;
    private javax.swing.JSeparator jSeparator1;
    private javax.swing.JSeparator jSeparator2;
    private javax.swing.JSeparator jSeparator3;
    private javax.swing.JSeparator jSeparator4;
    private javax.swing.JMenuItem cutMenuItem;
    private javax.swing.JMenuBar menuBar;
    private javax.swing.JMenuItem saveMenuItem;
    private javax.swing.JMenuItem newJDBCExplorerMenuItem;
    private javax.swing.JMenuItem newJDBCExplorerMenuItem2;
    private javax.swing.JCheckBoxMenuItem viewCubesMenuItem;
    private javax.swing.JButton toolbarSaveButton;
    private javax.swing.JMenuItem copyMenuItem;
    private javax.swing.JDesktopPane desktopPane;
    private javax.swing.JMenu viewMenu;
    private javax.swing.JMenu toolsMenu;
    private javax.swing.JMenu newMenu;
    private javax.swing.JMenuItem deleteMenuItem;
    private javax.swing.JMenuItem newSchemaMenuItem;
    private javax.swing.JMenuItem newSchemaMenuItem2;
    private javax.swing.JMenuItem exitMenuItem;
    private javax.swing.JButton toolbarPreferencesButton;
    private javax.swing.JCheckBoxMenuItem viewMeasuresMenuItem;
    private javax.swing.JMenu editMenu;
    private javax.swing.JMenuItem pasteMenuItem;
    private javax.swing.JMenuItem preferencesMenuItem;
    private javax.swing.JCheckBoxMenuItem viewDimensionsMenuItem;
    private javax.swing.JCheckBoxMenuItem viewXMLMenuItem;
    private javax.swing.JMenuItem saveAsMenuItem;
    private javax.swing.JToolBar jToolBar1;
    private javax.swing.JToolBar jToolBar2;
    private javax.swing.JPopupMenu toolbarNewPopupMenu;
    private javax.swing.JMenu windowMenu;
    private javax.swing.JMenu helpMenu;
    private javax.swing.JMenuItem aboutMenuItem;
    private javax.swing.JMenuItem cascadeMenuItem;
    private javax.swing.JMenuItem tileMenuItem;
    private javax.swing.JMenuItem minimizeMenuItem;
    private javax.swing.JMenuItem maximizeMenuItem;
    private javax.swing.JMenuItem closeAllMenuItem;
// End of variables declaration


    /*
class SchemaFrameComp implements Comparator {
  public int compare (Object o1, Object o2) {
    return o1 == o2;
  }
}
     **/

    public javax.swing.JCheckBoxMenuItem getViewXMLMenuItem() {
        return viewXMLMenuItem; // used by schema framewhen it uses 'view xml' to update view xml menu item
    }


}

// End Workbench.java
