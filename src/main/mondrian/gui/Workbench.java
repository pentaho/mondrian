/*
 * Workbench.java
 *  * 
 * This software is subject to the terms of the Common Public License
 * Agreement, available at the following URL:
 * http://www.opensource.org/licenses/cpl.html.
 * Copyright (C) 1999-2003 Kana Software, Inc. and others.
 * All Rights Reserved.
 * You must accept the terms of that agreement to use this software.
 * 
 * Created on September 26, 2002, 11:28 AM
 * Modified on 15-Jun-2003 by ebengtso
 *  
 */
package mondrian.gui;

import mondrian.olap.Connection;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianProperties;

import javax.swing.*;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 *
 * @author  sean
 */
public class Workbench extends javax.swing.JFrame
{

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

	private final ClassLoader myClassLoader;
	private Properties workbenchProperties;

	/** Creates new form Workbench */
	public Workbench()
	{
		myClassLoader = this.getClass().getClassLoader();

		loadWorkbenchProperties();
		initDataSource();
		initComponents();
		ResourceBundle resources = ResourceBundle.getBundle("mondrian.gui.resources.gui", Locale.getDefault(), myClassLoader);

		ImageIcon icon = new javax.swing.ImageIcon(myClassLoader.getResource(resources.getString("cube")));

		this.setIconImage(icon.getImage());
	}

	/**
	 * load properties
	 * 
	 * @return
	 */
	private void loadWorkbenchProperties()
	{
		workbenchProperties = new Properties();
		try
		{
			workbenchProperties.load(new FileInputStream(new File("workbench.properties")));
		}
		catch (Exception e)
		{
			// TODO deal with exception
		}
	}

	/**
	 * save properties 
	 * @param propertyName
	 * @return
	 */
	private void storeWorkbenchProperties()
	{
		//save properties to file		
		OutputStream out = null;
		try
		{
			out = (OutputStream) new FileOutputStream(new File("workbench.properties"));
			workbenchProperties.store(out, "Workbench configuration");
		}
		catch (Exception e)
		{
			//TODO deal with exception
		}
		finally
		{
			try
			{
				out.close();
			}
			catch (IOException eIO)
			{
				//TODO deal with exception
			}
		}
	}

	/**
	 * Initialize the data source from a property file
	 */
	private void initDataSource()
	{
		jdbcDriverClassName = workbenchProperties.getProperty("jdbcDriverClassName");
		jdbcConnectionUrl = workbenchProperties.getProperty("jdbcConnectionUrl");
	}

	/** This method is called from within the constructor to
	 * initialize the form.
	 */
	private void initComponents()
	{
		desktopPane = new javax.swing.JDesktopPane();
		jToolBar1 = new javax.swing.JToolBar();
		toolbarNewButton = new javax.swing.JButton();
		toolbarOpenButton = new javax.swing.JButton();
		toolbarSaveButton = new javax.swing.JButton();
		toolbarSaveAsButton = new javax.swing.JButton();
		jPanel1 = new javax.swing.JPanel();
		toolbarPreferencesButton = new javax.swing.JButton();
		menuBar = new javax.swing.JMenuBar();
		fileMenu = new javax.swing.JMenu();
		newMenu = new javax.swing.JMenu();
		newSchemaMenuItem = new javax.swing.JMenuItem();
		newQueryMenuItem = new javax.swing.JMenuItem();
		newJDBCExplorerMenuItem = new javax.swing.JMenuItem();
		openMenuItem = new javax.swing.JMenuItem();
		lastUsed1MenuItem = new javax.swing.JMenuItem();
		lastUsed2MenuItem = new javax.swing.JMenuItem();
		lastUsed3MenuItem = new javax.swing.JMenuItem();
		lastUsed4MenuItem = new javax.swing.JMenuItem();
		saveMenuItem = new javax.swing.JMenuItem();
		saveAsMenuItem = new javax.swing.JMenuItem();
		jSeparator1 = new javax.swing.JSeparator();
		jSeparator2 = new javax.swing.JSeparator();
		exitMenuItem = new javax.swing.JMenuItem();
		editMenu = new javax.swing.JMenu();
		cutMenuItem = new javax.swing.JMenuItem();
		copyMenuItem = new javax.swing.JMenuItem();
		pasteMenuItem = new javax.swing.JMenuItem();
		deleteMenuItem = new javax.swing.JMenuItem();
		viewMenu = new javax.swing.JMenu();
		viewDimensionsMenuItem = new javax.swing.JCheckBoxMenuItem();
		viewMeasuresMenuItem = new javax.swing.JCheckBoxMenuItem();
		viewCubesMenuItem = new javax.swing.JCheckBoxMenuItem();

		setTitle("Mondrian Workbench");
		addWindowListener(new java.awt.event.WindowAdapter()
		{
			public void windowClosing(java.awt.event.WindowEvent evt)
			{
				exitForm(evt);
			}
		});

		getContentPane().add(desktopPane, java.awt.BorderLayout.CENTER);
		toolbarNewButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/New16.gif")));
		toolbarNewButton.setToolTipText("New");
		jToolBar1.add(toolbarNewButton);

		toolbarOpenButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Open16.gif")));
		toolbarOpenButton.setToolTipText("New");
		toolbarOpenButton.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				openMenuItemActionPerformed(evt);
			}
		});

		jToolBar1.add(toolbarOpenButton);

		toolbarSaveButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Save16.gif")));
		toolbarSaveButton.setToolTipText("New");
		toolbarSaveButton.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				saveMenuItemActionPerformed(evt);
			}
		});

		jToolBar1.add(toolbarSaveButton);

		toolbarSaveAsButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/SaveAs16.gif")));
		toolbarSaveAsButton.setToolTipText("New");
		toolbarSaveAsButton.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				saveAsMenuItemActionPerformed(evt);
			}
		});

		jToolBar1.add(toolbarSaveAsButton);

		jPanel1.setMaximumSize(new java.awt.Dimension(8, 32767));
		jToolBar1.add(jPanel1);

		toolbarPreferencesButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Preferences16.gif")));
		toolbarPreferencesButton.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				toolbarPreferencesButtonActionPerformed(evt);
			}
		});

		jToolBar1.add(toolbarPreferencesButton);

		getContentPane().add(jToolBar1, java.awt.BorderLayout.NORTH);

		fileMenu.setText("File");
		newMenu.setText("New");

		newQueryMenuItem.setText("MDX Query");
		newQueryMenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				newQueryMenuItemActionPerformed(evt);
			}
		});

		newMenu.add(newQueryMenuItem);

		newJDBCExplorerMenuItem.setText("JDBC Explorer");
		newJDBCExplorerMenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				newJDBCExplorerMenuItemActionPerformed(evt);
			}
		});

		newMenu.add(newJDBCExplorerMenuItem);

		fileMenu.add(newMenu);

		openMenuItem.setText("Open");
		openMenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				openMenuItemActionPerformed(evt);
			}
		});

		fileMenu.add(openMenuItem);

		saveMenuItem.setText("Save");
		saveMenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				saveMenuItemActionPerformed(evt);
			}
		});

		fileMenu.add(saveMenuItem);

		saveAsMenuItem.setText("Save As ...");
		saveAsMenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				saveAsMenuItemActionPerformed(evt);
			}
		});

		fileMenu.add(saveAsMenuItem);

		//add last used
		fileMenu.add(jSeparator2);

		lastUsed1MenuItem.setText(workbenchProperties.getProperty("lastUsed1"));
		lastUsed1MenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				lastUsed1MenuItemActionPerformed(evt);
			}
		});
		fileMenu.add(lastUsed1MenuItem);

		lastUsed2MenuItem.setText(workbenchProperties.getProperty("lastUsed2"));
		lastUsed2MenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				lastUsed2MenuItemActionPerformed(evt);
			}
		});
		fileMenu.add(lastUsed2MenuItem);

		lastUsed3MenuItem.setText(workbenchProperties.getProperty("lastUsed3"));
		lastUsed3MenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				lastUsed3MenuItemActionPerformed(evt);
			}
		});
		fileMenu.add(lastUsed3MenuItem);

		lastUsed4MenuItem.setText(workbenchProperties.getProperty("lastUsed4"));
		lastUsed4MenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
				lastUsed4MenuItemActionPerformed(evt);
			}
		});
		fileMenu.add(lastUsed4MenuItem);

		updateLastUsedMenu();
		fileMenu.add(jSeparator1);

		exitMenuItem.setText("Exit");
		exitMenuItem.addActionListener(new java.awt.event.ActionListener()
		{
			public void actionPerformed(java.awt.event.ActionEvent evt)
			{
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

		setJMenuBar(menuBar);

		pack();
	}

	private void newJDBCExplorerMenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		try
		{
			JInternalFrame jf = new JInternalFrame();
			jf.setTitle("JDBC Explorer - " + this.jdbcConnectionUrl);

			Class.forName(this.jdbcDriverClassName);
			java.sql.Connection con = java.sql.DriverManager.getConnection(this.jdbcConnectionUrl);

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
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	private void toolbarPreferencesButtonActionPerformed(java.awt.event.ActionEvent evt)
	{
		PreferencesDialog pd = new PreferencesDialog(this, true);
		pd.setJDBCConnectionUrl(jdbcConnectionUrl);
		pd.setJDBCDriverClassName(jdbcDriverClassName);

		pd.show();

		if (pd.accepted())
		{
			jdbcConnectionUrl = pd.getJDBCConnectionUrl();
			jdbcDriverClassName = pd.getJDBCDriverClassName();

			workbenchProperties.setProperty("jdbcDriverClassName", jdbcDriverClassName);
			workbenchProperties.setProperty("jdbcConnectionUrl", jdbcConnectionUrl);

		}

	}

	private void newQueryMenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		JInternalFrame jf = new JInternalFrame();
		jf.setTitle("MDX Query");
		QueryPanel qp = new QueryPanel();
		qp.setConnection(connection);
		jf.getContentPane().add(qp);
		jf.setBounds(0, 0, 500, 480);
		jf.setClosable(true);
		jf.setIconifiable(true);
		jf.setMaximizable(true);
		jf.setResizable(true);
		jf.setVisible(true);

		desktopPane.add(jf);

		jf.show();

	}

	private void saveAsMenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		JInternalFrame jf = desktopPane.getSelectedFrame();
		if (jf.getContentPane().getComponent(0) instanceof SchemaExplorer)
		{
			SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);
			java.io.File schemaFile = se.getSchemaFile();
			MondrianDef.Schema schema = se.getSchema();
			JFileChooser jfc = new JFileChooser();
			MondrianProperties.instance();
			jfc.setSelectedFile(schemaFile);
			if (jfc.showSaveDialog(this) == JFileChooser.APPROVE_OPTION)
			{
				try
				{
					schemaFile = jfc.getSelectedFile();
					mondrian.xom.XMLOutput out = new mondrian.xom.XMLOutput(new java.io.FileWriter(jfc.getSelectedFile()));
					schema.displayXML(out);
					se.setSchemaFile(schemaFile);
					jf.setTitle("Schema - " + schemaFile.getName());
				}
				catch (Exception ex)
				{
					ex.printStackTrace();
				}
			}
		}
	}

	private void saveMenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		JInternalFrame jf = desktopPane.getSelectedFrame();
		if (jf.getContentPane().getComponent(0) instanceof SchemaExplorer)
		{
			SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);
			java.io.File schemaFile = se.getSchemaFile();
			MondrianDef.Schema schema = se.getSchema();
			MondrianProperties.instance();
			try
			{
				mondrian.xom.XMLOutput out = new mondrian.xom.XMLOutput(new java.io.FileWriter(schemaFile));
				schema.displayXML(out);
			}
			catch (Exception ex)
			{
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
	private void setLastUsed(String name, String url)
	{
		if (workbenchProperties.getProperty(LAST_USED3) != null)
		{
			workbenchProperties.setProperty(LAST_USED4, workbenchProperties.getProperty(LAST_USED3));
			workbenchProperties.setProperty(LAST_USED4_URL, workbenchProperties.getProperty(LAST_USED3_URL));
		}
		if (workbenchProperties.getProperty(LAST_USED2) != null)
		{
			workbenchProperties.setProperty(LAST_USED3, workbenchProperties.getProperty(LAST_USED2));
			workbenchProperties.setProperty(LAST_USED3_URL, workbenchProperties.getProperty(LAST_USED2_URL));
		}
		if (workbenchProperties.getProperty(LAST_USED1) != null)
		{
			workbenchProperties.setProperty(LAST_USED2, workbenchProperties.getProperty(LAST_USED1));
			workbenchProperties.setProperty(LAST_USED2_URL, workbenchProperties.getProperty(LAST_USED1_URL));
		}
		workbenchProperties.setProperty(LAST_USED1, name);
		workbenchProperties.setProperty(LAST_USED1_URL, url);
		updateLastUsedMenu();
	}

	private void updateLastUsedMenu()
	{
		if (workbenchProperties.getProperty(LAST_USED1) == null)
			jSeparator2.setVisible(false);
		else
			jSeparator2.setVisible(true);

		if (workbenchProperties.getProperty(LAST_USED1) != null)
			lastUsed1MenuItem.setVisible(true);
		else
			lastUsed1MenuItem.setVisible(false);
		if (workbenchProperties.getProperty(LAST_USED2) != null)
			lastUsed2MenuItem.setVisible(true);
		else
			lastUsed2MenuItem.setVisible(false);
		if (workbenchProperties.getProperty(LAST_USED3) != null)
			lastUsed3MenuItem.setVisible(true);
		else
			lastUsed3MenuItem.setVisible(false);
		if (workbenchProperties.getProperty(LAST_USED4) != null)
			lastUsed4MenuItem.setVisible(true);
		else
			lastUsed4MenuItem.setVisible(false);

		lastUsed1MenuItem.setText(workbenchProperties.getProperty(LAST_USED1));
		lastUsed2MenuItem.setText(workbenchProperties.getProperty(LAST_USED2));
		lastUsed3MenuItem.setText(workbenchProperties.getProperty(LAST_USED3));
		lastUsed4MenuItem.setText(workbenchProperties.getProperty(LAST_USED4));
	}

	private void lastUsed1MenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		try
		{
			openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED1_URL))));
		}
		catch (URISyntaxException e)
		{
			e.printStackTrace();
		}
	}

	private void lastUsed2MenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		try
		{
			openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED2_URL))));
		}
		catch (URISyntaxException e)
		{
			e.printStackTrace();
		}
	}

	private void lastUsed3MenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		try
		{
			openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED3_URL))));
		}
		catch (URISyntaxException e)
		{
			e.printStackTrace();
		}
	}

	private void lastUsed4MenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		try
		{
			openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED4_URL))));
		}
		catch (URISyntaxException e)
		{
			e.printStackTrace();
		}
	}

	private void openSchemaFrame(File file)
	{
		try
		{
			JInternalFrame schemaFrame = new JInternalFrame();
			schemaFrame.setTitle("Schema - " + file.getName());
			Class.forName(jdbcDriverClassName);
			String connectString = "Provider=mondrian;" + "Jdbc=" + jdbcConnectionUrl + ";" + "Catalog=" + file.toURL().toString();
			connection = DriverManager.getConnection(connectString, null, false);

			schemaFrame.getContentPane().add(new SchemaExplorer(file));

			schemaFrame.setBounds(0, 0, 500, 480);
			schemaFrame.setClosable(true);
			schemaFrame.setIconifiable(true);
			schemaFrame.setMaximizable(true);
			schemaFrame.setResizable(true);
			schemaFrame.setVisible(true);

			desktopPane.add(schemaFrame, javax.swing.JLayeredPane.DEFAULT_LAYER);
			schemaFrame.show();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}

	}
	private void openMenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		JFileChooser jfc = new JFileChooser();
		MondrianProperties.instance();
		if (jfc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION)
		{
			try
			{
				setLastUsed(jfc.getSelectedFile().getName(), jfc.getSelectedFile().toURL().toString());
			}
			catch (MalformedURLException e)
			{
				e.printStackTrace();
			}
			openSchemaFrame(jfc.getSelectedFile());
		}
	}

	private void exitMenuItemActionPerformed(java.awt.event.ActionEvent evt)
	{
		storeWorkbenchProperties();
		System.exit(0);
	}

	/** Exit the Application */
	private void exitForm(java.awt.event.WindowEvent evt)
	{
		System.exit(0);
	}

	/**
	 * @param args the command line arguments
	 */
	public static void main(String args[])
	{
		Workbench w = new Workbench();
		w.setSize(800, 600);
		w.show();
	}

	// Variables declaration - do not modify//GEN-BEGIN:variables
	private javax.swing.JButton toolbarSaveAsButton;
	private javax.swing.JMenuItem openMenuItem;
	private javax.swing.JMenuItem lastUsed1MenuItem;
	private javax.swing.JMenuItem lastUsed2MenuItem;
	private javax.swing.JMenuItem lastUsed3MenuItem;
	private javax.swing.JMenuItem lastUsed4MenuItem;
	private javax.swing.JMenu fileMenu;
	private javax.swing.JMenuItem newQueryMenuItem;
	private javax.swing.JPanel jPanel1;
	private javax.swing.JButton toolbarOpenButton;
	private javax.swing.JButton toolbarNewButton;
	private javax.swing.JSeparator jSeparator1;
	private javax.swing.JSeparator jSeparator2;
	private javax.swing.JMenuItem cutMenuItem;
	private javax.swing.JMenuBar menuBar;
	private javax.swing.JMenuItem saveMenuItem;
	private javax.swing.JMenuItem newJDBCExplorerMenuItem;
	private javax.swing.JCheckBoxMenuItem viewCubesMenuItem;
	private javax.swing.JButton toolbarSaveButton;
	private javax.swing.JMenuItem copyMenuItem;
	private javax.swing.JDesktopPane desktopPane;
	private javax.swing.JMenu viewMenu;
	private javax.swing.JMenu newMenu;
	private javax.swing.JMenuItem deleteMenuItem;
	private javax.swing.JMenuItem newSchemaMenuItem;
	private javax.swing.JMenuItem exitMenuItem;
	private javax.swing.JButton toolbarPreferencesButton;
	private javax.swing.JCheckBoxMenuItem viewMeasuresMenuItem;
	private javax.swing.JMenu editMenu;
	private javax.swing.JMenuItem pasteMenuItem;
	private javax.swing.JCheckBoxMenuItem viewDimensionsMenuItem;
	private javax.swing.JMenuItem saveAsMenuItem;
	private javax.swing.JToolBar jToolBar1;
	// End of variables declaration

}
