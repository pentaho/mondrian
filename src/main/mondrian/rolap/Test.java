/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.MondrianResource;
import mondrian.olap.Connection;
import mondrian.olap.DriverManager;

import java.io.PrintWriter;
import java.util.Vector;

/**
 * todo:
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 **/
public class Test {
	PrintWriter pw;
	RolapConnection connection;

	static public void main(String[] args) {
		Test test = new Test(args);
		if (true) {
			test.run();
		} else {
			try {
				test.convertFoodMart();
			} catch (java.sql.SQLException e) {
				System.out.println("Error: " + mondrian.olap.Util.getErrorMessage(e));
			}
		}
	}
	Test(String[] args)
	{
		pw = new PrintWriter(System.out, true);
		String connectString = "Data Source=LOCALHOST;Provider=msolap;Catalog=Foodmart";
        boolean fresh = true;
		connection = (RolapConnection) DriverManager.getConnection(connectString, null, fresh);
	}
	void convertFoodMart() throws java.sql.SQLException
	{
		java.sql.Connection connection = null;
		java.sql.Statement statement = null, statement2 = null;
		try {
			try {
				Class.forName("com.ms.jdbc.odbc.JdbcOdbcDriver");
			} catch (ClassNotFoundException e) {
			}
			String connectString = "jdbc:odbc:DSN=FoodMart2";
			connection = java.sql.DriverManager.getConnection(connectString);
			statement = connection.createStatement();
			statement2 = connection.createStatement();
			String sql =
				"select * from (" +
				" select *, \"fname\" + ' ' + \"lname\" as \"name\" from \"customer\")" +
				"order by \"country\", \"state_province\", \"city\", \"name\"";
//			sql = "select * from \"customer\" " +
//				"where (\"country\", \"state_province\") = " +
//				" ('Canada', 'BC')";
//			sql = "select * from \"customer\" " +
//				"where \"country\" = 'Canada' " +
//				" and \"state_province\" = 'BC'";
			java.sql.ResultSet resultSet = statement.executeQuery(sql);
			if (true) {
//				return;
			}
			int i = 0;
			while (resultSet.next()) {
				int customer_id = resultSet.getInt("customer_id");
				statement2.executeUpdate(
					"update \"customer\" set \"ordinal\" = " + (++i * 3) +
					" where \"customer_id\" = " + customer_id);
			}
			connection.commit();
		} finally {
			if (statement2 != null) {
				try {statement2.close();} catch (java.sql.SQLException e) {} 
			}
			if (statement != null) {
				try {statement.close();} catch (java.sql.SQLException e) {} 
			}
			if (connection != null) {
				try {connection.close();} catch (java.sql.SQLException e) {} 
			}
		}
	}

	void run() 
	{
		RolapCube salesCube = (RolapCube) connection.getSchema().lookupCube("Sales", true);
		RolapHierarchy measuresHierarchy = salesCube.measuresHierarchy;
		testMemberReader(measuresHierarchy.memberReader);

		RolapHierarchy genderHierarchy = (RolapHierarchy)
			salesCube.lookupHierarchy("Gender", false);
		testMemberReader(genderHierarchy.memberReader);

		RolapHierarchy customerHierarchy = (RolapHierarchy)
			salesCube.lookupHierarchy("Customers", false);
		testMemberReader(customerHierarchy.memberReader);
	}
	void testMemberReader(MemberReader reader)
	{
		pw.println();
		pw.println("MemberReader class=" + reader.getClass());
		pw.println("Count=" + reader.getMemberCount());

		pw.print("Root member(s)=");
		RolapMember[] rootMembers = reader.getRootMembers();
		print(rootMembers);
		pw.println();

		RolapLevel[] levels = (RolapLevel[]) rootMembers[0].getHierarchy().getLevels();
		RolapLevel level = levels[levels.length > 1 ? 1 : 0];
		pw.print("Members at level " + level.getUniqueName() + " are ");
		RolapMember[] members = reader.getMembersInLevel(level, 0, Integer.MAX_VALUE);
		print(members);
		pw.println();

		pw.println("First children of first children: {");
		Vector firstChildren = new Vector();
		RolapMember member = rootMembers[0];
		while (member != null) {
			firstChildren.addElement(member);
			pw.print("\t");
			print(member);
			RolapMember[] children = reader.getMemberChildren(
				new RolapMember[] {member});
			if (children == null) {
				break;
			}
			pw.print(" (" + children.length + " children)");
			RolapMember leadMember = (RolapMember) member.getLeadMember(5);
			pw.print(", lead(5)=");
			print(leadMember);
			if (children.length > 1) {
				member = children[1];
			} else if (children.length > 0) {
				member = children[0];
			} else {
				member = null;
			}
			pw.println();
		}
		pw.println("}");
	}
	private void print(RolapMember member)
	{
		if (member == null) {
			pw.print("Member(null)");
			return;
		}
		pw.print("Member(" + member.getUniqueName() + ")");
	}
	private void print(RolapMember[] members)
	{
		pw.print("{");
		for (int i = 0; i < members.length; i++) {
			if (i > 0) {
				pw.print(", ");
			}
			print(members[i]);
		}
		pw.print("}");
	}		
}


// End Test.java
