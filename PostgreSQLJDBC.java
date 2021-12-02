import java.sql.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import java.math.BigDecimal;

import java.util.Scanner;

public class PostgreSQLJDBC 
{
	public static void main(String args[]) 
	{
		Connection c = null;
		try
		{
			// Load Postgresql Driver class
			Class.forName("org.postgresql.Driver");
			// Using Driver class connect to databased on localhost, port=5432, database=postgres, user=postgres, password=postgres. If cannot connect then exception will be generated (try-catch block)
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres","postgres", "postgres");
			System.out.println("Opened database successfully");
			
			// Create instance of this class to call other methods
			PostgreSQLJDBC p = new PostgreSQLJDBC();
			p.setSearchPath(c);
			p.insertInTable(c);
			p.queryTable(c);
			p.updTable(c);
			p.call_book_taxi(c);
			p.delTable(c);
			c.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
	
	//Methhod for setting a search path
	void setSearchPath(Connection c)
	{
		Statement stmt = null;
		try
		{
			stmt = c.createStatement();
			String sql = "SET search_path TO project;";
			stmt.executeUpdate(sql);
			stmt.close();
			System.out.println("Changed Search Path successfully");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}

	//Method for inserting a row in the table works_in
	void insertInTable(Connection c)
	{
		PreparedStatement stmt = null;
		String sql = "INSERT INTO works_in VALUES (?, ?)";
		try
		{
			stmt = c.prepareStatement(sql);

			System.out.println("Enter Shift ID:");
			short a = inputclass.in.nextShort();
			System.out.println("You entered Shift ID "+a);
			stmt.setShort(1, a);

			System.out.println("Enter Driver ID:");
			int b = inputclass.in.nextInt();
			System.out.println("You entered Driver ID "+a);
			stmt.setInt(2, b);
			
			int affectedRows = stmt.executeUpdate();
			stmt.close();
			System.out.println("Inserted in Table Works_in successfully: Rows Affected: " + affectedRows);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}
	
	//Method for executing select query
	void queryTable(Connection c)
	{
		Statement stmt = null;
		try
		{
			stmt = c.createStatement();

			ResultSet rs = stmt.executeQuery("select r.booking_id AS Bid,r.date_r AS dat,p.user_id AS Uid,r.reg_no AS regno,p.fname AS f_name,p.mname AS m_name,p.lname as l_name from ride as r join passenger as p on(r.user_id=p.user_id) where booking_id in (select booking_id from payment_detail as p where status='False')" );
			
			while(rs.next())
			{
				// Datatype mapping and functions to use with JDBC is found at https://www.tutorialspoint.com/jdbc/jdbc-data-types.htm
				String regno, f_name, m_name, l_name;
				BigDecimal Bid, Uid;
				java.sql.Date dat;
				// Employee Infor
				regno = rs.getString("regno");
				f_name = rs.getString("f_name");
				m_name = rs.getString("m_name");
				l_name = rs.getString("l_name");
				Bid = rs.getBigDecimal("Bid");
				Uid = rs.getBigDecimal("Uid");
				dat = rs.getDate("dat");
								
				System.out.println("Passenger Detail: User_ID-" + Uid + ", Name-" + f_name + m_name + l_name +  " Ride_Detail: Booking_ID-" + Bid + ", Date-" + dat + ", Regisration_No:" + regno);
			}
			
			stmt.close();
			System.out.println("Table Queried successfully");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}

   }
   

    //Method for updating a driver table
   	void updTable(Connection c)
	{
		PreparedStatement stmt = null;
		String sql = "UPDATE driver SET mobile_no = ? WHERE driver_id = ?";
		try
		{
			stmt = c.prepareStatement(sql);
			stmt.setBigDecimal(1, new BigDecimal(7802873233L));
			stmt.setBigDecimal(2, new BigDecimal(2669));
			int affectedRows = stmt.executeUpdate();
			stmt.close();
			System.out.println("Table Updated successfully: Rows Updated: " + affectedRows);

		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}

	//Method for deleting a row from works_in table
	void delTable(Connection c)
	{
		PreparedStatement stmt = null;
		String sql = "DELETE from works_in where shift_id = ? and driver_id = ?";
		try
		{

			stmt = c.prepareStatement(sql);

			System.out.println("Enter Shift ID of row which you want to delete:");
			short a = inputclass.in.nextShort();
			System.out.println("You entered Shift ID "+a);
			stmt.setShort(1, a);

			System.out.println("Enter Driver ID of row which you want to delete:");
			int b = inputclass.in.nextInt();
			System.out.println("You entered Driver ID "+a);
			stmt.setInt(2, b);
			
			int affectedRows = stmt.executeUpdate();
			stmt.close();
			System.out.println("Deleted from Table Works_in successfully: Rows Affected: " + affectedRows);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}

	//Method for calling stored procedure book_taxi
   	void call_book_taxi(Connection c)
	{
		CallableStatement stmt = null;
		try
		{
			int userid  = 11081;
			String modelno = "ECO00125PC";
			String picloc = "abc";
			String sql = "call book_taxi(?,?,?,?)";
			stmt = c.prepareCall(sql);
			stmt.setInt(1, userid);
			stmt.setString(2, modelno);
			stmt.setString(3, picloc);
			stmt.setNull(4, Types.NULL);
			stmt.execute();
			
			System.out.println("Function Called successfully");
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
		}
	}

}

class inputclass
{
    static Scanner in = new Scanner(System.in);
}