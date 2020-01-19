import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/*
 * 
 * External References used
 * https://docs.oracle.com/javase/8/docs/api/java/sql/PreparedStatement.html
 * https://codeburst.io/insert-with-select-statement-for-columns-with-foreign-key-constraint-in-mysql-with-examples-f9ab57c8e4dd
 * https://blog.jooq.org/2014/01/16/what-you-didnt-know-about-jdbc-batch/
 * https://coderanch.com/t/627237/databases/track-failed-record-batch-excution
 * http://tutorials.jenkov.com/jdbc/batchupdate.html
 * https://docs.oracle.com/javase/tutorial/jdbc/basics/prepared.html
 * https://stackoverflow.com/questions/23776145/jdbc-display-retrive-data-into-table-structure
 */

class JDBCConn
{
	Connection conn;
	String hostname,database;
	
	public JDBCConn(String sys,String data)
	{
		hostname = sys;
		database = data;
	}
	
	public void InsertRecordBook(JDBCConn conObj) throws IOException, SQLException
	{
		
		Statement stmt = null;
		try {
			PreparedStatement ps = null;
			String jdbc_insert_sql = "INSERT INTO BOOK VALUES (?,?,?,?,?,?,?)";
			stmt = conObj.conn.createStatement();
			String filename = "./Input_Data/Book.csv";
			System.out.println(filename);
			File file = new File(filename);
			Scanner scObj = new Scanner(file);
			int row = 0;
			//System.out.println(scObj.hasNext());
			//Running this loop for each row of the csv files
			while(scObj.hasNext())
			{
				String data = scObj.nextLine();
				System.out.println(data);
				//getting each column value from single row of csv file
				String [] inputData = data.split(",");
				System.out.println(inputData[0].trim());
				
				row++;
				try
				 {
				  stmt.executeUpdate(
				            "insert into BOOK " +
				            "values("+ inputData[0] + "," +  inputData[1] +"," + inputData[2] +
				             "," + inputData[3] + ","+ inputData[4] + ","+ inputData[5] + ","+ inputData[6] + ")");
				  System.out.println("Working inside");
			    }catch (SQLException e) {
			    	e.printStackTrace();
			   }
			}
			scObj.close();
			stmt.close();
			
			System.out.println("Insert completed successfully for the table Book.Total number of rows inserted " + row);
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void InsertRecordMember(JDBCConn conObj) throws IOException, SQLException, ParseException
	{
	   
		Statement stmt;
		PreparedStatement ps = null;
		boolean isDone = false;
		try {
			stmt = conObj.conn.createStatement();
			String filename = "./Input_Data/Member.csv";
			File file = new File(filename);
			Scanner scObj = new Scanner(file);
			//for counting number of rows inserted
			int row = 0;
			int count = 0;
			
			//Running this loop for each row of the csv files
			while(scObj.hasNext())
			{
				String data = scObj.nextLine();
				//getting each column value from single row of csv file
				String [] inputData = data.split(",");
				row++;
					try
					{				
					  String sql_stmt = "INSERT INTO Member " + 
			 		                  "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";	
						//creating parameters to be passed to the sql query statement defined above
					  ps = conObj.conn.prepareStatement(sql_stmt);
					  ps.setInt(1,Integer.parseInt(inputData[0]));
						  
						       //converting date value to dd-mm-yy format and creating parameter of type sqlDate
					               if(!inputData[1].isEmpty())
					               {
					            	   String year = inputData[1].substring(3, 5);
									   String month = inputData[1].substring(6, 8);
									   String date = inputData[1].substring(9, 11);
									   String finalDate = date + "-" + month + "-" + year;
									   System.out.println(finalDate);
									   Date utilDate = new SimpleDateFormat("dd-mm-yy").parse(finalDate); 
									   java.sql.Date sqlCardIssuingDate = new java.sql.Date(utilDate.getTime());
									   ps.setDate(2,sqlCardIssuingDate); 
					               }
					               else
					               {
					            	   ps.setNull(2,java.sql.Types.DATE);   
					               }
								  
					               if(!inputData[2].isEmpty())
							       {
							    	   String year1 = inputData[2].substring(3, 5);
									   String month1 = inputData[2].substring(6, 8);
									   String date1 = inputData[2].substring(9, 11);
									   String finalDate1 = date1 + "-" + month1 + "-" + year1;
									   Date utilDate1 = new SimpleDateFormat("dd-mm-yy").parse(finalDate1);
									   java.sql.Date sqlCardExpiryDate = new java.sql.Date(utilDate1.getTime());
									   ps.setDate(3,sqlCardExpiryDate);  
							       }
					               else
					               {
					            	   ps.setNull(3,java.sql.Types.DATE); 
					               }
							  ps.setString(4,inputData[3]);
							  ps.setString(5,inputData[4]);
							  ps.setString(6,inputData[5]);
							  ps.setString(7,inputData[6]);
							  ps.setString(8,inputData[7]);
							  ps.setString(9,inputData[8]);
							  ps.setString(10,inputData[9]);
							  ps.setString(11,inputData[10]);
							  ps.setString(12,inputData[11]);
							  
							  //executing sql statement defined by the string sql_stmt
							  try
							  {
							  ps.executeUpdate();
							  }
							  catch(Exception ex)
							  {
								 ex.printStackTrace();
								  //continue; 
								  //ps.executeUpdate();
							  }
							  finally
							  {
								  if(ps!=null)
								  {
									  ps.close();
								  }
							  }
							  
					
				}catch (Exception e) {
					   System.out.println("Error due to SQL Exception");
				        e.printStackTrace();
					   }
				//ps.close();
				
			   } 
			 scObj.close();
			 System.out.println("Insert operation successfully completed for the table Member.Total number of rows inserted " + row);
		} catch (FileNotFoundException e) {
			// TDO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public void InsertRecordCatalog(JDBCConn conObj) throws IOException, SQLException, ParseException
	{
		boolean isDone = false;
		try 
		{

			String filename = "./Input_Data/Catalog.csv";
			File file = new File(filename);
			Scanner scObj = new Scanner(file);
			int row = 0;
            //Looping for each row of csv file
			while(scObj.hasNext())
			{
				String data = scObj.nextLine();
				//getting each column value from single row of csv file
				String [] inputData = data.split(",");
				row++;
				String isbn = "";
				try
				{
					Statement smt = null;
					String select_stmt = "SELECT ISBN FROM BOOK WHERE ISBN= "+ inputData[1]+" ";
					
					try
					{
						smt = conObj.conn.createStatement();
						ResultSet rs = smt.executeQuery(select_stmt);
						
						while(rs.next())
						{
							if(rs.getString("ISBN")!=null)
							{
								isbn = rs.getString("ISBN");	
							}
						}
						String str = inputData[1];
						str = str.replaceAll("\'","");
						
						if(isbn.equals(str))
						{
							String sql_stmt = "Insert into Catalog values(?,?,?,?)";
							
							//creating parameters to be passed to the sql query statement defined above	
							PreparedStatement ps = conObj.conn.prepareStatement(sql_stmt);
						
							try
							{
						      ps.setString(1,inputData[0]);
							  ps.setString(2,isbn);
							  ps.setInt(3,Integer.parseInt(inputData[2]));
							  ps.setInt(4,Integer.parseInt(inputData[3]));
							//adding it to the batch
							  ps.addBatch();
								 
						   } catch (SQLException e) {
							   System.out.println("Error due to SQL Exception");
					        e.printStackTrace();
						   }
							//executing complete batch, here sql statements and all parameters are sent to the database
							try
							{
							
							int[] res = ps.executeBatch();
							ps.close();	  
							}
							catch(BatchUpdateException ex)
							{
							  ex.printStackTrace();	
							}
						  }  						
						}
					catch(Exception ex)
					{
						
					}
				}
				catch(Exception ex)
				{
					
				}
			}
				
			System.out.println("Insert operation successfully completed for the table Catalog. Total number of rows inserted " + row);
			isDone = true;
			scObj.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void InsertRecordBA(JDBCConn conObj) throws IOException, SQLException, ParseException
	{
		try {

			String filename = "./Input_Data/BorrowingActivity.csv";
			File file = new File(filename);
			Scanner scObj = new Scanner(file);
			int row = 0;
			Statement stmt = conObj.conn.createStatement();
			//Looping for each row of csv file
			while(scObj.hasNext())
			{
				String isbn = "";
				String data = scObj.nextLine();
				//getting each column value from single row of csv file
				String [] inputData = data.split(",");
				row++;
				try
				{
					Statement smt = null;
					String select_stmt = "SELECT ISBN FROM BOOK WHERE ISBN= "+ inputData[0]+ " ";
					
					try
					{
						smt = conObj.conn.createStatement();
						ResultSet rs = smt.executeQuery(select_stmt);
						
						while(rs.next())
						{
							if(rs.getString("ISBN")!=null)
							{
								isbn = rs.getString("ISBN");	
							}
						}
						String strISBN = inputData[0];
						strISBN = strISBN.replaceAll("\'","");
						int memberSSN = 0;
						if(isbn.equals(strISBN))
						{
							Statement secondstmt = null;
							
							String select_secondstmt = "SELECT SSN FROM MEMBER WHERE SSN = "+ Integer.parseInt(inputData[1])+ " ";
							
							try
							{
								secondstmt = conObj.conn.createStatement();
								ResultSet secondrs = secondstmt.executeQuery(select_secondstmt);
								//System.out.println(secondrs.getRow());
								while(secondrs.next())
								{
									if(secondrs.getString("SSN")!=null)
									{
										memberSSN = secondrs.getInt("SSN");	
									}
								}
								int strMemberSSN = Integer.parseInt(inputData[1]);
								//strMemberSSN = strMemberSSN.replaceAll("\'","");
								//System.out.println(isbn);
								if(memberSSN == strMemberSSN)
								{
								    //System.out.println("SSN Matched");
									//insert the record
									//create Prepared Statement and passing the SQL query string
									PreparedStatement ps = conObj.conn.prepareStatement("Insert into BorrowingActivity values(?,?,?,?,?)");
								
									try
									{
									//creating parameters to be passed to the sql query statement defined above	
								     ps.setString(1,isbn);
								     ps.setInt(2,memberSSN);
								     
								     String year = inputData[2].substring(3, 5);
									 String month = inputData[2].substring(6, 8);
									 String date = inputData[2].substring(9, 11);
									 String finalDate = date + "-" + month + "-" + year;									 
									 Date utilDate = new SimpleDateFormat("dd-mm-yy").parse(finalDate);
								     java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
									 ps.setDate(3,sqlDate);
									 
									 String year1 = inputData[3].substring(3, 5);
									 String month1 = inputData[3].substring(6, 8);
									 String date1 = inputData[3].substring(9, 11);
									 String finalDate1 = date1 + "-" + month1 + "-" + year1;
									 Date utilDate1 = new SimpleDateFormat("dd-mm-yy").parse(finalDate1);
								     java.sql.Date sqlDate1 = new java.sql.Date(utilDate1.getTime());
								     
									 ps.setDate(4,sqlDate1);
									 
									 String year2 = inputData[4].substring(3, 5);
									 String month2 = inputData[4].substring(6, 8);
									 String date2 = inputData[4].substring(9, 11);
									 String finalDate2 = date2 + "-" + month2 + "-" + year2;
									 Date utilDate2 = new SimpleDateFormat("dd-mm-yy").parse(finalDate2);
								     java.sql.Date sqlDate2 = new java.sql.Date(utilDate2.getTime());
									 
								     ps.setDate(5,sqlDate2);
								     
								     //adding it to the batch
					                 ps.addBatch();
										
								   } catch (SQLException e) {
									   System.out.println("Error due to SQL Exception");
							        e.printStackTrace();
								   }

									 //executing complete batch, here sql statements and all parameters are sent to the database
									 int[] res = ps.executeBatch();
									 ps.close();
								}
							}
							catch(Exception ex)
							{
								ex.printStackTrace();
							}
						}
					}
					catch(Exception ex)
					{
						ex.printStackTrace();
					}
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
				}
				
				   
			}
			System.out.println("Insert operation successfully completed for the table BorrowingActivity. Total number of rows inserted " + row);
			scObj.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public boolean OpenConnection() throws SQLException 
	{
		try
		{
		 //checking if the jdbc driver is available 
	 	 Class.forName("oracle.jdbc.driver.OracleDriver");
		}
		catch(ClassNotFoundException e)
		{
	     System.out.println("Driver not found exception");
	     e.printStackTrace();
	     return false;
		}
		try
		{
		//if hostname equals localhost and database name equals development then establish connection to the database url
	       if(hostname.equals("localhost"))
			 {
			  if(database.equals("Development"))
				{
				  
				  conn = DriverManager.getConnection("jdbc:oracle:thin:ANURAGSQLDEV/mini46@localhost:1521/xe");	
				}
			 }
		  else
		    {
			  System.out.println("Wrong hostname");
			}
		}
		catch(Exception ex)
		{
		 ex.printStackTrace();	
		}
		return true;
	}
   
	public void displayWeeklyReport(JDBCConn conObj,String fDate, String eDate) throws IOException, SQLException, ParseException
	{
		//check if date range falls within one week
		SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yyyy",Locale.ENGLISH);
		java.util.Date firstDate = sdf.parse(fDate);
		java.util.Date lastDate = sdf.parse(eDate);
		
		long diffInMilli = Math.abs(lastDate.getTime() - firstDate.getTime());
		long dif = TimeUnit.DAYS.convert(diffInMilli, TimeUnit.MILLISECONDS);
        
		if(dif > 7 || dif < 0)
		{
			System.out.println("Not a valid date range. Please input valid start and end date for a week");
			return;
		}
		
		//Retreiving borrowing activity for one week
		
		
		Statement stmt = conObj.conn.createStatement();
		try
		{
		String sqlSelectStatement = "SELECT BOOK.isbn as ISBN, BOOK.AUTHOR as AUTHOR, BOOK.SUBJECTAREA AS SUBJECTAREA, MEMBER.SSN AS SSN, borrowingactivity.checkedoutdate AS CHECKEDOUTDATE,borrowingactivity.returndate AS RETURNDATE,catalog.checkedoutcount AS CHECKEDOUTCOUNT " +
		                            " FROM BOOK, MEMBER, borrowingactivity, catalog " +
		                            " WHERE BOOK.isbn = borrowingactivity.bookisbn AND borrowingactivity.returndate is not null AND borrowingactivity.bookisbn = CATALOG.BOOKISBN " +
		                            " AND MEMBER.SSN =  borrowingactivity.memberssn " + "AND borrowingactivity.checkedoutdate BETWEEN ? AND ? " +
		                            "group by BOOK.subjectarea, BOOK.author, borrowingactivity.checkedoutdate, borrowingactivity.returndate, BOOK.isbn, catalog.checkedoutcount, member.ssn";
		
		PreparedStatement pstmt = conObj.conn.prepareStatement(sqlSelectStatement);
		
		pstmt.setDate(1,new java.sql.Date(firstDate.getTime()));
		pstmt.setDate(2, new java.sql.Date(lastDate.getTime()));
		
		 ResultSet rs = pstmt.executeQuery();
		 System.out.println("******  Weekly Borrowing Activity  ****** ");
		 System.out.println(" ");
		 String format = "%s\t| %s\t| %s\t| %s\t| %s\t| %s\t| %s";
		 
		 System.out.println(String.format(format, "isbn","author","subjectArea","ssn","checkoutDate","returnDate","checkoutCount"));
		 while(rs.next())
		 {
			 String isbn = rs.getString("ISBN");
			 String author = rs.getString("AUTHOR");
			 String subjectArea = rs.getString("SUBJECTAREA");
			 int ssn = Integer.parseInt(rs.getString("SSN"));
			 String checkoutDate = rs.getString("CHECKEDOUTDATE");
			 String returnDate = rs.getString("RETURNDATE");
			 String checkoutCount = rs.getString("CHECKEDOUTCOUNT");
			 
			 System.out.println(String.format(format, isbn,author,subjectArea,ssn,checkoutDate,returnDate,checkoutCount));
		 }
		}
		catch(SQLException ex)
		{
			ex.printStackTrace();
		}
	}
}


public class main {

	public static void main(String[] args) throws SQLException,IOException, ClassNotFoundException,ParseException{
		// TODO Auto-generated method stub
		JDBCConn jdbcObj = new JDBCConn("localhost","Development");
		boolean  result = jdbcObj.OpenConnection();
		
		jdbcObj.InsertRecordBook(jdbcObj);
		jdbcObj.InsertRecordMember(jdbcObj);
		jdbcObj.InsertRecordCatalog(jdbcObj);
		jdbcObj.InsertRecordBA(jdbcObj);
		
		Scanner reader = new Scanner(System.in);
		System.out.println("Please enter starting date of the week in the format MM//dd//yyyy");
		String fDate = reader.next();
		System.out.println("Please enter end date of the week in the format MM//dd//yyyy");
		String eDate = reader.next();
		jdbcObj.displayWeeklyReport(jdbcObj,fDate,eDate);
	}

}
