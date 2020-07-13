package helper.database;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.log4j.Logger;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com.event.decompress.EventXmlDeCompressUtil;

import helper.LoggerHelper;
import helper.Report;
import helper.application.Browser;
import helper.constants.Environment;
import helper.constants.GlobalConstants;
import main.java.helper.InitialSetup;
import main.java.helper.Record;
import main.java.security.SecureDatabaseConnection;

public class DatabaseHelper extends SecureDatabaseConnection{
	private static Logger				LOG						= LoggerHelper.getLogger(DatabaseHelper.class.getSimpleName());

	/**
	 * <p>
	 * This method can be used to select the data using "select" statement.<br>
	 * This method retrieves the data from back end to front end i.e. from database to a string.
	 * </p>
	 * <P>
	 * <B><U>Initialization of Variables.</U></B>
	 * <dd>
	 * <li><U>String environment</U>="UAT2";<br>
	 * <dd>
	 * <li><U>String query</U>="select * from NYHBEODB_929.ELG_MEMBER_PROGRAM_RESULT";<br>
	 * <dd>
	 * <li><U>DatabaseHelper.executeQuery( environment,query) ;<br>
	 * 
	 * @param sql
	 * @param getColumnNames
	 * 
	 * @return 2 dimension string array with all of the returned query data
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	@Test
	public static String[][] executeQuery(String sql,boolean getColumnNames) {
		if (LOG.isTraceEnabled())
			LOG.trace(" >> executeQuery()");
		
		Connection conn = null;
		ResultSet rset = null;
		PreparedStatement pstmt = null;
//		Statement stmt1 = null;
		String[][] $ = null;	
		if(GlobalConstants.server != null && GlobalConstants.server.equals(Environment.MAX)) {
			sql = sql.replaceAll("NYHBEODB_929", "NYHBEODB_102");
		}
		sql = sqlSchemaCorrection(sql);
		InitialSetup setting = null;
		try {
			setting = setupConnection(sql);
			conn = setting.getConnection();
			sql = setting.getSql();
			// Initialize the oracle driver and get connection
			pstmt = conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			LOG.debug(""+sql);
			rset = pstmt.executeQuery();
			ResultSetMetaData rsmd = rset.getMetaData();
			int noOfColumns = rsmd.getColumnCount();

			rset.last();
			int rowCount = rset.getRow();
			LOG.debug("row count is: " + rowCount + " || columns count is: " + noOfColumns);
			rset.beforeFirst();
			int count = 0;
			
			if (!getColumnNames)
				$ = new String[rowCount][noOfColumns];
			else {
				$ = new String[rowCount + 1][noOfColumns + 1];
				for (int e = 0; e < rsmd.getColumnCount(); ++e)
					$[0][e] = rsmd.getColumnName(e + 1);
				count = 1;
			}			
			
			for (; rset.next(); ++count)
				for (int e = 0; e < noOfColumns; ++e)
					$[count][e] = rset.getString(e + 1);
		} catch (SQLException e) {
			// handle any errors
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
			System.out.println(sql);
			e.printStackTrace();
			// System.exit(1);
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
			// System.exit(1);
		} finally {
			try {
				if(rset!=null)
					rset.close();
				if(pstmt!=null)
					pstmt.close();
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				System.exit(1);
			}
			closeConnections(setting);
		}

		if (LOG.isTraceEnabled())
			LOG.trace(" << executeQuery()");
		return $;
	}
	
	
	
	/**
	 * <p>
	 * This method can be used to select the data using "select" statement.<br>
	 * This method retrieves the data along with column Names from back end to front end i.e. from database to a string.
	 * </p>
	 * <P>
	 * <B><U>Initialization of Variables.</U></B>
	 * <dd>
	 * <li><U>String environment</U>="UAT2";<br>
	 * <dd>
	 * <li><U>String query</U>="select * from NYHBEODB_929.ELG_MEMBER_PROGRAM_RESULT";<br>
	 * <dd>
	 * <li><U>DatabaseHelper.executeQuery( environment,query) ;<br>
	 * 
	 * @param sql	
	 *  
	 * @return 2 dimension string array with all of the returned query data
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static String[][] executeQueryGetDataWithColumnName(String sql) {
		return executeQuery(sql, true);		
	}
	/**
	 * <p>
	 * This method can be used to select the data using "select" statement.<br>
	 * This method retrieves the data from back end to front end i.e. from database to a string.
	 * </p>
	 * <P>
	 * <B><U>Initialization of Variables.</U></B>
	 * <dd>
	 * <li><U>String environment</U>="UAT2";<br>
	 * <dd>
	 * <li><U>String query</U>="select * from NYHBEODB_929.ELG_MEMBER_PROGRAM_RESULT";<br>
	 * <dd>
	 * <li><U>DatabaseHelper.executeQuery( environment,query) ;<br>
	 * 
	 * @param sql	
	 * 
	 * @return 2 dimension string array with all of the returned query data
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static String[][] executeQuery(String sql) {
		return executeQuery(sql, false);		
	}

	/**  
	 * @param sql
	 * @return
	 */
	public static Record executeQueryAsRecords(String sql){
		if (LOG.isTraceEnabled())
			LOG.trace(" >> executeQueryAsRecords()");
		double init = System.currentTimeMillis();
		Record $ = new Record();
		Connection conn = null;
		String[][] outputData = null;
		PreparedStatement pstmt = null;
		ResultSet rset =null;
		if(GlobalConstants.server != null && GlobalConstants.server.equals(Environment.MAX)) {
			sql = sql.replaceAll("NYHBEODB_929", "NYHBEODB_102");
		}
		sql = sqlSchemaCorrection(sql);
		InitialSetup setting = null;
		try {
			setting = setupConnection(sql);
			conn = setting.getConnection();
			sql = setting.getSql();
			// Initialize the oracle driver and get connection
			pstmt = conn.prepareStatement(sql);
			LOG.debug("Connection: " + (System.currentTimeMillis() - init)/1000 + " seconds");
			LOG.debug("" + sql);
			rset = pstmt.executeQuery();
			for (ResultSetMetaData rsmd = rset.getMetaData(); rset.next();) {
				CaseInsensitiveMap<String, String> row = new CaseInsensitiveMap<String, String>();
				for (int e = 1; e <= rsmd.getColumnCount(); ++e){
					if(row.get(rsmd.getColumnLabel(e))!=null){
						continue;
					}
					//row.put(rsmd.getColumnLabel(e).toUpperCase(),
					//		!"REQUEST_DATA_XML".equalsIgnoreCase(rsmd.getColumnLabel(e)) ? rset.getString(e)
					//				: EventXmlDeCompressUtil.eventXmlDecompress(rset.getBytes("REQUEST_DATA_XML")));
					if(rsmd.getColumnType(e) == Types.BLOB && rset.getString(e) != null){
						row.put(rsmd.getColumnLabel(e).toUpperCase(), EventXmlDeCompressUtil.eventXmlDecompress(rset.getBytes(rsmd.getColumnLabel(e).toUpperCase())));
					}else{
						String columnvalue = null;
						if(rset.getString(e) != null) columnvalue = rset.getString(e).trim();
						row.put(rsmd.getColumnLabel(e).toUpperCase(), columnvalue);
					}
				}
				$.add(row);
			}
			LOG.debug("" + 
					"row count is: " + $.size() + " || columns count is: " + ($.isEmpty() ? 0 : $.getFirst().size()));
		} catch (SQLException e) {
			// handle any errors
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
			System.out.println(sql);
			LOG.fatal(e);
			e.printStackTrace();
			// System.exit(1);
		} catch (Exception e) {
			LOG.fatal(e);
			e.printStackTrace();
			// System.exit(1);
		} finally {
			try {
				rset.close();
				pstmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				System.exit(1);
			}
			closeConnections(setting);
		}
		LOG.debug((System.currentTimeMillis() - init)/1000 + " seconds");
		if (LOG.isTraceEnabled())
			LOG.trace(" << executeQueryAsRecords()");
		return $;
	}
	
	/**
	 * Execute an update or insert
	 * 
	 * @param sql
	 * 
	 * TODO: Create a validation that does a check if the insertion was complete
	 */
	public static void executeUpdate(String sql) {
		if (LOG.isTraceEnabled())
			LOG.trace(" >> executeUpdate()");
		
		Connection conn = null;
//		Statement stmt1 = null;
		PreparedStatement pstmt = null;
		sql = sqlSchemaCorrection(sql);
		InitialSetup setting = null;
		try {
			setting = setupConnection(sql);
			conn = setting.getConnection();
			conn.setAutoCommit(true);
			sql = setting.getSql();
			// Initialize the oracle driver and get connection
			pstmt =  conn.prepareStatement(sql);
//			stmt1 = conn.createStatement();
			LOG.debug("" + sql);
			pstmt.executeUpdate();
//			stmt1.executeUpdate(sql);
			
		} catch (SQLException e) {
			// handle any errors
			LOG.info("SQLException: " + e.getMessage());
			LOG.info("SQLState: " + e.getSQLState());
			LOG.info("VendorError: " + e.getErrorCode());
			LOG.info(sql);
			Report.update("FAIL::Update/Insert failed in database: " + sql + " : "+Browser.getExceptionString(e));
			// System.exit(1);
		} catch (Exception e) {
			LOG.error(e);
			e.printStackTrace();
			// System.exit(1);
		} finally {
			try {
//				stmt1.close();
				pstmt.close();
				conn.close();
			} catch (Exception e2) {
				e2.printStackTrace();
				System.exit(1);
			}
			closeConnections(setting);
		}

		if (LOG.isTraceEnabled())
		 LOG.trace(" << executeUpdate()");
	}

	

	
	public static String queryXML(String sql) {
		if (LOG.isTraceEnabled())
			LOG.trace(" >> queryXML()");

		Connection conn = null;
		ResultSet $ = null;
		PreparedStatement pstmt =  null;
		sql = sqlSchemaCorrection(sql);
		InitialSetup setting = null;
		try {
			setting = setupConnection(sql);
			conn = setting.getConnection();
			sql = setting.getSql();
			pstmt = conn.prepareStatement(sql);
			$ = pstmt.executeQuery();
			LOG.debug("" + sql);
			if ($.next())
				return convertInputStreamToString($.getBinaryStream(1));
		} catch (SQLException e) {
			e.printStackTrace();
			// System.exit(1);
		} catch (Exception e) {
			e.printStackTrace();
			// System.exit(1);
		} finally {
			try {
				$.close();
				pstmt.close();
				conn.close();
			} catch (Exception e2) {
				// TODO: handle exception
			}
			closeConnections(setting);
		}
		return new String();
	}

	
	/**
	 * Converts inputStream to String
	 * 
	 * @param data
	 * @return
	 * @throws Exception
	 */
	private static String convertInputStreamToString(InputStream data) throws Exception {
		ByteArrayOutputStream $ = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		for (int e = data.read(buf); e >= 0;)
			$.write(buf, 0, e);

		data.close();
		return String.valueOf($.toByteArray());
	}
	
	
	public static String sqlSchemaCorrection(String sql){
		String newSql = sql;
		if(sql.toUpperCase().contains("NYHBEODB_929")) {
			switch(Environment.valueOf(env.toString())) {
				case SBIT_AT:
					setDB2Username("eeapp1");
					setDB2Password("ee09876%");
					break;
				case PE:
				default:
					resetUserPass();
					break;
			}
		}
		if(sql.toUpperCase().contains("NYHBEODB_929")) {
			switch(Environment.valueOf(env.toString())) {
				case SBIT_AT:
				case PE:
					LOG.debug(" SQL Corrected for " + env.toString());
					newSql = sql.replace("NYHBEODB_929", "NYHBEODB");
					newSql = newSql.replace("nyhbeodb_929", "nyhbeodb");
					LOG.debug(""+newSql);
					break;
				default:
					break;
			}
		}
		
		if (sql.toUpperCase().split(" ")[0].contains("SELECT") && sql.toUpperCase().contains("LIMIT"))
			newSql = sql.replaceAll("(LIMIT|limit) (\\d{0,2})", "FETCH FIRST $2 ROWS ONLY");
		return newSql;
	}

	
}
