package DBRemote.SelectDB;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class demo implements RequestHandler<String, String> {
	private static String DB_URL = "jdbc:mariadb://crmviet.c09wjkmeasrs.ap-southeast-1.rds.amazonaws.com:3306/crmviet_sharding";
//	private static String DB_URL = "jdbc:mysql://localhost:3309/crmviet_sharding";
	private static String USER_NAME = "crmviet";
	private static String PASSWORD = "altalab123";

	public String handleRequest(String input, Context context) {
		Connection conn = null;
		try {
			conn = setConnection(DB_URL, USER_NAME, PASSWORD);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		switch (input) {
		case "select100DB":
			return selectDb(conn);
		case "callProcedure":
			return callProcedure(conn);
		case "task3":
			return task3CRUD(conn);
		case "Environment variables":
			return environmentVariables(conn);
		}
		return null;

	}

	public String environmentVariables(Connection conn) {
		String val=System.getenv("getENV");
		System.out.println(val);
		return "Get Environment Variables "+val;
	}

	private Connection setConnection(String url, String user, String pass) throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
//		Class.forName("com.mysql.jdbc.Driver");
		try {
			Connection connection = DriverManager.getConnection(url, user, pass);
			return connection;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String selectDb(Connection conn) {
		String content = "";
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT FIRST_NAME, LAST_NAME FROM crm_lead LIMIT 5;");
			int count = 0;
			while (rs.next()) {
				content += (count++) + " - " + rs.getString(1) + " - " + rs.getString(2) + "\n";
			}
			conn.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return content;
	}

	public String callProcedure(Connection conn) {

		try {
			CallableStatement demo = conn.prepareCall(
					"{CALL lead_count('d1cc9878-59d0-11ed-b6e4-0244d1d782a0','aafa1cfc-8a3d-11e8-ae72-028ff1f3e03a',NULL,NULL,NULL,NULL,'is',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)}");
			ResultSet rs = demo.executeQuery();
			while (rs.next()) {
				System.out.println("thành công");
			}
			conn.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return null;

	}

	public String task3CRUD(Connection conn) {

		return null;

	}
}
