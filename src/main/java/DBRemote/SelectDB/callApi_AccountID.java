package DBRemote.SelectDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class callApi_AccountID implements RequestHandler<String, String> {
	private static String DB_URL_AWS = "jdbc:mariadb://" + System.getenv("endPoint") + ":" + System.getenv("port")
			+ "/";
	private static String USER_NAME = System.getenv("user");
	private static String PASSWORD = System.getenv("pass");
	private static String DB_SYSTEM = System.getenv("db_system");
	public static final Map<String, Connection> connectionMap = new HashMap<>();
	public static Connection systemConnection;
	static {
		try {
			systemConnection = setConnection(DB_URL_AWS + DB_SYSTEM, USER_NAME, PASSWORD);
			String sql = "SELECT d.database, d.database_id FROM sys_databases d";
			Statement stmt = systemConnection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String dbName = rs.getString("database");
				String databaseId = rs.getString("database_id");
				String accountDBUrl = DB_URL_AWS + dbName;
				Connection accountConnection = setConnection(accountDBUrl, USER_NAME, PASSWORD);
				connectionMap.put(databaseId, accountConnection);
			}

		} catch (Exception e) {
			System.out.println("Exception handling set connection" + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public String handleRequest(String input, Context context) {
		try {
			systemConnection = setConnection(DB_URL_AWS + DB_SYSTEM, USER_NAME, PASSWORD);
			Statement stmt = systemConnection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT DATABASE_ID FROM sys_accounts WHERE ACCOUNT_ID='" + input + "';");
			if (rs.next()) {
				String databaseId = rs.getString(1);
				Connection accountConnection = connectionMap.get(databaseId);
				if (accountConnection != null && !accountConnection.isClosed()) {
					JSONObject input_Json=new JSONObject(System.getenv("input"));
					switch (System.getenv("method")) {
					case "create":
						return saveUser("create", input_Json, accountConnection);
					case "update":
						return saveUser("update",input_Json, accountConnection);
					case "delete":
						return deleteUser(input_Json, accountConnection);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Exception handling set connection" + e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	private static Connection setConnection(String url, String user, String pass) throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
		try {
			Connection connection = DriverManager.getConnection(url, user, pass);
			return connection;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	public String saveUser(String status, JSONObject input, Connection conn) {
		System.out.println(input.toString());
		String sql_String = "";
		String mess = "";
		try {
			if (status=="create") {
				sql_String = "INSERT INTO crm_quote (ACCOUNT_ID,OWNER_ID, QUOTE_DATE) VALUES ('"+input.getString("ACCOUNT_ID")+"', '"+input.getString("OWNER_ID")+"', '"+LocalDateTime.now()+"');";
				mess = "Create Success";
			} else if(status=="update") {
				sql_String = "UPDATE crm_quote SET OWNER_ID='"+input.getString("OWNER_ID")+"', QUOTE_DATE='"+ LocalDateTime.now()+"' WHERE QUOTE_ID ='"+ input.getString("QUOTE_ID")+"';";
				mess = "Update Success";
			}
			System.out.println("Query_SQL= " + sql_String);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql_String);
			System.out.println("Result: " + rs);
			System.out.println("Message: " + mess);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mess;
	}

	public String deleteUser(JSONObject input, Connection conn) {
		String sql_delete = "DELETE FROM crm_quote WHERE QUOTE_ID='" + input.get("QUOTE_ID") + "';";
		String mess = "";
		System.out.println("Query_SQL= " + sql_delete);
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql_delete);
			mess = "ERROR";
			if (rs.next()) {
				mess = "Delete Success";
				System.out.println("Result: " + rs.next());
				System.out.println("Message: " + mess);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mess;
	}

	public String getUser(JSONObject input, Connection conn) {
		String sql_getData = "select * from crm_quote where QUOTE_ID='" + input.get("QUOTE_ID") + "';";
		String mess = "";
		System.out.println("Query_SQL= " + sql_getData);
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql_getData);
			mess = "ERROR";
			if (rs.next()) {
				mess = "Get Success";
				System.out.println("Result: " + rs.next());
				System.out.println("Message: " + mess);
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mess;
	}

}
