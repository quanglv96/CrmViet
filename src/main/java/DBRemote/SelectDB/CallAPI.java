package DBRemote.SelectDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import org.json.JSONObject;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class CallAPI implements RequestHandler<String, String> {
	private static String DB_URL_AWS = "jdbc:mariadb://" + System.getenv("endPoint") + ":" + System.getenv("port") + "/"
			+ System.getenv("db_system");
//	private static String DB_URL_Client = "jdbc:mysql://localhost:3309/"+System.getenv("db_system");
	private static String USER_NAME = System.getenv("user");
	private static String PASSWORD = System.getenv("pass");

	public String handleRequest(String input, Context context) {
		Connection conn = null;
		System.out.println("input: " + input);
		try {
			conn = setConnection(DB_URL_AWS, USER_NAME, PASSWORD);
//			conn=setConnection(DB_URL_Client, USER_NAME, PASSWORD);
			JSONObject input_Json = new JSONObject(input);
			switch (System.getenv("method")) {
			case "create":
				return saveUser(input_Json, conn);
			case "update":
				return saveUser(input_Json, conn);
			case "delete":
				return deleteUser(input_Json, conn);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
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

	public String saveUser(JSONObject input, Connection conn) {
		System.out.println(input.toString());
		String sql_String = "";
		String mess = "";
		try {
			if (input.get("userId") == null) {
				sql_String = "INSERT INTO sys_users (USER_ID , USER_NAME, USER_EMAIL,PHONE,UPD_DTTM,CONFIRM) VALUES ("
						+ input.get("userName") + "," + input.get("userName") + "," + input.get("email") + ","
						+ input.get("phone") + "," + LocalDateTime.now() + ",0);";
				mess = "Create Success";
			} else {
				sql_String = "update sys_users set  USER_NAME='" + input.get("userName") + "', USER_EMAIL='"
						+ input.get("email") + "', PHONE='" + input.get("phone") + "', UPD_DTTM='" + LocalDateTime.now()
						+ "' where USER_ID='" + input.get("userId") + "';";
				mess = "Update Success";
			}
			System.out.println("Query_SQL= " + sql_String);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql_String);
			if (rs.next()) {
				mess = "Delete Success";
				System.out.println("Result: " + rs.next());
				System.out.println("Message: " + mess);
			} else {
				mess = "ERROR";
			}
			System.out.println("Result: " + rs);
			System.out.println("Message: " + mess);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mess;
	}

	public String deleteUser(JSONObject input, Connection conn) {
		String sql_delete = "DELETE FROM sys_users WHERE USER_ID='" + input.get("userId") + "';";
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
		String sql_getData = "select * from sys_users where USER_ID='" + input.get("userId") + "';";
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
