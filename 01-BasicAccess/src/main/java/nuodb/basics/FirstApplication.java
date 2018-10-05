package nuodb.basics;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.nuodb.jdbc.DataSource;

/**
 * Very simple first application to show a Java application using NuoDB.
 * Performs the following:
 * <ul>
 * <li>Creates a DataSource for a NuoDB database using the properties in
 * {@code default.propeerties} - modify this file if need be to suit your
 * database.
 * <li>Creates a connection using the DataSource
 * <li>Drops the test table (Accounts) if it exists already
 * <li>Creates the Accounts table
 * <li>Inserts a single row
 * <li>Selects and displays all the rows in the table
 * <li>Closes the connection.
 * </ul>
 * 
 * @author Paul Chapman
 */
public class FirstApplication implements SqlCommands {

	public static final String DEFAULT_PROPERTIES = "default.properties";

	/**
	 * The demo application.
	 * 
	 * @param args Command line arguments (unused).
	 */
	public static void main(String[] args) {

		Properties props = new Properties();

		try {
			// Load the default properties - assumes you started a database called
			// "testdb" with username and password "dba". Modify default.properties
			// for a different database,.
			props.load(new FileInputStream(new File(DEFAULT_PROPERTIES)));
			System.out.println("Propeprties: " + props);
		} catch (IOException e) {
			System.out.println("Unable to read " + DEFAULT_PROPERTIES + //
					" so unable to configure the DataSource - abort.");
		}

		try (DataSource dataSource = new DataSource(props); //
				Connection connection = dataSource.getConnection();) {
			// These SQL command strings are in SqlCommands
			modify(connection, DROP_TABLE);
			modify(connection, CREATE_TABLE);
			modify(connection, ADD_ACCOUNT);
			execute(connection, LIST_ACCOUNTS);
			System.out.println("Application finished");
		} catch (Exception oops) {
			System.out.println("Application failed");
			System.out.println();
			oops.printStackTrace();
		}
	}

	/**
	 * Perform a database modification - internally creates a prepared statement and
	 * uses {@link PreparedStatement#executeLargeUpdate()}.
	 * 
	 * @param connection Database connection.
	 * @param sql        The SQL to run - it should not be SELECT.
	 */
	protected static void modify(Connection connection, String sql) throws SQLException {
		System.out.println(sql);

		// Using try block so statement is automatically closed
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			int result = statement.executeUpdate();
			System.out.println("  Result: " + result);
		}
	}

	/**
	 * Perform a database modification - internally creates a prepared statement and
	 * uses {@link PreparedStatement#executeQuery()}.
	 * 
	 * @param connection Database connection.
	 * @param sql        The SQL to run - a SELECT is expected.
	 */
	protected static void execute(Connection connection, String sql) throws SQLException {
		System.out.println(sql);

		// Using try block so statement is automatically closed
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			ResultSet results = statement.executeQuery();
			while (results.next()) {
				System.out.println("Account " + results.getInt("Id") + " " + //
						results.getString("Number") + " owned by " + results.getString("Name") + //
						" " + results.getBigDecimal("Balance"));
			}

		}
	}
}
