package nuodb.basics;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Same application as FirstApplication but using Spring Boot to automatically
 * create the DataSource and a {@link JdbcTemplate}. The JdbcTemplate performs
 * all the SQL access, hiding all Connection management and reducing the
 * likelihood of resource leakage.
 * <ul>
 * <li>Spring Boot creates a DataSource for a NuoDB database using the
 * properties in {@code application.propeerties} - modify this file if need be
 * to suit your database.
 * <li>Spring Boot creates a JdbcTemplate also
 * <li>Application then uses the JdbcTemplate to:
 * <ul>
 * <li>Drops the test table (Accounts) if it exists already
 * <li>Creates the Accounts table
 * <li>Inserts a single row
 * <li>Selects and displays all the rows in the table
 * </ul>
 * </ul>
 * <p>
 * Spring's {@code JdbcTemplate} aggressively creates Connections when it needs
 * them and closes thme immediately. In this example 4 connections are created
 * and discarded.
 * <p>
 * A connection pool should always be used as it is far more efficient to use
 * existing connections from the pool than to create them on demand. This Spring
 * Boot application automatically uses an Hikari connection pool (the default
 * since Spring Boot V2).
 * 
 * @author Paul Chapman
 */
@SpringBootApplication
public class FirstApplicationWithSpring implements SqlCommands {
	public static void main(String[] args) {
		SpringApplication.run(FirstApplicationWithSpring.class, args);
	}

	public static final class SqlRunner implements CommandLineRunner {

		private JdbcTemplate jdbcTemplate;

		public SqlRunner(JdbcTemplate jdbcTemplate) {
			this.jdbcTemplate = jdbcTemplate;
		}

		@Override
		public void run(String... args) throws Exception {
			// These SQL command strings are in SqlCommands
			jdbcTemplate.update(DROP_TABLE);
			jdbcTemplate.update(CREATE_TABLE);
			jdbcTemplate.update(ADD_ACCOUNT);

			jdbcTemplate.query(LIST_ACCOUNTS, (results) -> {
				System.out.println("Account " + results.getInt("Id") + " " + //
				results.getString("Number") + " owned by " + results.getString("Name") + //
				" " + results.getBigDecimal("Balance"));
			});
			System.out.println("Application finished");
		}

	}

	@Bean
	public SqlRunner sqlRunner(JdbcTemplate jdbcTemplate) {
		return new SqlRunner(jdbcTemplate);
	}
}
