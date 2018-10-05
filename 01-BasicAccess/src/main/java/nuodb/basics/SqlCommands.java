package nuodb.basics;

public interface SqlCommands {
	public static final String DROP_TABLE = "DROP TABLE User.Accounts IF EXISTS";

	public static final String CREATE_TABLE = "CREATE TABLE User.Accounts\n" + //
			"(\n" + //
			"   Id       BIGINT not NULL generated always as identity primary key,\n" + //
			"   Number   CHAR(8),\n" + //
			"   Name     VARCHAR(20),\n" + //
			"   Balance  DECIMAL" + //
			")";

	public static final String ADD_ACCOUNT = "INSERT INTO User.Accounts (Number, Name, Balance) VALUES "
			+ " ('12345678', 'Leslie Smyth', 2000.00)";

	public static final String LIST_ACCOUNTS = "SELECT Id, Number, Name, Balance FROM User.Accounts";
}
