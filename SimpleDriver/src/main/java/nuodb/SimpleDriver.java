/* SimpleDriver.java */

package nuodb;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * nuodb.SimpleDriver.
 * Simple database load driver.
 * Usage: java nuodb.SimpleDriver [-option[=| ]value] [-option ...]
 *   <br>Options can be specified on the command-line or loaded from a file - see the -config option.
 *   <br>A command-line option can be in any of the forms: -option=value; -option value; or -option[=true];
 *   <br>where -option is one of the options below:
 * <ul>
 *   <li>-url         -> the JDBC connection URL - no default, required.
 *      <br>Example: jdbc:com.nuodb://localhost/testdb
 *   </li>
 *   <li>-user        -> the authentication USER for the database connection - no default, required
 *   </li>
 *   <li>-password    -> the password for the authentication USER - no default, required
 *   </li>
 *   <li>-threads     -> the number of SQL threads to run - default=10
 *   </li>
 *   <li>-time        -> the time in seconds to run the load - default=1
 *   </li>
 *   <li>-batch       -> the number of statements to batch into each commit - default=1
 *   </li>
 *   <li>-rate        -> the target rate of transactions per second - optional.
 *      <br>The workload will be adjusted to maintain the specified transaction rate.
 *      <br>The SQL task will sleep as necessary to match the current transaction rate to the specified target rate.
 *      <br>-rate helps answer the question: "Can this configuration support this load?"
 *   </li>
 *   <li>-load    -> the target database load factor percentage for SQL statements - default=95(%).
 *      <br>The workload includes a calculated wait-time to keep the database busy at the specified load percentage.
 *      <br>The SQL task will sleep for a time equal to (query-time) x (1.0-load).
 *      <br>-load helps answer the question: "What is the maximum load this configuration can support?"
 *   </li>
 *   <li>-report      -> time period in seconds to report statistics - default=1
 *   </li>
 *   <li>-config      -> path to a config file in Java Properties file format - optional.
 *      <br>An option set on the command-line overrides an option from file.
 *      <br>Option names in file do not have a '-' prefix, they can also be specified simply as name=value.
 *   </li>
 *   <li>-property    -> add a name/value pair to the set of properties - optional.
 *      <br>Format is name=value -or- name:value
 *      <br>Use this to set a property for which there is no command-line switch, but which is needed
 *      <br>  for the database properties, or variable resolution, etc.
 *      <br>Example: -property schema=User would cause "schema=User" to be passed to the Driver when connecting to the database;
 *      <br>and for some other property with a value such as: ${schema}.table, to be resolved as User.table.
 *   </li>
 *   <li>-logging     -> path to a Java Logging config file - optional.
 *      <br>See the Java documentation on the values for this file.
 *   </li>
 *   <li>-data        -> path to a data file to use for query parameter values - optional.
 *      <br>If the file has a .csv suffix, then it is parsed as CSV
 *      <br>otherwise it is parsed into space-separated words.
 *      <br>see "-params" below.
 *   </li>
 *   <li>-iterate     -> enable/disable iteration through all rows of each query - default=false.
 *   </li>
 *   <li>-iterate=false helps answer the question: "what is the throughput of the database?"
 *   </li>
 *   <li>-iterate=true helps answer the question: "what is the throughput of the application?"
 *   </li>
 *   <li>-sql         -> the SQL statement to run on the SQL thread(s) - default=SELECT * from User.Teams where year < ?{int,1910,2010}
 *   </li>
 *   <li>-params      -> Specification for the generation of values for parameter references in the SQL statement. optional
 *   </li>
 *   <li>-check       -> show the resolved values for options - default=false
 *   </li>
 *   <li>-help        -> show this help text and exit - default=false
 * </ul>
 *
 *   <br> Parameter specifications are separated by semicolons, and each is in the form
 *   <br> {type,format,X,Y,parseFormat} where:
 * <ul>
 *   <li>- type is one of [int, long, string, boolean, date, value];
 *   </li>
 *   <li>- format is a sprintf-style format specification, or can be omitted completely
 *   </li>
 *   <li>- (X and Y) define the range of generated values:
 * <ul>
 *   <li>- int, long, date: the first (X) and last (Y) in the value range;
 *   </li>
 *   <li>- string: shortest (X) and longest (Y) string length;
 *   </li>
 *   <li>- boolean - X is the relative percentage of <em>true</em> values (default=50);
 *   </li>
 *   <li>- value: X => first valid line (X=1 => skip first line); Y => column number;
 * </ul>
 *
 *   </li>
 *   <li>- parseFormat is a parse string specific to the parameter type for parsing string-values.
 *   </li>
 *   <li>- currently this only has an effect for 'date' type, where it is format string for a SimpleDateFormat.
 *<p></p>
 * Examples:
 * <ul>
 *   <li>-{int,1900,2011}
 *      <br>integer - generate a random value between 1900 and 2011;
 *   </li>
 *   <li>-{int,user-%d,100,999}
 *      <br>generate a random integer between 100 and 999, and return a <em>STRING</em> value formatted as "user-n";
 *   </li>
 *   <li>-{string,5,10}
 *      <br>string - generate a random string between 5 and 10 characters long;
 *   </li>
 *   <li>-{string,product-%s,5,10}
 *      <br>generate a random string between 5 and 10 characters long, then return it in the form "product-xyz"
 *   </li>
 *   <li>-{boolean}
 *      <br>generate a random boolean value, with values evenly spread between <em>true</em> and <em>false</em>
 *   </li>
 *   <li>-{boolean,30}
 *      <br>generate a random boolean value, which is <em>true</em> 30% of the time;
 *   </li>
 *   <li>-{date,%tc,1910/1/1,2011/12/31,yyyy/MM/dd}
 *      <br>generate a random date between Jan 1 1910 and Dec 31 2011, formatted with %tc.
 *   </li>
 *   <li>-{value,1,0}
 *      <br>value - retrieve a random line from the loaded data file; ignore the first line (X=1); and use the 0th column (Y=0) from that line;
 * </ul>
 *
 *<p></p>
 * Every '?' in the SQL is replaced by a value generated according to the corresponding format specifier.
 *<p></p>
 * Note also that if you need only change the log output format, then Java allows you to provide the
 *   <br>   "java.util.logging.SimpleFormatter.format" option in the Java options
 *   <br>    Ex: java -cp target:nuodbjdbc.jar -Djava.util.logging.SimpleFormatter.format="%4\$s: %5\$s%n" nuodb.SimpleDriver -url ...
 *<p></p>
 * See the documentation on java.util.logging.SimpleFormatter for details of the format string.
 */
public class SimpleDriver {

    private static final int DEFAULT_HISTORY_SIZE            = 10000;

    private static final String JAVA_LOG_FORMAT              = "java.util.logging.SimpleFormatter.format";
    private static final String JAVA_LOGFILE_SYSTEM_PROPERTY = "java.util.logging.config.file";

    /*  future feature
    private static final AtomicIntegerArray bucket = new AtomicIntegerArray(1);
    private static final int BURST_BUCKET           = 1;
    */

    private static final String className = SimpleDriver.class.getName();
    private static Logger log = Logger.getLogger(className);
    private static final String reportLoggerName = "nuodb.MonitorTask";

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private static final String DEFAULT_LOG_LEVEL       = "FINE";
    private static final String DEFAULT_REPORT_LEVEL    = "INFO";
    private static final String DEFAULT_LOG_FORMAT      = "%4$s: %1$tb:%1$td:%1$tH:%1$tM:%1$tS; %5$s%n";
    private static final String DEFAULT_LOG_CONFIG;

    static {
        DEFAULT_LOG_CONFIG =
                "handlers=java.util.logging.FileHandler, java.util.logging.ConsoleHandler\n"
                + String.format("java.util.logging.ConsoleHandler.level=%s\n", DEFAULT_LOG_LEVEL)
                + String.format("java.util.logging.SimpleFormatter.format=%s\n", DEFAULT_LOG_FORMAT)
                + String.format("%s.level=%s\n", className, DEFAULT_LOG_LEVEL)
                + String.format("%s.level=%s\n", reportLoggerName , DEFAULT_REPORT_LEVEL);
    }

    /**
     * Main program entry-point.
     *
     * @param args - a String array of the command-line arguments - see the Opt.xyz constants.
     */
    public static void main(String[] args) {
        ExecutorService executor = null;

        try {
            Properties props = configureApp(args);
            if (props == null)
                return;

            DataSource dataSource = new com.nuodb.jdbc.DataSource(props);
            executor = Executors.newCachedThreadPool();
            AtomicLongArray stats = new AtomicLongArray(11);

            int threadCount = Integer.parseInt(getOption(props, Opt.THREADS));

            CountDownLatch latch = new CountDownLatch(threadCount + 1);     // +1 for the monitor thread

            for (int index = 0; index < threadCount; index++) {
                executor.submit(new SqlTask(index, dataSource, latch, props, stats));
            }

            executor.submit(new MonitorTask(stats, latch, props));
        }
        catch (NullPointerException oops) {
            oops.printStackTrace();
            throw oops;
        }
        catch (Exception failure) {
            System.err.println("Fatal error - exiting: " + failure.toString());
        }
        finally {
            try { executor.shutdown(); }
            catch (Exception shutdownFailure) { /* ignore */ }
        }
    }

    /**
     * Logic for the SQL task - this is run on multiple threads - see {@link Opt#THREADS}.
     */
    private static class SqlTask implements Runnable {

	    private final int id;
        private final String sql;
        private final String verb;
        private final DataSource dataSource;
        private final CountDownLatch latch;
        private final AtomicLongArray stats;

        private final RingBuffer history;

        private final long threadCount;
        private final long timeout;
        private final int queryPerTx;
        private final long targetTxTime;
        private final long targetOpTime;
        private final float desaturation;
        private final boolean iterate;

        private static final String SELECT = "SELECT";
        private static final String INSERT = "INSERT";
        private static final String UPDATE = "UPDATE";
        private static final String DELETE = "DELETE";
        private static final String EXECUTE = "EXECUTE";

        private final List<ValueGenerator> param = new ArrayList<>(16);

        /**
         * The logic executed by this task.
         */
        @Override
        public void run() {

            try { latch.await(); }
            catch (InterruptedException interrupt) {}

	        int retry = 0;
            int teId = -1;

            stats.compareAndSet(STATS_START_TIME, 0, System.nanoTime());

            // loop until timeout expires
            while (System.currentTimeMillis() < timeout) {

                // keep a whole lot of stats
                int count = 0;
                long inactive = 0;
                long response = 0;
                long elapsed = 0;
                long start = 0;
                long time = 0;

                long begin = System.nanoTime();

                try (Connection conn = dataSource.getConnection()) {

                    teId = com.nuodb.jdbc.Connection.class.cast(conn).getConnectedNodeId();
                    conn.setAutoCommit(false);

                    try (PreparedStatement sql = conn.prepareStatement(this.sql, (verb.equals(INSERT) ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS))) {

                        for (int qx = 0; qx < queryPerTx; qx++) {

                            setParams(sql);

                            switch (verb) {

                                case SELECT:
                                {
                                    start = System.nanoTime();
                                    ResultSet rs = sql.executeQuery();
                                    response += System.nanoTime() - start;

                                    while (iterate && rs.next()) {
                                        count++;
                                    }
                                }
                                break;

                                case INSERT:
                                case UPDATE:
                                case DELETE:
                                    start = System.nanoTime();
                                    int rowCount = sql.executeUpdate();
                                    response += System.nanoTime() - start;
                                    break;

                                case EXECUTE:
                                    start = System.nanoTime();
                                    sql.execute();
                                    response += System.nanoTime() - start;
                                    break;

                                default:
                                    throw new RuntimeException("Unrecognised verb: " + verb);
                            }

                            time = System.nanoTime() - start;
                            elapsed += time;
                        }

                        // update the stats
                        stats.addAndGet(STATS_LATENCY_TIME, response);
                        stats.addAndGet(STATS_OPS_TIME, elapsed);
                        stats.incrementAndGet(STATS_TX_COUNT);
                        long current = stats.addAndGet(STATS_OPS_COUNT, queryPerTx);
                        stats.addAndGet(STATS_ROW_COUNT, count);

                        log.finer(String.format("Completed %d queries in task %d on TE %d in %.2fms; total=%d", queryPerTx, id, teId, NANO_TO_MILLIS * response, current));
                    }
                    catch (SQLTransactionRollbackException rolledBack) {
                        log.finer(String.format("Conflict executing sql >>%s<< on TE %d\n\t%s", sql, teId, rolledBack.toString()));
                        conn.rollback();
                        stats.incrementAndGet((rolledBack.toString().contains("deadlock") ? STATS_ABORT_DEADLOCK : STATS_ABORT_CONFLICT));
                        continue;
                    }

                    // commit the batch
                    start = System.nanoTime();
                    conn.commit();
                    long end = System.nanoTime();

                    long txTime = end - begin;
                    stats.addAndGet(STATS_TX_TIME, txTime);

                    history.add(begin, end);

                    log.finer(String.format("commit time=%.2f; txTime=%.2f; opTime=%.2f",
                            NANO_TO_MILLIS * (end - start), NANO_TO_MILLIS * txTime, NANO_TO_MILLIS * response));

                    // if -rate has been set, then we may have to sleep to adjust the transaction rate
                    if (targetTxTime > 0) {
                        log.finer(String.format("target tx=%.2f; txTime=%.2f; sleep=%.2f",
                                        NANO_TO_MILLIS * targetTxTime, NANO_TO_MILLIS * txTime, NANO_TO_MILLIS * (targetTxTime - txTime))
                        );

                        long sleepTime = history.getSleepTime(targetTxTime);
                        if (sleepTime > 0) {
                            inactive += sleep(sleepTime);
                        }
                    }
                    // otherwise if -saturate has been set, then sleep a time proportional to the statement response time
                    else if (desaturation > 0 && history.getCount() > 1) {
                        log.finer(String.format("desaturate - time=%.2f; sleep=%.2f", NANO_TO_MILLIS * txTime, NANO_TO_MILLIS * (response * desaturation)));
                        inactive += sleep((long) (response * desaturation));
                    }

                    stats.addAndGet(STATS_INACTIVE_TIME, inactive);
                    stats.set(STATS_END_TIME, end);
                }
                catch (SQLTransientConnectionException transientFailure) {
                    log.info(String.format("Communication ceased - TE %d no longer contactable. Automatically continuing with replacement TE (auto-failover)...\n\t%s", teId, transientFailure.toString()));
                }
                catch (SQLNonTransientConnectionException nonTransientFailure) {
                    log.info(String.format("Unable to establish initial connection - retrying...\n\t%s", nonTransientFailure.toString()));

		            retry++;
	  	            if (retry > 3) {
			            log.warning("Too many retries - exiting");
			            break;
		            }

		            try { Thread.sleep(300*retry); }
		            catch (InterruptedException interrupted) {}
                }
                catch (SQLException queryFailure) {
                    log.severe(String.format("Error committing sql >>%s<< on TE %d\n\t%s", sql, teId, queryFailure.toString()));
		            break;
                }
                catch (Exception nonSqlError) {
                    if (log.getLevel().intValue() >= Level.FINER.intValue()) {
                        nonSqlError.printStackTrace();
                    }

		            log.severe(String.format("Processing error\n\t%s", nonSqlError.toString()));
		            break;
		        }
            }
        }

        /**
         * Construct a new SqlTask
         *
         * @param id - unique numeric ID of this task
         * @param dataSource - the DataSource to use for database connections
         * @param latch - countdown latch to synchronise thread startup
         * @param props - run-time options
         */
        public SqlTask(int id, DataSource dataSource, CountDownLatch latch, Properties props, AtomicLongArray stats) {
            this.id = id;
            this.dataSource = dataSource;
            this.latch = latch;
            this.stats = stats;

            String sqlText = getOption(props, Opt.SQL);

            // get the parameter specifiers
            String paramOption = getOption(props, Opt.PARAMS);
            String[] paramList = (paramOption != null ? formatPattern.split(paramOption) : EMPTY_STRING_ARRAY);

            StringBuffer newSql = new StringBuffer(sqlText.length());

            // extract and remove the value specifiers from the SQL text
            Matcher matcher = paramPattern.matcher(sqlText);
            for (int paramIndex = 0; matcher.find(); paramIndex++) {
                String format = matcher.group(1);
                param.add(new ValueGenerator(paramIndex < paramList.length && paramList[paramIndex] != null ? paramList[paramIndex] : format));
                matcher.appendReplacement(newSql, "?");
            }
            matcher.appendTail(newSql);

            sql = (newSql.length() > 0 ? newSql.toString() : sqlText);

            log.finer(String.format("Query rewritten to: %s", sql));

            Matcher sqlVerb = verbPattern.matcher(sql);
            if (sqlVerb.find()) {
                verb = sqlVerb.group(1).toUpperCase();
            } else {
                throw new IllegalArgumentException(String.format("Could not parse VERB from sql %s", sql));
            }

            long duration = 1000 * Long.parseLong(getOption(props, Opt.TIME));
            timeout = System.currentTimeMillis() + duration;

            threadCount = Integer.parseInt(getOption(props, Opt.THREADS));

            queryPerTx = Integer.parseInt(getOption(props, Opt.BATCH));

            float txRate = Float.parseFloat(props.getProperty(Opt.RATE.toString(), "0"));

            targetTxTime = (long) (txRate > 0 ? ((NANOS_IN_SECOND * threadCount) / txRate) : 0);
            targetOpTime = (long) (txRate > 0 ? (targetTxTime / queryPerTx) : 0);
            log.finer(String.format("targetTxTime=%.2fms; targetOpTime=%.2f", NANO_TO_MILLIS * targetTxTime, NANO_TO_MILLIS * targetOpTime));

            int satPcnt = Integer.parseInt(getOption(props, Opt.LOAD));
            desaturation = (satPcnt > 0 && satPcnt < 100 ? (100.0f - satPcnt) / satPcnt : 0.0f);
            log.finer(String.format("saturation = %d%%; desaturation=%.2f", satPcnt, desaturation));

            iterate = (getOption(props, Opt.ITERATE).equalsIgnoreCase("true"));

            int maxHistory = (int) (txRate * threadCount * duration * MILLIS_TO_SECONDS);
            history = new RingBuffer(Math.max(maxHistory, DEFAULT_HISTORY_SIZE));

            latch.countDown();
        }

        /**
         * Set the SQL statement parameter values from the ValueGenerator(s)
         * @param sql the JDBC PreparedStatement object to set the values on.
         */
        private void setParams(PreparedStatement sql) {

            try {
                for (int paramNo = 1, max = param.size(); paramNo <= max; paramNo++) {

                    Object paramValue = param.get(paramNo - 1).getNextValue();
                    log.finest("next value=" + paramValue.toString());
                    Class type = paramValue.getClass();

                    if (type == Integer.class) {
                        sql.setInt(paramNo, (Integer) paramValue);
                    } else if (type == Long.class) {
                        sql.setLong(paramNo, (Long) paramValue);
                    } else if (type == String.class) {
                        sql.setString(paramNo, paramValue.toString());
                    } else if (type == Boolean.class) {
                        sql.setBoolean(paramNo, (Boolean) paramValue);
                    } else if (type == java.util.Date.class) {
                        sql.setDate(paramNo, new java.sql.Date(((java.util.Date) paramValue).getTime()));
                    } else {
                        sql.setObject(paramNo, paramValue);
                    }
                }
            } catch (SQLException jdbcError) {
                throw new RuntimeException(String.format("Error setting sql param: %s", jdbcError.toString()), jdbcError);
            }
        }
    }

    /**
     * Logic for the stats monitor thread.
     */
    private static class MonitorTask implements Runnable {

        private final AtomicLongArray stats;
        private final CountDownLatch latch;

        private final long reportPeriod;
        private final long timeout;
        private final int threadCount;

        private static Logger report = Logger.getLogger("nuodb.MonitorTask");

        @Override
        public void run() {

            try { latch.await(); }
            catch (InterruptedException interrupt) {}

            log.fine("monitor starting");

            while (System.currentTimeMillis() < timeout) {
                try { Thread.sleep(reportPeriod); }
                catch (InterruptedException interrupt) {
                    log.info("Run complete...");
                }

                long start = stats.get(STATS_START_TIME);
                long end = stats.get(STATS_END_TIME);
                long txCount = stats.get(STATS_TX_COUNT);
                long opCount = stats.get(STATS_OPS_COUNT);
                long rowCount = stats.get(STATS_ROW_COUNT);
                long latencyTime = stats.get(STATS_LATENCY_TIME);
                long txTime = stats.get(STATS_TX_TIME);

                long totalTime = end - start;
                double ops = (1.0d * NANOS_IN_SECOND * opCount) / totalTime;
                long aveLatency = latencyTime / opCount;
                long aveTxTime = txTime / txCount;

                report.info(String.format("   work=%.2fop/s; time=%.2f; ave latency=%.2fms; ave tx=%.2fms",
                        ops, NANO_TO_MILLIS * totalTime, NANO_TO_MILLIS * aveLatency, NANO_TO_MILLIS * aveTxTime));
            }

            long start = stats.get(STATS_START_TIME);
            long end = stats.get(STATS_END_TIME);
            long txCount = stats.get(STATS_TX_COUNT);
            long opCount = stats.get(STATS_OPS_COUNT);
            long rowCount = stats.get(STATS_ROW_COUNT);
            long inactive = stats.get(STATS_INACTIVE_TIME);

            long totalTime = end - start;
            double ops = (1.0d * NANOS_IN_SECOND * opCount) / (end - start);
            double aveLatency = stats.get(STATS_LATENCY_TIME) / opCount;
            double aveTx = stats.get(STATS_TX_TIME) / txCount;

            long conflict = stats.get(STATS_ABORT_CONFLICT);
            long deadlock = stats.get(STATS_ABORT_DEADLOCK);

            report.info(String.format("Total statements=%,d; elapsed=%.2fms (sleep=%.3fms); rows=%,d; rate=%.2fop/s; ave latency=%.2fms; ave tx=%.2fms;",
                    opCount, NANO_TO_MILLIS * totalTime, NANO_TO_MILLIS * (inactive / threadCount), rowCount, ops, NANO_TO_MILLIS * aveLatency, NANO_TO_MILLIS * aveTx));

            if (conflict > 0 || deadlock > 0) {
                report.info(String.format("* Total Rollbacks=%d; Deadlock=%d; other=%d", deadlock + conflict, deadlock, conflict));
            }
        }

        /**
         * Create a new Monitor task - the Monitor task is run on a single background thread.
         *
         * @param stats - an AtomicArray of stats values
         * @param latch - a countdown latch to synchronise thread startup
         * @param props - the properties containing all configured options.
         */
        public MonitorTask(AtomicLongArray stats, CountDownLatch latch, Properties props) {

            this.stats = stats;
            this.latch = latch;

            reportPeriod = 1000 * Integer.parseInt(getOption(props, Opt.REPORT));

            long duration = 1000 * Integer.parseInt(getOption(props, Opt.TIME));
            timeout = System.currentTimeMillis() + duration + 100;

            threadCount = Integer.parseInt(getOption(props, Opt.THREADS));

            latch.countDown();
        }
    }

    private static Properties configureApp(String[] args) {

        Properties props = parseCommandLine(args);

        String optsFile = props.getProperty(Opt.CONFIG.toString());
        if (optsFile != null && optsFile.length() > 0) {
            props = mergePropertiesFile(props, optsFile);
        }

        resolveVariables(props);

        configureLogging(props);

        if (getOption(props, Opt.CHECK).equals("true")) {
            System.out.println(String.format("Properties set: %s", props));
        }

        String helpOption = getOption(props, Opt.HELP);
        if (!helpOption.equals("false")) {
            if (helpOption.startsWith("javadoc:")) {
                writeJavadoc(helpOption.substring("javadoc:".length()));
            } else {
                System.out.println(getHelp());
            }

            return null;
        }

        validateOptions(props);

        String dataFile = getOption(props, Opt.DATA);
        if (dataFile != null) {
            ValueGenerator.loadDataFile(dataFile);
        }

        return props;
    }

    /**
     * Parse options from the command-line, and return them in a Properties object.
     *
     * @param args String array of command-line parameters
     * @return a Properties object containing all parsed options
     */
    private static Properties parseCommandLine(String[] args) {

        // create an empty Properties object
        Properties props = new Properties();

        // iterate the command arguments
        Matcher compoundArg;
        String name = null;
        String value;

        for (String arg : args) {

            // arg begins with '-', so it's an option name
            if (arg.charAt(0) == '-') {
                if (name != null) {
                    props.setProperty(name, "true");
                }

                if (arg.charAt(0) == '-')
                    arg = arg.substring(1);

                compoundArg = propertyPattern.matcher(arg);
                if (compoundArg.find()) {
                    name = compoundArg.group(1);
                    value = compoundArg.group(2);
                }
                else {
                    name = arg;
                    value = null;
                }

                try { Opt.valueOf(name.trim().toUpperCase()); }
                catch (Exception invalidOption) {
                    throw new IllegalArgumentException(String.format("Invalid option: %s%n%s", name, invalidOption.toString()));
                }

                if (value != null) {

                    if (name.equals(Opt.PROPERTY.toString())) {
                        compoundArg = propertyPattern.matcher(value);

                        name = compoundArg.group(1);
                        value = compoundArg.group(2);

                        if (value == null || value.length() == 0) {
                            value = "true";
                        }
                    }

                    props.setProperty(name, value);
                    name = null;
                }
            }

            // arg comes immediately after an option name
            else if (name != null) {

                value = arg;

                if (name.equals(Opt.PROPERTY.toString())) {
                    compoundArg = propertyPattern.matcher(value);

                    if (compoundArg.find()) {
                        name = compoundArg.group(1);
                        value = compoundArg.group(2);

                        if (value == null || value.length() == 0) {
                            value = "true";
                        }
                    }
                }

                props.setProperty(name, value);
		        name = null;
            }

            // no preceding name - error
            else {
                throw new IllegalArgumentException(String.format("Option value with no name: %s", arg));
            }
        }

        // last option
        if (name != null) {
            props.setProperty(name, "true");
        }

        return props;
    }

    /**
     * Read properties from a file, and merge them into the existing Properties object.
     *
     * Properties in the existing Properties override those read from file.
     *
     * @param props
     * @param filePath
     * @return the merged Properties object
     * @throws IOException - if the <code>filePath</code> does not refer to a valid file.
     */
    private static Properties mergePropertiesFile(Properties props, String filePath) {
        try {
            Reader file = new FileReader(filePath);

            Properties newProps = new Properties();
            newProps.load(file);

            // existing options override options read from file
            newProps.putAll(props);

            return newProps;
        }
        catch (IOException fileError) {
            throw new IllegalArgumentException(String.format("Error reading properties file: %s", fileError), fileError);
        }
    }

    /**
     * Resolve variable references within property values.
     * So, given the following Properties:
     * <pre>
     *     prop1=value1
     *     prop2=${prop1}-value2
     * </pre>
     *
     * After variables have been resolved, the Properties would then be:
     * <pre>
     *     prop1=value1
     *     prop2=value1-value2
     * </pre>
     *
     * @param props
     */
    private static void resolveVariables(Properties props) {

        StringBuffer newValue = new StringBuffer(1024);
        for (Map.Entry<Object, Object> entry : props.entrySet()) {

            String value = entry.getValue().toString();
            newValue.setLength(0);

            Matcher match = variableReferencePattern.matcher(value);
            while (match.find()) {
                String resolve = props.getProperty(match.group(1));

                if (resolve != null) {
                    match.appendReplacement(newValue, resolve);
                }
            }

            if (newValue.length() > 0) {
                match.appendTail(newValue);
                entry.setValue(newValue.toString());
            }
        }
    }

    /**
     * Ensure the specified options are sane.
     *
     * @param props
     */
    private static void validateOptions(Properties props) {

        if (getOption(props, Opt.URL) == null || getOption(props, Opt.USER) == null || getOption(props, Opt.PASSWORD) == null) {
            throw new IllegalArgumentException("Missing command-line option(s). You must specify all of: -url URL; -user USER; -password password");
        }

        int rate = -1;
        if (props.containsKey(Opt.RATE)) {
            rate = Integer.parseInt(props.getProperty(Opt.RATE.toString()));
            if (rate <= 0)
                throw new IllegalArgumentException("Value for -rate muat be > 0");
        }

        int threads = Integer.parseInt(getOption(props, Opt.THREADS));
        int time = Integer.parseInt(getOption(props, Opt.TIME));

        // we need at least 2 operations per thread
        if (rate > 0 && rate * time < 2 * threads)
            throw new IllegalArgumentException(String.format("Invalid RATE setting. Projected rate < 2 operations per thread: total ops=%d; threads=%d",
                    rate * time, threads));

        int saturation = -1;
        if (props.containsKey(Opt.LOAD.toString())) {
            saturation = Integer.parseInt(props.getProperty(Opt.LOAD.toString()));
            if (saturation <= 0 || saturation > 100)
                throw new IllegalArgumentException("Value for -saturation must be between 1 and 100 inclusive");
        }
        else if (rate <= 0) {
            log.info(String.format("Saturation defaulting to %s%%", Opt.LOAD.defaultValue));
        }

        if (rate > 0 && saturation > 0) {
            log.warning("Both -rate and -saturation set - only rate will have an effect, and saturation will be ignored");
        }
    }

    /** return the value of the specified option, or its configured default value if the option is not in props */
    private static String getOption(Properties props, Opt option) {
        return props.getProperty(option.toString(), option.defaultValue);
    }

    /**
     * perform the thread sleep.
     *
     * @param sleepNanos - the time, in nanoseconds, to sleep.
     * @return the time, in nanoseconds, actually slept
     */
    private static long sleep(long sleepNanos) {
        log.finer(String.format("sleep - time=%.4f", NANO_TO_MILLIS * sleepNanos));
        try {
            Thread.sleep((long) (sleepNanos / NANOS_IN_MILLI), (int) (sleepNanos % NANOS_IN_MILLI));
        }
        catch (InterruptedException interrupt) {}

        return sleepNanos;
    }

    /**
     * Configure the logging using either the command-line option (see {@link Opt#LOGGING}), or the
     * Java system property (see {@link #JAVA_LOGFILE_SYSTEM_PROPERTY}).
     *
     * @param props
     */
    private static void configureLogging(Properties props) {

        // delay reporting errors until the logger has been configured
        List<String> errors = new ArrayList<>(8);

        // see if the formatter has been set
        String logFormat = System.getProperty(JAVA_LOG_FORMAT);
        if (logFormat != null) {
            try { String.format(logFormat, new Date(), "caller", "test.Logger", Level.INFO, "Test message", "CauseException"); }
            catch (Exception formatError) {
                throw new IllegalArgumentException(String.format("Invalid Logger format string: %s", logFormat), formatError);
            }
        }

        // see if a java logging config file property was set on the command-line
        String logConfig = System.getProperty(JAVA_LOGFILE_SYSTEM_PROPERTY);

        // if a logging config file has not been set, then configure from the default config file
        if (logConfig == null || logConfig.length() == 0) {
            logConfig = getOption(props, Opt.LOGGING);

            InputStream configProperties = null;

            if (logConfig != null) {
                try { configProperties = new FileInputStream(logConfig);}
                catch (IOException fileError) {
                    errors.add(String.format("Logging config file %s specified - but could not be read\n\t%s",
                            logConfig, fileError)
                    );
                }
            }

            if (configProperties == null) {
                try { configProperties = new ByteArrayInputStream(DEFAULT_LOG_CONFIG.getBytes("UTF8")); }
                catch (Exception encodingError) {
                    errors.add(String.format("Internal error with default logging config:\n\t%s", encodingError));
                }
            }

            if (configProperties != null) {
                try { LogManager.getLogManager().readConfiguration(configProperties); }
                catch (Exception readError) {
                    errors.add(String.format("Unable to load logging config %s; logging config left at Java default:\n\t",
                            logConfig, readError)
                    );
                }
            }
        }

        log = Logger.getLogger(className);

        for (String err : errors) {
            log.warning(err);
        }
    }

    /**
     * RingBuffer - a sliding window on average times.
     * Used to calculate the correct sleep time to maintain a target elapsed time.
     */
    private static class RingBuffer {

        private final long[][] ring;
        private int first = 0;
        private int last = 0;
        private int size = 0;

        private static final int START  = 0;
        private static final int END    = 1;

        RingBuffer(int size) {
            ring = new long[size][2];
        }

        public void add(long start, long end) {

            if (size > 0) {
                last = (last + 1) % ring.length;
            }

            ring[last][START] = start;
            ring[last][END] = end;

            if (size > 0 && last == first) {
                first++;
            }
            else {
                size++;
            }
        }

        /**
         * Calculate the sleep time required to adjust to the target time.
         *
         * @param targetTime - the target time, in nanoseconds.
         * @return the sleep time that would adjust the current rate to match the target rate.
         */
        public long getSleepTime(long targetTime) {

            if (size > 1) {
                long time = ring[last][END] - ring[first][START];
                log.finer(String.format("start=%d; end=%d; count=%d", ring[first][START], ring[last][END], size));
                long avgTime = time / size;

                log.finer(String.format("history count=%d; Time=%.2f; avg = %.2f; target=%.2f",
                        size, NANO_TO_MILLIS * time, NANO_TO_MILLIS * avgTime, NANO_TO_MILLIS * targetTime));

                if (avgTime < targetTime) {
                    log.finer("tx sleep");
                    return size * (targetTime - avgTime);
                }
            }

            return 0;
        }

        public int getCount() {
            return size;
        }
    }

    /** Generator of random parameter values */
    private static class ValueGenerator {

        private final String type;
        private final String format;
        private final long first;
        private final long second;
        private final long delta;
        private final Object parser;

        private static final List<String[]> data = new ArrayList<>(1024);

        private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy/MM/dd");
        private static final DateFormat dateTimeFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        private static final Random randomGenerator = new Random();
        private static final String[] EMPTY_STRING_ARRAY = new String[0];

        private static final Pattern argPattern = Pattern.compile(" *, *");
        private static final Pattern doubleQuote = Pattern.compile("\"\"");
        private static final Pattern wordPattern = Pattern.compile(" +");

        /**
         * return the next random value for this ValueGenerator
         *
         * @return the next generated value.
         */
        private Object getNextValue() {

            double rand = randomGenerator.nextDouble();

            Object value = null;

            switch (type) {
                case "int":
                    value = (Integer) ((int) first + (int) (rand * delta));
                    return (format != null ? String.format(format, value) : value);

                case "long":
                    value = (Long) (first + (long) (rand * delta));
                    return (format != null ? String.format(format, value) : value);

                case "string":
                    value = stringValue((int) first + (int) (rand * delta));
                    return (format != null ? String.format(format, value) : value);

                case "boolean":
                    value = (rand * 100 < first);
                    return (format != null ? String.format(format, value) : value);

                case "date":
                    value = new Date(first + (long) (rand * delta));
                    return (format != null ? String.format(format, value) : value);

                case "value":
                    int lineNo = (int) (first + (rand * delta));
                    value = data.get(lineNo)[(int) second];
                    return (format != null ? String.format(format, value) : value);

                default:
                    throw new RuntimeException(String.format("Invalid parameter type: %s", type));
            }
        }

        /**
         * Construct a ValueGenerator from the specified String specifier
         * @param specifier - string representation of the value parameters.
         */
        public ValueGenerator(String specifier) {
            if (specifier.charAt(0) == '{') {
                specifier = specifier.substring(1, specifier.length() - 1);
            }

            String[] args = argPattern.split(specifier);

            int maxArg = args.length;
            int argNo = 0;

            type = args[0];
            argNo++;

            format = (maxArg > argNo && !Character.isDigit(args[argNo].charAt(0)) ? args[argNo] : null);
            if (format != null) argNo++;

            String firstValue = (maxArg > argNo ? args[argNo] : null);
            argNo++;

            String secondValue = (maxArg > argNo ? args[argNo] : null);
            argNo++;

            String parseFormat = (maxArg > argNo ? args[argNo] : null);

            switch (type.toLowerCase()) {
                case "int":
                    first = (firstValue != null ? Integer.parseInt(firstValue) : 0);
                    second = (secondValue != null ? Integer.parseInt(secondValue) : Integer.MAX_VALUE);
                    delta = second - first;
                    parser = null;
                    break;

                case "long":
                    first = (firstValue != null ? Long.parseLong(firstValue) : 0);
                    second = (secondValue != null ? Long.parseLong(secondValue) : Long.MAX_VALUE);
                    delta = second - first;
                    parser = null;
                    break;

                case "string":
                    first = (firstValue != null ? Long.parseLong(firstValue) : 5);
                    second = (secondValue != null ? Long.parseLong(secondValue) : first);
                    delta = second - first;
                    parser = null;
                    break;

                case "boolean":
                    first = (firstValue != null ? Long.parseLong(firstValue) : 5);
                    second = 0;
                    delta = 0;
                    parser = null;
                    break;

                case "date":
                    parser = (parseFormat != null ? new SimpleDateFormat(parseFormat) : null);
                    first = (firstValue != null ? dateValue(firstValue, (DateFormat) parser).getTime() : new java.util.Date().getTime());
                    second = (secondValue != null ? dateValue(secondValue, (DateFormat) parser).getTime() : first);
                    delta = second - first;
                    break;

                case "value":
                    if (data.size() == 0)
                        throw new IllegalArgumentException(String.format("A parameter specifier refers to the DATA file, but there has been no data read from file: %s", specifier));

                    first = (firstValue != null ? Integer.parseInt(firstValue) : 0);
                    second = (secondValue != null ? Integer.parseInt(secondValue) : 0);
                    delta = data.size()- first;
                    parser = null;
                    break;

                default:
                    throw new IllegalArgumentException(String.format("Unsupported parameter type: %s", type));
            }

            log.finer(String.format("ValueGenerator: first=%d; second=%d; delta=%d", first, second, delta));
        }

        /**
         * Load the named file for all ValueGenerators to access.
         * @param fileName the path to the file to load.
         */
        public static void loadDataFile(String fileName) {
            try {
                if (fileName.length() < 5) {
                    throw new IllegalArgumentException(String.format("\"file\" name does not have a valid suffix: %s", fileName));
                }

                String suffix = fileName.substring(fileName.lastIndexOf('.')).toLowerCase();

                LineNumberReader in = new LineNumberReader(new FileReader(fileName));

                for (String line = in.readLine(); line != null && line.length() > 0; line = in.readLine()) {

                    if (suffix.startsWith(".csv")) {
                        data.add(parseCSV(line));
                    } else {
                        data.add(wordPattern.split(line));
                    }
                }

                log.fine(String.format("Read %d lines from file %s", data.size(), fileName));
            } catch (IOException ioError) {
                throw new RuntimeException(String.format("Error reading values file %s", fileName), ioError);
            }
        }

        /**
         * Simple CSV parser.
         *
         * Handles the CSV quoting rules.
         *
         * @param line - String of a single line from the CSV file.
         * @return an array of String objects - 1 per column in the parsed CSV line
         */
        private static String[] parseCSV(String line) {

            List<String> list = new ArrayList<>(64);

            int start = 0;
            boolean quoted = false;

            for (int cx = 0, max = line.length()-1; cx <= max; cx++) {

                char c = line.charAt(cx);
                if (c == '"') {
                    quoted = !quoted;
                }

                else if ((c == ',' && quoted == false) || cx == max) {
                    if (!quoted) {
                        String val = (cx < max ? line.substring(start, cx) : line.substring(start)).trim();
                        start = cx+1;

                        // strip enclosing quotes
                        if (val.startsWith("\"") && val.endsWith("\"")) {
                            val = val.substring(1, val.length() - 1);
                            val = doubleQuote.matcher(val).replaceAll("\"");
                        }

                        list.add(val);
                    }
                }
            }

            return list.toArray(EMPTY_STRING_ARRAY);
        }

        /**
         * Generate a random String of the specified length.
         * @param length - the length of the generated string.
         * @return the newly created String value
         */
        private static String stringValue(int length) {
            StringBuilder builder = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                builder.append(printable[randomGenerator.nextInt(printable.length)]);
            }

            return builder.toString();
        }

        /**
         * Generate a Date value using a DateFormat object as the parser.
         *
         * @param date - string representation of the date.
         * @param parseFormat a DateFormat (usually a SimpleDateFormat) object to parse the date String into a Date object.
         * @return a newly created Date object.
         */
        private static Date dateValue(String date, DateFormat parseFormat) {
            DateFormat parser = (parseFormat != null ? parseFormat
                    : (date.contains(" ") ? dateTimeFormatter : dateFormatter)
            );
            try { return parser.parse(date); }
            catch (Exception formatError) {
                throw new IllegalArgumentException(String.format("Invalid Date string: %s", date), formatError);
            }
        }

        /** The set of chars to use when generating a random string */
        private static char[] printable = new char[] {
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'
        };
    }

    /**
     * generate the HELP text.
     * @return the help text as a String.
     */
    private static String getHelp() {
        String spaces = "          ";

        StringBuilder optHelp = new StringBuilder(1024);
        optHelp.append("Usage: java ").append(className).append(" [-option[=| ]value] [-option ...]");
        optHelp.append("\n  Options can be specified on the command-line or loaded from a file - see the -config option.");
        optHelp.append("\n  A command-line option can be in any of the forms: -option=value; -option value; or -option[=true];");
        optHelp.append("\n  where -option is one of the options below:");

        for (Opt opt : Opt.values()) {
            String name = opt.toString().toLowerCase();
            optHelp.append("\n   -").append(name).append(spaces.substring(0, 12 - name.length())).append("-> ");
            optHelp.append(String.format(opt.description.replaceAll("\n", "\n       "), opt.defaultValue != null ? "default=" + opt.defaultValue : "optional"));
        }

        optHelp.append("\n\n").append(HELP_EXPLANATION);

        return optHelp.toString();
    }

    /** Generate Javadoc format of the HELP text, and write it to the named file */
    private static void writeJavadoc(String path) {
        String text = getHelp();

        text = String.format("/**%n * %s.\n * Simple database load driver.%n * %s%n */%n", className, text);
        text = text.replaceAll(":\n {3,}-", ":\n * <ul>\n *   <li>-");
        text = text.replaceAll("\n {3,}-", "\n *   </li>\n *   <li>-");
        text = text.replaceAll("\n       ", "\n *      <br>");
        text = text.replaceAll("\n  ", "\n *   <br>");
        text = text.replaceAll("\n \n", "\n *<p></p>\n * ");
        text = text.replace("\n\n", "\n * </ul>\n *\n");
        text = text.replaceAll(" (true|false)[ .]", " <em>$1</em> ");

        try (Writer output = new FileWriter(path)) {
            output.write(text);
            output.flush();
        } catch (IOException fileError) {
            System.err.println(String.format("Could not write JavaDoc to file: %s", path));
        }
    }

    /** The command-line options strings recognised, with their defaults and simple description. */
    private enum Opt {
        URL(null, "the JDBC connection URL - no default, required."
                + "\nExample: jdbc:com.nuodb://localhost/testdb"
        ),
        USER(null, "the authentication USER for the database connection - no default, required"),
        PASSWORD(null, "the password for the authentication USER - no default, required"),
        THREADS("10", "the number of SQL threads to run - %s"),
        TIME("1", "the time in seconds to run the workload - %s"),
        BATCH("1", "the number of statements to batch into each commit - %s"),
        RATE(null, "the target rate of transactions per second - %s."
                + "\nThe workload will be adjusted to maintain the specified transaction rate."
                + "\nThe SQL task will sleep as necessary to match the current transaction rate to the specified target rate."
                + "\n-rate helps answer the question: \"Can this configuration support this load?\""
        ),
        LOAD("95", "the target percentage database load from the SQL statements - %s."
                + "\nThe workload includes a calculated wait-time to keep the database busy at the specified load percentage."
                + "\nThe SQL task will sleep for a time equal to (query-time) x (1.0-load)."
                + "\n-load helps answer the question: \"What is the maximum load this configuration can support?\""
        ),
        REPORT("1", "time period in seconds to report statistics - %s"),
        CONFIG(null, "path to a config file in Java Properties file format - %s."
                + "\nAn option set on the command-line overrides an option from file."
                + "\nOption names in file do not have a '-' prefix, they are specified simply as name=value."
        ),
        PROPERTY(null, "add a name/value pair to the set of properties - %s.\nFormat is name=value or name:value"
                + "\nUse this to set a property for which there is no command-line switch, but which is needed"
                + "\n  for the database properties, or variable resolution, etc."
                + "\nExample: -property schema=User would cause \"schema=User\" to be included in the properties passed to the database Driver;"
                + "\n  and for some other property with a value such as: ${schema}.table, to be resolved to User.table."
        ),
        LOGGING(null, "path to a Java Logging config file - %s.\nSee the Java documentation on the values for this file."),
        DATA(null, "path to a data file to use for query parameter values - %s."
                + "\nIf the file has a .csv suffix, then it is parsed as CSV"
                + "\notherwise it is parsed into space-separated words."
                + "\nsee \"-params\" below."
        ),
        ITERATE("false", "enable/disable iteration through all rows of each query - %s."
                + "\n-iterate=false helps answer the question: \"what is the throughput of the database?\""
                + "\n-iterate=true helps answer the question: \"what is the throughput of the application?\""
        ),
        SQL("SELECT * from User.Teams where year < ?{int,1910,2010}", "the SQL statement to run on the SQL thread(s) - %s"
                + "\nNote that other statements such as INSERT, UPDATE, DELETE and EXECUTE are also supported."
        ),
        PARAMS(null, "Specification for the generation of values for parameter references in the SQL statement. %s"),
        CHECK("false", "show the resolved values for options - %s"),
        HELP("false", "show this help text and exit - %s");

        private String defaultValue;
        private String description;

        Opt(String defaultValue, String description) {
            this.defaultValue = defaultValue;
            this.description = description;
        }

        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    /** The static text of the HELP explanation */
    private static final String HELP_EXPLANATION =
            "   Parameter specifications are separated by semicolons, and each is in the form\n"
                    + "   {type,format,X,Y,parseFormat} where:\n"
                    + "      - type is one of [int, long, string, boolean, date, value];\n"
                    + "      - format is a sprintf-style format specification; or can be omitted completely\n"
                    + "      - (X and Y) define the range of generated values:\n"
                    + "         - int, long, date: the first (X) and last (Y) in the value range;\n"
                    + "         - string: shortest and longest string length;\n"
                    + "         - boolean - X is the relative percentage of true values (default=50);\n"
                    + "         - value: X => first valid line (X=1 => skip first line); Y => column number;\n\n"
                    + "      - parseFormat is a parse string specific to the parameter type for parsing string-values.\n"
                    + "         - currently this only has an effect for 'date' type, where it is format string for a SimpleDateFormat.\n"
                    + " \nExamples:\n"
                    + "      -{int,1900,2011}\n"
                    + "       integer - generate a random value between 1900 and 2011;\n"
                    + "      -{int,user-%d,100,999}\n"
                    + "       generate a random integer between 100 and 999, and return a <em>STRING</em> value formatted as \"user-n\";\n"
                    + "      -{string,5,10}\n"
                    + "       string - generate a random string between 5 and 10 characters long;\n"
                    + "      -{string,product-%s,5,10}\n"
                    + "       generate a random string between 5 and 10 characters long, then return it in the form \"product-xyz\"\n"
                    + "      -{boolean}\n"
                    + "       generate a random boolean value, with values evenly spread between true and false.\n"
                    + "      -{boolean,30}\n"
                    + "       generate a random boolean value, which is true 30% of the time;\n"
                    + "      -{date,%tc,1910/1/1,2011/12/31,yyyy/MM/dd}\n"
                    + "       generate a random date between Jan 1 1910 and Dec 31 2011, formatted with %tc.\n"
                    + "      -{value,1,0}\n"
                    + "       value - retrieve a random line from the loaded data file; ignore the first line (X=1); and use the 0th column (Y=0) from that line;\n\n"
                    + " \nEvery '?' in the SQL is replaced by a value generated according to the corresponding format specifier.\n"
                    + " \nNote also that if you need only change the log output format, then Java allows you to provide the\n"
                    + "     \"java.util.logging.SimpleFormatter.format\" option in the Java options\n"
                    + "      Ex: java -cp target:nuodbjdbc.jar -Djava.util.logging.SimpleFormatter.format=\"%4\\$s: %5\\$s%n\" nuodb.SimpleDriver -url ...\n"
                    + " \nSee the documentation on java.util.logging.SimpleFormatter for details of the format string."


        ;

    /** The slots in the stats array */
    private static final int STATS_START_TIME        = 0;
    private static final int STATS_END_TIME          = 1;
    private static final int STATS_OPS_COUNT         = 2;
    private static final int STATS_ROW_COUNT         = 3;
    private static final int STATS_TX_COUNT          = 4;
    private static final int STATS_LATENCY_TIME      = 5;
    private static final int STATS_INACTIVE_TIME     = 6;
    private static final int STATS_OPS_TIME          = 7;
    private static final int STATS_TX_TIME           = 8;
    private static final int STATS_ABORT_CONFLICT    = 9;
    private static final int STATS_ABORT_DEADLOCK    =10;

    /** constants for time calculations */
    private static final double NANO_TO_MILLIS = 1.0d / 1000000;
    private static final double MILLIS_TO_SECONDS = 1.0d / 1000;
    private static final long NANOS_IN_SECOND = 1000000000;
    private static final long NANOS_IN_MILLI = 1000000;

    /** compiled patterns for matching and splitting text */
    private static final Pattern paramPattern = Pattern.compile("\\?(\\{[^{]+\\})?");

    private static final Pattern verbPattern = Pattern.compile("^([^ ]+) +.+");
    private static final Pattern propertyPattern = Pattern.compile("^([^=: ]+)[=:](.+)$");
    private static final Pattern formatPattern = Pattern.compile(" *; *");
    private static final Pattern variableReferencePattern = Pattern.compile("\\$\\{([^\\}]+)\\}");

}
