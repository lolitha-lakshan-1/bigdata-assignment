import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class PostgresResultSink {

    private static final String POSTGRES_URL =
            "jdbc:postgresql://postgres-task4:5432/bigdata-task-4";
    private static final String POSTGRES_USER = "bigdata";
    private static final String POSTGRES_PASSWORD = "bigdata";

    private static final String INSERT_SQL =
            "INSERT INTO hashtag_results " +
            "(flink_job_id, time_window, raw_event_count, hashtag_event_count, processDateTimeMillis) " +
            "VALUES (?, ?, ?, ?, ?)";

    public static SinkFunction<Result> createResultSink() {
        JdbcExecutionOptions.Builder executionBuilder = JdbcExecutionOptions.builder();
        executionBuilder.withBatchSize(500);
        executionBuilder.withBatchIntervalMs(1000);
        executionBuilder.withMaxRetries(3);
        JdbcExecutionOptions executionOptions = executionBuilder.build();

        JdbcConnectionOptions.JdbcConnectionOptionsBuilder connectionBuilder =
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        connectionBuilder.withUrl(POSTGRES_URL);
        connectionBuilder.withDriverName("org.postgresql.Driver");
        connectionBuilder.withUsername(POSTGRES_USER);
        connectionBuilder.withPassword(POSTGRES_PASSWORD);
        JdbcConnectionOptions connectionOptions = connectionBuilder.build();

        ResultJdbcStatementBuilder statementBuilder = new ResultJdbcStatementBuilder();

        SinkFunction<Result> sink =
                JdbcSink.sink(
                        INSERT_SQL,
                        statementBuilder,
                        executionOptions,
                        connectionOptions
                );

        return sink;
    }
}
