import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ResultJdbcStatementBuilder implements JdbcStatementBuilder<Result> {

    @Override
    public void accept(PreparedStatement statement, Result result) throws SQLException {
        String jobId = result.getFlinkJobId();
        long windowEndMillis = result.getWindowEndMillis();
        long rawEventCount = result.getRawEventCount();
        long hashtagEventCount = result.getHashtagEventCount();

        statement.setString(1, jobId);
        statement.setTimestamp(2, new Timestamp(windowEndMillis));
        statement.setLong(3, rawEventCount);
        statement.setLong(4, hashtagEventCount);
        statement.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
    }
}
