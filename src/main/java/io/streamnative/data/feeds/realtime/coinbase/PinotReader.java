package io.streamnative.data.feeds.realtime.coinbase;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PinotReader implements Closeable {

    public static final String HOST_PROPERTY_NAME = "db.host";
    public static final String PORT_PROPERTY_NAME = "db.port";
    public static final String USER_SECRET_NAME = "db.user";
    public static final String PASSWORD_SECRET_NAME = "db.password";
    public static final String QUERY_PROPERTY_NAME = "db.query";
    private String host;

    private Integer port;

    private String userName;

    private String password;

    private String query;

    private String jdbcUrl;

    private Connection connection;

    private ScheduledExecutorService scheduler;

    private PushSource pushSource;

    public PinotReader(PushSource pushSrc, SourceContext srcCtx) {
        this.pushSource = pushSrc;

        this.host = (String) srcCtx.getSourceConfig().getConfigs()
                .getOrDefault(HOST_PROPERTY_NAME, "localhost");

        this.port = (Integer)srcCtx.getSourceConfig().getConfigs()
                .getOrDefault(PORT_PROPERTY_NAME, 8000);

        this.query = (String) srcCtx.getSourceConfig().getConfigs()
                .get(QUERY_PROPERTY_NAME);

        this.userName = srcCtx.getSecret(USER_SECRET_NAME);
        this.password = srcCtx.getSecret(PASSWORD_SECRET_NAME);

        scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                executeQuery();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.MINUTES); // Run immediately and then every 1 minute

        // Shut down the scheduler gracefully when the program exits
        Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));
    }

    private void executeQuery() throws SQLException {
        try (Connection connection = getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(this.query);
            while (rs.next()) {
                Volatility volatility = new Volatility(rs.getLong(0),
                        rs.getLong(1),
                        rs.getString(2),
                        rs.getFloat(3));
                this.pushSource.consume(new VolatiltyRecord(volatility));
            }

            rs.close();
            stmt.close();
        }
    }

    private String getJdbcUrl() {
        if (jdbcUrl == null) {
            jdbcUrl = "jdbc:pinot://" + host + ":" + port;
        }
        return jdbcUrl;
    }

    private Connection getConnection() throws SQLException {
        if (this.connection == null) {
            this.connection = DriverManager.getConnection(getJdbcUrl(), userName, password);
        }
        return connection;
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
