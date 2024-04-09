package io.streamnative.data.feeds.realtime.coinbase;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.LocalRunner;

import java.util.HashMap;
import java.util.Map;

public class VolatilityTrackerLocalRunner {

    private static final Map<String, Object> CONFIGS = new HashMap<>();
    private static final Map<String, Object> SECRETS = new HashMap<>();
    private static final String QUERY = "";

    static {
        CONFIGS.put(PinotReader.PORT_PROPERTY_NAME, 8000);
        CONFIGS.put(PinotReader.HOST_PROPERTY_NAME, "localhost");
        CONFIGS.put(PinotReader.QUERY_PROPERTY_NAME, QUERY);
        SECRETS.put(PinotReader.USER_SECRET_NAME, "admin");
        SECRETS.put(PinotReader.PASSWORD_SECRET_NAME, "");
    }

    public static void main(String[] args) throws Exception {
        SourceConfig sourceConfig =
                SourceConfig.builder()
                        .className(VolatilityTracker.class.getName())
                        .configs(CONFIGS)
                        .secrets(SECRETS)
                        .topicName("persistent://public/default/volatility")
                        .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATMOST_ONCE)
                        .schemaType(Schema.JSON(Volatility.class).getSchemaInfo().getName())
                        .name("volatility-tracker")
                        .build();

        LocalRunner localRunner =
                LocalRunner.builder()
                        .brokerServiceUrl("pulsar://192.168.1.100:6650")
                        .sourceConfig(sourceConfig)
                        .build();

        localRunner.start(false);
        Thread.sleep(120 * 1000);
        localRunner.stop();
    }
}
