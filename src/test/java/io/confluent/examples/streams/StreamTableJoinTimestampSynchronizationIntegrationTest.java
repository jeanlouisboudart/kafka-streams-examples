package io.confluent.examples.streams;

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Time;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * End-to-end integration test that demonstrates how timestamp synchronization works between KStreams and KTable.
 */
public class StreamTableJoinTimestampSynchronizationIntegrationTest {

    private static final String userClicksTopic = "user-clicks";
    private static final String userRegionsTopic = "user-regions";
    private static final String outputTopic = "output-topic";
    private static final String intermediateTopic = "intermediate-topic";

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(new Properties() {
        {
            put(KafkaConfig.ZkConnectionTimeoutMsProp(), "30000");
        }
    });
    private KafkaStreams streams;

    @BeforeClass
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopic(userClicksTopic);
        CLUSTER.createTopic(userRegionsTopic);
        CLUSTER.createTopic(intermediateTopic);
        CLUSTER.createTopic(outputTopic);
    }


    public void simpleJoinTopology(final TimestampExtractor timestampExtractor) {

        final Serde<String> stringSerde = Serdes.String();

        final Properties streamsConfiguration = buildStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();


        // a simple use case with a KStream Topic that needs to join with a KTable
        // internally KTable are versioned per timestamp.
        // the timestamp used by default is ingestion time,
        // but you can change that to your need to use event time (a field from the message) by configuring
        // a timestamp extractor on the stream and / or table
        // when doing a KStreams/KTable join the framework will look up for the value of a given key
        // in the KTable at a timestamp <= to the timestamp of the event on the stream side

        final KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));
        final KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));

        userClicksStream
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
    }

    @After
    public void stopStreams() {
        if (streams != null) {
            streams.close();
        }
    }

    @Test
    public void shouldMatchIfEventArriveInRightOrder() throws ExecutionException, InterruptedException {
        //with the default timestamp extractor which rely on the kafka timestamp (ingestion time)
        simpleJoinTopology(new FailOnInvalidTimestamp());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();


        // publish an event in KTABLE

        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                // at T0 alice leaves in asia
                Arrays.asList(new KeyValue<>("alice", "100|asia")),
                producerConfig
        );

        // publish an event in KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                // at T1 alice clicks
                Arrays.asList(new KeyValue<>("alice", "200|user click 1")),
                producerConfig);

        // publish an event in KTABLE

        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                // at T2 alice leaves in europe
                Arrays.asList(new KeyValue<>("alice", "100|europe")),
                producerConfig
        );


        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );

        // we should have a result as timestamp from Ktable is <= from the timestamp from the event on KStream side
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(new KeyValue<>("alice","200|user click 1 --- 100|asia")));

    }

    @Test(expected = AssertionError.class)
    public void shouldNotMatchIfEventDoesNotArriveInRightOrder() throws ExecutionException, InterruptedException {
        //with the default timestamp extractor which rely on the kafka timestamp (ingestion time)
        simpleJoinTopology(new FailOnInvalidTimestamp());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();

        //publish an event in  KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                // at T0 alice clicks
                Arrays.asList(new KeyValue<>("alice", "200|user click 1")),
                producerConfig);


        //publish an event in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                // at T1 alice leaves in asia
                Arrays.asList(new KeyValue<>("alice", "100|asia")),
                producerConfig
        );

        // we should have no result as the timestamp from KTable is > to KStream side
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig,
                outputTopic,
                1
        );
    }

    @Test
    public void shouldMatchIfEventArriveDoesNotInRightOrderWithTimestampExtractor() throws ExecutionException, InterruptedException {
        //with a custom timestamp extractor that will use for the demo the value before the pipe character as a timestamp !
        simpleJoinTopology(new MyTimestampExtractor());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();

        // publish an event in  KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                // at T0 (ingest time) alice clicks but contains a business timestamp of 200
                Arrays.asList(new KeyValue<>("alice", "200|user click 1")),
                producerConfig);


        //publish an event in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                // at T1 (ingest time) alice leaves in asia but contains a business timestamp of 100
                Arrays.asList(new KeyValue<>("alice", "100|asia")),
                producerConfig
        );

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        //we should have one result as we defined a custom timestamp extractor
        //as such kafka streams will use that as a referential for the ktable to do timestamp synchronization
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(new KeyValue<>("alice","200|user click 1 --- 100|asia")));

    }

    @Test
    public void multiEvent() throws ExecutionException, InterruptedException {
        //with a custom timestamp extractor that will use for the demo the value before the pipe character as a timestamp !
        simpleJoinTopology(new MyTimestampExtractor());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();

        // publish multiple events in KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        // at T0 (ingest time) alice clicks but contains a business timestamp of 200
                        new KeyValue<>("alice", "200|click 1"),
                        // at T1 (ingest time) bob clicks but contains a business timestamp of 201
                        new KeyValue<>("bob", "201|click 1"),
                        // at T2 (ingest time) joe clicks but contains a business timestamp of 202
                        new KeyValue<>("joe", "202|click 1")
                        ),
                producerConfig
        );


        // publish multiple events in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        // at T3 (ingest time) alice leaves in asia but contains a business timestamp of 100
                        new KeyValue<>("alice", "100|asia"),
                        // at T4 (ingest time) bob leaves in eruope but contains a business timestamp of 101
                        new KeyValue<>("bob", "101|europe"),
                        // at T5 (ingest time) joe leaves in asia but contains a business timestamp of 300
                        new KeyValue<>("joe", "300|us")
                        ),
                producerConfig
        );

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        2
                );

        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia"),
                new KeyValue<>("bob","201|click 1 --- 101|europe")
        ));

    }

    @Test
    @Ignore("Result should be 200|click 1 --- 100|asia")
    public void sameTimestampWhenKTableEventIsReceivedAfterKStreamEvent() throws ExecutionException, InterruptedException {
        //with a custom timestamp extractor that will use for the demo the value before the pipe character as a timestamp !
        simpleJoinTopology(new MyTimestampExtractor());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();

        //publish an event in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                // at T1 (ingest time) alice leaves in asia but contains a business timestamp of 100
                Arrays.asList(new KeyValue<>("alice", "100|asia")),
                producerConfig
        );

        // publish an event in  KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                // at T0 (ingest time) alice clicks but contains a business timestamp of 100
                Arrays.asList(new KeyValue<>("alice", "100|click 1")),
                producerConfig);



        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        //we should have one result as we defined a custom timestamp extractor
        //as such kafka streams will use that as a referential for the ktable to do timestamp synchronization

        //Edge case where both KStream and KTable have same timestamp, but do arrive in first KTable then KStreams side, today join doesn't match
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(new KeyValue<>("alice","100|click 1 --- 100|asia")));
    }

    @Test(expected = AssertionError.class)
    public void shouldNotMatchIfEventIsATombstone() throws ExecutionException, InterruptedException {
        //with the default timestamp extractor which rely on the kafka timestamp (ingestion time)
        simpleJoinTopology(new FailOnInvalidTimestamp());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();

        // publish multiple events in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        // at T0 alice leavers in asia
                        new KeyValue<>("alice", "100|asia"),
                        // at T1 alice is deleted from the system
                        new KeyValue<>("alice", null)
                ),
                producerConfig
        );

        // publish multiple events in KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        // at T2 alice clicks
                        new KeyValue<>("alice", "200|click 1")
                ),
                producerConfig
        );

        // we should have no result as the timestamp from KTable is > to KStream side
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                consumerConfig,
                outputTopic,
                1
        );

    }

    @Test
    public void shouldMatchIfTombstoneIsAfterKStreamEvent() throws ExecutionException, InterruptedException {
        //with the default timestamp extractor which rely on the kafka timestamp (ingestion time)
        simpleJoinTopology(new FailOnInvalidTimestamp());
        streams.start();

        final Properties producerConfig = producerConfig();
        final Properties consumerConfig = consumerConfig();

        // publish multiple events in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        // at T0 alice leaves in asia
                        new KeyValue<>("alice", "100|asia")
                ),
                producerConfig
        );

        // publish multiple events in KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        // at T1 alice clicks
                        new KeyValue<>("alice", "200|click 1")
                ),
                producerConfig
        );

        // publish multiple events in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        // at T2 alice is deleted from the system
                        new KeyValue<>("alice", null)
                ),
                producerConfig
        );


        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig,
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
    }

    /*
     EDGE CASE
     In all cases result should be
     Results should be "200|click 1 --- 100|asia"
     */
    @Test
    public void worksAsExpected() throws ExecutionException, InterruptedException {
        simpleJoinTopology(new FailOnInvalidTimestamp());
        streams.start();


        aliceChangesRegionsAfterClickingOnWebsite(producerConfig());

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(),
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
    }

    @Test
    @Ignore("Result should be 200|click 1 --- 100|asia")
    public void returnsWrongKTableEventWithRepartitionTopic() throws ExecutionException, InterruptedException {
        kStreamWithImplicitReKeyJoinTopology(new FailOnInvalidTimestamp());
        streams.start();


        aliceChangesRegionsAfterClickingOnWebsite(producerConfig());

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(),
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
    }

    @Test
    @Ignore("Result should be 200|click 1 --- 100|asia")
    public void returnsWrongKTableEventWithIntermediateTopic() throws ExecutionException, InterruptedException {
        kStreamWithIntermediateTopicReKeyJoinTopology(new FailOnInvalidTimestamp());
        streams.start();


        aliceChangesRegionsAfterClickingOnWebsite(producerConfig());

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(),
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
    }

    @Test
    @Ignore("Result should be 200|click 1 --- 100|asia")
    public void returnsWrongKTableEventWithIntermediateTopicAndTimestampExtractor() throws ExecutionException, InterruptedException {
        kStreamWithIntermediateTopicReKeyJoinTopology(new MyTimestampExtractor());
        streams.start();


        aliceChangesRegionsAfterClickingOnWebsite(producerConfig());

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(),
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
    }

    @Test
    @Ignore("Result should be 200|click 1 --- 100|asia")
    public void returnsWrongKTableEventWithExplicitSubTopologyAndTimestampExtractor() throws ExecutionException, InterruptedException {
        kStreamWithExplicitReKeyJoinWithExplicitSubTopology(new MyTimestampExtractor());
        streams.start();


        aliceChangesRegionsAfterClickingOnWebsite(producerConfig());

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(),
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
    }

    @Test
    public void worksAsExpectedWithTwoSeparatedApp() throws ExecutionException, InterruptedException {
        final TimestampExtractor timestampExtractor = new MyTimestampExtractor();
        KafkaStreams reKeyedStreams = reKeyStreamTopology(timestampExtractor);
        reKeyedStreams.start();

        KafkaStreams joinedStreams = joinKTableSubTopology(timestampExtractor);
        joinedStreams.start();

        aliceChangesRegionsAfterClickingOnWebsite(producerConfig());

        final List<KeyValue<String, String>> actualClicksPerRegion =
                IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                        consumerConfig(),
                        outputTopic,
                        1
                );
        assertThat(actualClicksPerRegion).isEqualTo(Arrays.asList(
                new KeyValue<>("alice","200|click 1 --- 100|asia")
        ));
        reKeyedStreams.close();
        joinedStreams.close();
    }

    private void aliceChangesRegionsAfterClickingOnWebsite(Properties producerConfig) throws ExecutionException, InterruptedException {
        // publish an events in KTABLE
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        // at T0 alice leaves in asia
                        new KeyValue<>("alice", "100|asia")
                ),
                producerConfig
        );
        // publish multiple events in KSTREAM
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userClicksTopic,
                Arrays.asList(
                        // at T1 alice clicks
                        new KeyValue<>("alice", "200|click 1")
                ),
                producerConfig
        );
        // publish multiple events in KTABLE at T2
        IntegrationTestUtils.produceKeyValuesSynchronously(
                userRegionsTopic,
                Arrays.asList(
                        // at T2
                        new KeyValue<>("alice", "300|europe")
                ),
                producerConfig
        );
    }

    public void kStreamWithImplicitReKeyJoinTopology(final TimestampExtractor timestampExtractor) {

        final Serde<String> stringSerde = Serdes.String();

        final Properties streamsConfiguration = buildStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));
        final KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));

        userClicksStream
                .selectKey((key, value) -> key)
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
    }

    public void kStreamWithIntermediateTopicReKeyJoinTopology(final TimestampExtractor timestampExtractor) {

        final Serde<String> stringSerde = Serdes.String();

        final Properties streamsConfiguration = buildStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));
        final KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));

        userClicksStream
                .selectKey((key, value) -> key)
                .through(intermediateTopic,Produced.with(stringSerde, stringSerde))
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
    }

    public void kStreamWithExplicitReKeyJoinWithExplicitSubTopology(final TimestampExtractor timestampExtractor) {

        final Serde<String> stringSerde = Serdes.String();

        final Properties streamsConfiguration = buildStreamsConfig();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));
        final KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));

        userClicksStream
                .selectKey((key, value) -> key)
                .to(intermediateTopic, Produced.with(stringSerde, stringSerde));

        builder.stream(intermediateTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor))
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private KafkaStreams reKeyStreamTopology(TimestampExtractor timestampExtractor) {
        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties streamsConfiguration = buildStreamsConfig();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,"sub-topology-1");
        final KStream<String, String> userClicksStream = builder.stream(userClicksTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));

        userClicksStream
                .selectKey((key, value) -> key)
                .to(intermediateTopic, Produced.with(stringSerde, stringSerde));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

    private KafkaStreams joinKTableSubTopology(TimestampExtractor timestampExtractor) {
        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();
        final Properties streamsConfiguration = buildStreamsConfig();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,"sub-topology-2");
        final KStream<String, String> rekeyedStream = builder.stream(intermediateTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));
        final KTable<String, String> userRegionsTable = builder.table(userRegionsTopic, Consumed.with(stringSerde, stringSerde).withTimestampExtractor(timestampExtractor));

        rekeyedStream
                .join(userRegionsTable, this::join)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }


    private Properties buildStreamsConfig() {
        Properties streamsConfiguration = new Properties();
        //config for integration tests
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-timestamp-sync-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

    private Properties consumerConfig() {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "punctuate-test-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return consumerConfig;
    }

    private Properties producerConfig() {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return producerConfig;
    }

    private String join(final String left, final String right) {
        return left + " --- " + right;
    }

    /*
    For the demo the timestamp extractor will the value before the pipe in the record value,
    however real life you may need to deserialize the event to extract the appropriate field in the payload
     */
    private class MyTimestampExtractor implements TimestampExtractor {

        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
            return Long.valueOf(record.value().toString().split("\\|")[0]);
        }
    }

}
