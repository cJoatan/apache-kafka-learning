package org.example;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {
    public static void main(String[] args) throws IOException {

        final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class);

        final RestHighLevelClient openSearchClient = createOpenSearchClient();
        final KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

        try(openSearchClient; kafkaConsumer) {

            final boolean wikimediaExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!wikimediaExists) {
                final CreateIndexRequest wikimedia = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(wikimedia, RequestOptions.DEFAULT);
            }

            kafkaConsumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));

                final int recordsCount = records.count();
                log.info("Received: " + recordsCount + " records");

                for (ConsumerRecord<String, String> record : records) {
                    IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON);

                    openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    log.info("Inserted 1 document into the OpenSearch");



                }
            }
        }
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {

        final String bootstrapServer = "localhost:9092";
        final String groupId = "consumer-opensearch-demon";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        return new KafkaConsumer<String, String>(properties);
    }

    private static RestHighLevelClient createOpenSearchClient() {
        final String connString = "htto://localhost:9200";

        RestHighLevelClient restHighLevelClient;
        final URI uri = URI.create(connString);
        final String userInfo = uri.getUserInfo();

        if(userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort())));
        } else {

            final String[] auth = userInfo.split(":");
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            final HttpHost httpHost = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
            final RestClientBuilder restClientBuilder = RestClient.builder(httpHost)
                .setHttpClientConfigCallback(
                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                );

            restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        }

        return restHighLevelClient;
    }


}