package com.reltio.db.layer;

import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.out;

public class ScanV2Test {
    static final String TABLE = "random2";
    final DynamoDbAsyncClient client;
    final Random rnd = new Random();
    final List<Pair<String, Long>> data = new ArrayList<>();

    ScanV2Test() {
        try {
            client = connect();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        ScanV2Test test = new ScanV2Test();
        try {
            test.delete();
            test.create();
            test.generate();
//            dump();
            int nrequests = 100000;
            int limit = 100;
            {
                Statistics stat = new Statistics("scan sequential");
                for (int i = 0; i < nrequests; i++) {
                    ScanResponse rs = stat.measure(
                        () -> test.client.scan(ScanRequest.builder()
                                .tableName(TABLE)
                                .limit(limit)
                                .filterExpression("#nid > :n")
                                .expressionAttributeNames(Collections.singletonMap("#nid", "nid"))
                                .expressionAttributeValues(Collections.singletonMap(":n", AttributeValue.fromN(Long.toString(test.rnd.nextInt(8000)))))
                                .consistentRead(FALSE)
                                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                .build())
                        .join()
                    );
                    stat.addConsumed(rs.consumedCapacity());
                    if (rs.count() == 0 || rs.count() > limit) throw new RuntimeException("wrong number of rows returned " + rs.count());
                }
                stat.print(nrequests);
            }
            {
                Statistics stat = new Statistics("scan parallel");
                for (int b = 0; b < 100; b++) {
                    List<CompletableFuture<ScanResponse>> requests = new ArrayList<>(nrequests / 100);
                    for (int i = 0; i < nrequests / 100; i++) {
                        long st = System.currentTimeMillis();
                        requests.add(test.client.scan(ScanRequest.builder()
                                .tableName(TABLE)
                                .limit(limit)
                                .filterExpression("#nid > :n")
                                .expressionAttributeNames(Collections.singletonMap("#nid", "nid"))
                                .expressionAttributeValues(Collections.singletonMap(":n", AttributeValue.fromN(Long.toString(test.rnd.nextInt(8000)))))
                                .consistentRead(FALSE)
                                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                .build())
                            .thenApply(
                                rs -> {
                                    stat.add(System.currentTimeMillis() - st);
                                    stat.addConsumed(rs.consumedCapacity());
                                    if (rs.count() == 0 || rs.count() > limit)
                                        throw new RuntimeException("wrong number of rows returned " + rs.count());
                                    return rs;
                                })
                        );
                    }
                    CompletableFuture.allOf(requests.toArray(new CompletableFuture[1])).join();
                }
                stat.print(nrequests);
            }
            {
                Statistics stat = new Statistics("getitem sequential");
                for (int i = 0; i < nrequests; i++) {
                    Pair<String, Long> d = test.data.get(test.rnd.nextInt(test.data.size()));
                    GetItemResponse rs = stat.measure(() -> test.client.getItem(GetItemRequest.builder()
                        .tableName(TABLE)
                        .key(ImmutableMap.of("id", AttributeValue.fromS(d.a),
                                             "nid", AttributeValue.fromN(d.b.toString())))
                        .attributesToGet("id", "nid", "data")
                        .consistentRead(FALSE)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .build()).join()
                    );
                    stat.addConsumed(rs.consumedCapacity());
                    if (!rs.item().get("id").s().equals(d.a) ||
                        !rs.item().get("nid").n().equals(d.b.toString()))
                        throw new RuntimeException("invalid getItem response " + rs);
                }
                stat.print(nrequests);
            }
            {
                Statistics stat = new Statistics("getitem parallel");
                for (int b = 0; b < 100; b++) {
                    List<CompletableFuture<GetItemResponse>> requests = new ArrayList<>(nrequests / 100);
                    for (int i = 0; i < nrequests / 100; i++) {
                        Pair<String, Long> d = test.data.get(test.rnd.nextInt(test.data.size()));
                        long st = System.currentTimeMillis();
                        requests.add(
                            test.client.getItem(GetItemRequest.builder()
                                    .tableName(TABLE)
                                    .key(ImmutableMap.of("id", AttributeValue.fromS(d.a),
                                        "nid", AttributeValue.fromN(d.b.toString())))
                                    .attributesToGet("id", "nid", "data")
                                    .consistentRead(FALSE)
                                    .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                                    .build())
                                .thenApply(rs -> {
                                    stat.add(System.currentTimeMillis() - st);
                                    stat.addConsumed(rs.consumedCapacity());
                                    if (!rs.item().get("id").s().equals(d.a) ||
                                        !rs.item().get("nid").n().equals(d.b.toString()))
                                        throw new RuntimeException("invalid getItem response " + rs);
                                    return rs;
                                })
                        );
                    }
                    CompletableFuture.allOf(requests.toArray(new CompletableFuture[1])).join();
                }
                stat.print(nrequests);
            }
        } catch (Exception e) {
            out.println("unhandled exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            test.delete();
        }
    }

    static DynamoDbAsyncClient connect() throws URISyntaxException {
        String key = System.getenv("AWS_ACCESS_KEY_ID");
        DynamoDbAsyncClientBuilder builder = DynamoDbAsyncClient.builder();
        if (key != null && !key.isEmpty()) {
            Region region = System.getenv("AWS_REGION") != null ?
                Region.of(System.getenv("AWS_RERGION")) : Region.US_EAST_1;
            out.println("connecting to region " + region + " with key " + key);
            builder.credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(key,
                         System.getenv("AWS_SECRET_ACCESS_KEY"), System.getenv("AWS_SESSION_TOKEN"))))
                .region(region);
        } else {
            String url = "http://localhost:8000";
            out.println("connecting to " + url + " as local/local");
            builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
                .endpointOverride(new URI(url))
                .region(Region.AWS_GLOBAL);
        }
        return builder
            .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                .maxConcurrency(1000)
                .maxPendingConnectionAcquires(100_000)
                .connectionMaxIdleTime(Duration.ofSeconds(60))
                .connectionTimeout(Duration.ofSeconds(30))
                .connectionAcquisitionTimeout(Duration.ofSeconds(30))
                .readTimeout(Duration.ofSeconds(30))
                .tcpKeepAlive(true)
                .connectionAcquisitionTimeout(Duration.ofSeconds(30))
            )
            .asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run))
            .build();
    }

    void create() {
        try {
            client.createTable(CreateTableRequest.builder()
                .tableName(TABLE)
                .attributeDefinitions(
                    AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
                    AttributeDefinition.builder().attributeName("nid").attributeType(ScalarAttributeType.N).build())
                .keySchema(
                    KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("nid").keyType(KeyType.RANGE).build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                    .readCapacityUnits(5000L).writeCapacityUnits(500L).build())
                .build()).join();
            TableStatus status = TableStatus.CREATING;
            do {
                try {
                    Thread.sleep(500);
                    DescribeTableResponse response = client.describeTable(DescribeTableRequest.builder().tableName(TABLE).build()).join();
                    status = response.table().tableStatus();
                    out.print("\rwaiting for ACTIVE ... now " + status +
                        " throughput " + response.table().provisionedThroughput().readCapacityUnits() + " rcu/" +
                            response.table().provisionedThroughput().writeCapacityUnits() + " wcu");
                } catch (CompletionException | InterruptedException e) {
                    if (!(e.getCause() instanceof ResourceInUseException)) {
                        out.println("describe table 2 exception: " + e.getMessage());
                        e.printStackTrace();
                        break;
                    }
                }
            } while (status != TableStatus.ACTIVE);
            out.println();
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof ResourceInUseException)) {
                out.println("create table exception: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }
    }

    void delete() {
        try {
            client.deleteTable(DeleteTableRequest.builder().tableName(TABLE).build()).join();
        } catch (CompletionException e) {
            if (!(e.getCause() instanceof ResourceNotFoundException)) {
                out.println("delete table execption : " + e.getMessage());
                e.printStackTrace();
            }
        }
        TableStatus status;
        do {
            try {
                Thread.sleep(500);
                DescribeTableResponse response = client.describeTable(DescribeTableRequest.builder().tableName(TABLE).build()).join();
                status = response.table().tableStatus();
                out.print("\rwaiting for DELETE ... now " + status);
            } catch (CompletionException | InterruptedException e) {
                if (!(e.getCause() instanceof ResourceNotFoundException)) {
                    out.println("describe table exception: " + e.getMessage());
                    e.printStackTrace();
                }
                break;
            }
        } while (status == TableStatus.DELETING || status == TableStatus.ACTIVE);
        out.println();
    }

    void generate() {
        long nid = 0L;
        for (int i = 0; i < 500; i++) {
            List<WriteRequest> requests = new ArrayList<>();
            for (int j = 0; j< 20; j++) {
                Map<String, AttributeValue> item = new HashMap<>();
                String id = UUID.randomUUID().toString();
                item.put("id", AttributeValue.fromS(id));
                item.put("nid", AttributeValue.fromN(Long.toString(nid)));
                String d = randomString(rnd.nextInt(100) + 100);
                item.put("data", AttributeValue.fromS(d));
                requests.add(WriteRequest.builder().putRequest(PutRequest.builder().item(item).build()).build());
                data.add(new Pair<>(id, nid));
                nid++;
            }
            client.batchWriteItem(BatchWriteItemRequest.builder()
                .requestItems(ImmutableMap.of(TABLE, requests))
                .build()).join();
        }
    }

    void dump() {
        ScanRequest rq = ScanRequest.builder()
            .tableName(TABLE)
            .consistentRead(TRUE)
            .attributesToGet("id", "nid", "data")
            .build();
        ScanResponse rs = client.scan(rq).join();
        out.println(rs.count());
        for (Map<String, AttributeValue> item : rs.items())
            out.println(item.get("id").s() + ", " + item.get("nid").n() + ": " + item.get("data").s());
    }

    String randomString(int len) {
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) builder.append((char)('a' + rnd.nextInt(26)));
        return builder.toString();
    }

    static class Statistics {
        final String title;
        final ArrayList<Long> points = new ArrayList<>(10000);
        final DoubleAdder rcu = new DoubleAdder();
        final long started;

        public Statistics(String t) {
            this.title = t;
            this.started = System.currentTimeMillis();
        }

        synchronized void add(long point) { points.add(point); }

        void addConsumed(ConsumedCapacity consumed) {
            if (consumed != null && consumed.capacityUnits() != null) {
                rcu.add(consumed.capacityUnits());
            }
        }

        Map<String, Long> percentiles() {
            if (points.isEmpty())  return Collections.emptyMap();
            if (points.size() == 1)  return ImmutableMap.of("50th", points.get(0), "90th", points.get(0));
            points.sort(Long::compareTo);
            Map<String, Long> result = new HashMap<>();
            int i = Math.min(points.size() * 50 / 100 + 1, points.size() - 1);
            while (i < points.size() - 1 && points.get(i - 1).equals(points.get(i))) i++;
            result.put("50th", points.get(i));
            i = Math.min(points.size() * 90 / 100 + 1, points.size() - 1);
            while (i < points.size() - 1 && points.get(i - 1).equals(points.get(i))) i++;
            result.put("90th", points.get(i));
            return result;
        }

        <T> T measure(Supplier<T> operation) {
            long started = System.currentTimeMillis();
            try {
                return operation.get();
            } finally {
                add(System.currentTimeMillis() - started);
            }
        }

        void print(int nrequests) {
            out.println(title + " " + nrequests + " total: " + (System.currentTimeMillis() - started) + "ms " +
                "consumed " + rcu + " rcu");
            out.println(title + " percentiles: " + percentiles()
                .entrySet().stream().map(e -> e.getKey() + " " + e.getValue() + "ms")
                .collect(Collectors.joining(", ")));
        }
    }

    static class Pair<A, B> {
        final A a;
        final B b;

        public Pair(A a, B b) { this.a = a; this.b = b; }
    }
}
