package com.reltio.db.layer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AtomicDouble;
import com.reltio.common.Pair;

import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.out;

public class ScanV1Test {
    static final String TABLE = "ScanV1Test";
    final AmazonDynamoDB client;
    final Random rnd = new Random();
    final List<Pair<String, Long>> data = new ArrayList<>();

    ScanV1Test() {
        try {
            client = connect();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        ScanV1Test test = new ScanV1Test();
        try {
            test.delete();
            test.create();
            test.generate();
//        dump();
            int nrequests = 200;
            int limit = 100;
            {
                Statistics stat = new Statistics("scan sequential");
                for (int i = 0; i < nrequests; i++) {
                    stat.measure(() -> {
                        ScanResult rs = test.client.scan(new ScanRequest(TABLE)
                            .withConsistentRead(FALSE)
                            .withLimit(limit)
                            .withFilterExpression("#nid > :n")
                            .withExpressionAttributeNames(Collections.singletonMap("#nid", "nid"))
                            .withExpressionAttributeValues(Collections.singletonMap(":n", new AttributeValue().withN(Long.toString(test.rnd.nextInt(8000)))))
                        );
                        if (rs.getCount() == 0 || rs.getCount() > limit) throw new RuntimeException("invalid number of rows returned " + rs.getCount());
                    });
                }
                stat.print(nrequests);
            }
            {
                Statistics stat = new Statistics("scan parallel");
                ExecutorService pool = Executors.newFixedThreadPool(50);
                for (int i = 0; i < nrequests; i++) {
                    pool.submit(() -> stat.measure(() -> {
                        ScanResult rs = test.client.scan(new ScanRequest(TABLE)
                            .withConsistentRead(FALSE)
                            .withLimit(limit)
                            .withFilterExpression("#nid > :n")
                            .withExpressionAttributeNames(Collections.singletonMap("#nid", "nid"))
                            .withExpressionAttributeValues(Collections.singletonMap(":n", new AttributeValue().withN(Long.toString(test.rnd.nextInt(8000)))))
                        );
                        if (rs.getCount() == 0 || rs.getCount() > limit) throw new RuntimeException("invalid number of rows returned " + rs.getCount());
                    }));
                }
                pool.shutdown();
                if (!pool.awaitTermination(1, TimeUnit.MINUTES))
                    throw new RuntimeException("incorrect pool termination");
                stat.print(nrequests);
            }
            {
                Statistics stat = new Statistics("getitem sequential");
                for (int i = 0; i < nrequests; i++) {
                    Pair<String, Long> d = test.data.get(test.rnd.nextInt(test.data.size()));
                    stat.measure(() -> {
                        GetItemResult rs = test.client.getItem(new GetItemRequest()
                            .withTableName(TABLE)
                            .withKey(ImmutableMap.of("id", new AttributeValue(d.a),
                                                     "nid", new AttributeValue().withN(d.b.toString())))
                            .withAttributesToGet("id", "nid", "data")
                            .withConsistentRead(FALSE));
                        if (!rs.getItem().get("id").getS().equals(d.a) ||
                            !rs.getItem().get("nid").getN().equals(d.b.toString()))
                            throw new RuntimeException("invalid getItem response " + rs);
                    });
                }
                stat.print(nrequests);
            }
            {
                Statistics stat = new Statistics("getitem parallel");
                ExecutorService pool = Executors.newFixedThreadPool(50);
                for (int i = 0; i < nrequests; i++) {
                    Pair<String, Long> d = test.data.get(test.rnd.nextInt(test.data.size()));
                    pool.submit(() -> stat.measure(() -> {
                        long st = System.currentTimeMillis();
                        GetItemResult rs = test.client.getItem(new GetItemRequest()
                            .withTableName(TABLE)
                            .withKey(ImmutableMap.of("id", new AttributeValue(d.a),
                                                    "nid", new AttributeValue().withN(d.b.toString())))
                            .withAttributesToGet("id", "nid", "data")
                            .withConsistentRead(FALSE));
                        stat.add(System.currentTimeMillis() - st);
                        if (!rs.getItem().get("id").getS().equals(d.a) ||
                            !rs.getItem().get("nid").getN().equals(d.b.toString()))
                            throw new RuntimeException("invalid getItem response " + rs);
                    }));
                }
                pool.shutdown();
                if (!pool.awaitTermination(1, TimeUnit.MINUTES))
                    throw new RuntimeException("incorrect pool termination");
                stat.print(nrequests);
            }
        } catch (Exception e) {
            out.println("unhandled exception " + e.getMessage());
            e.printStackTrace();
        } finally {
            test.delete();
        }
    }

    static AmazonDynamoDB connect() throws URISyntaxException {
        String key = System.getenv("AWS_ACCESS_KEY_ID");
        AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
        if (key != null && !key.isEmpty()) {
            Regions region = System.getenv("AWS_REGION") != null ?
                Regions.fromName(System.getenv("AWS_REGION")) : Regions.US_EAST_1;
            out.println("connecting to region " + region + " with key " + key);
            builder
                .withCredentials(new AWSStaticCredentialsProvider(new BasicSessionCredentials(key,
                    System.getenv("AWS_SECRET_ACCESS_KEY"), System.getenv("AWS_SESSION_TOKEN"))))
                .withRegion(region);
        } else {
            String url = "http://localhost:8000";
            out.println("connecting to " + url + " as local/local");
            builder
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("local", "local")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(url, null));
        }
        return builder
            .withClientConfiguration(new ClientConfiguration()
                .withMaxConnections(200)
                .withConnectionMaxIdleMillis(60_000)
                .withConnectionTimeout(30_000)
                .withClientExecutionTimeout(30_000)
                .withConnectionTTL(60_000)
                .withSocketTimeout(30_000)
                .withTcpKeepAlive(true)
                .withThrottledRetries(true)
            )
            .build();
    }

    void create() throws InterruptedException {
        try {
            client.createTable(new CreateTableRequest()
                .withTableName(TABLE)
                .withAttributeDefinitions(
                    new AttributeDefinition().withAttributeName("id").withAttributeType(ScalarAttributeType.S),
                    new AttributeDefinition().withAttributeName("nid").withAttributeType(ScalarAttributeType.N))
                .withKeySchema(
                    new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH),
                    new KeySchemaElement().withAttributeName("nid").withKeyType(KeyType.RANGE))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1000L).withWriteCapacityUnits(1000L)));
            TableStatus status = TableStatus.CREATING;
            do {
                try {
                    Thread.sleep(500);
                    DescribeTableResult rs = client.describeTable(new DescribeTableRequest().withTableName(TABLE));
                    status = TableStatus.valueOf(rs.getTable().getTableStatus());
                    out.print("\rwaiting for ACTIVE ... now " + status + " throughput " +
                        rs.getTable().getProvisionedThroughput().getReadCapacityUnits() + " rcu/" +
                        rs.getTable().getProvisionedThroughput().getWriteCapacityUnits() + " wcu");
                } catch (ResourceNotFoundException | ResourceInUseException ignore) {
                }
            } while (status == TableStatus.CREATING);
            out.println();
        } catch (ResourceInUseException ignored) {
        } catch (AmazonDynamoDBException e) {
            out.println("creating exception " + e.getMessage());
            e.printStackTrace();
        }
    }

    void delete() {
        try {
            client.deleteTable(new DeleteTableRequest().withTableName(TABLE));
        } catch (ResourceNotFoundException ignored) {
        }
        TableStatus status = TableStatus.ACTIVE;
        do {
            try {
                Thread.sleep(500);
                DescribeTableResult response = client.describeTable(new DescribeTableRequest().withTableName(TABLE));
                status = TableStatus.valueOf(response.getTable().getTableStatus());
                out.println("waiting for DELETE ... now " + status);
            } catch (ResourceNotFoundException | ResourceInUseException | InterruptedException e) {
                break;
            }
        } while (status == TableStatus.DELETING || status == TableStatus.ACTIVE);
    }

    void generate() {
        long nid = 0L;
        for (int i = 0; i < 500; i++) {
            List<WriteRequest> requests = new ArrayList<>();
            for (int j = 0; j < 20; j++) {
                Map<String, AttributeValue> item = new HashMap<>();
                String id = UUID.randomUUID().toString();
                item.put("id", new AttributeValue(id));
                item.put("nid", new AttributeValue().withN(Long.toString(nid)));
                item.put("data", new AttributeValue(randomString(rnd.nextInt(100) + 100)));
                requests.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
                data.add(new Pair<>(id, nid));
                nid++;
            }
            client.batchWriteItem(new BatchWriteItemRequest().withRequestItems(ImmutableMap.of(TABLE, requests)));
        }
    }

    void dump() {
        ScanRequest rq = new ScanRequest()
            .withTableName(TABLE)
            .withConsistentRead(TRUE)
            .withAttributesToGet("id", "nid", "data");
        ScanResult rs = client.scan(rq);
        out.println(rs.getCount());
        for (Map<String, AttributeValue> item : rs.getItems())
            out.println(item.get("id").getS() + ", " + item.get("nid").getN() + ": " + item.get("data").getS());
    }

    String randomString(int len) {
        StringBuilder builder = new StringBuilder(len);
        for (int i = 0; i < len; i++) builder.append((char)('a' + rnd.nextInt(26)));
        return builder.toString();
    }

    public void measure(String description, Runnable operation) {
        long started = System.currentTimeMillis();
        try {
            operation.run();
        } finally {
            out.printf(description + ": %dms\n", System.currentTimeMillis() - started);
        }
    }

    static class Statistics {
        final ArrayList<Long> points = new ArrayList<>(10000);
        final long started;
        final String title;
        final AtomicDouble rcu = new AtomicDouble(0);
        final AtomicDouble wcu = new AtomicDouble(0);

        public Statistics(String title) {
            this.started = System.currentTimeMillis();
            this.title = title;
        }

        synchronized void add(long point) { points.add(point); }
        synchronized void add(ConsumedCapacity consumed) {
            if (consumed != null && consumed.getReadCapacityUnits() != null) {
                rcu.addAndGet(consumed.getReadCapacityUnits());
                wcu.addAndGet(consumed.getWriteCapacityUnits());
            }
        }

        synchronized Map<String, Long> percentiles() {
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

        void measure(Runnable operation) {
            long started = System.currentTimeMillis();
            try {
                operation.run();
            } finally {
                add(System.currentTimeMillis() - started);
            }
        }

        void print(int nrequests) {
            out.println(title + " " + nrequests + " total: " + (System.currentTimeMillis() - started) + "ms" +
                "consumed " + rcu + " rcu/" + wcu + " wcu");
            out.println(title + " percentiles: " + percentiles()
                .entrySet().stream().map(e -> e.getKey() + " " + e.getValue() + "ms")
                .collect(Collectors.joining(", ")));
        }
    }
}
