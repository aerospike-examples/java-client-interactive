package com.aerospike.timf.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoop;
import com.aerospike.client.async.NioEventLoops;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpReadFlags;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.mapper.tools.virtuallist.ReturnType;
import com.aerospike.timf.client.Filter;
import com.aerospike.timf.client.MonitoringAerospikeClient;
import com.aerospike.timf.client.RecordingType;
import com.aerospike.timf.client.UiOptions;
import com.aerospike.timf.client.ui.CallbackNotifier;

public class FirstSample implements CallbackNotifier {

    private static final int NUM_INSERT_THREADS = 5;
    private static final int NUM_RW_THREADS = 10;
    private static final long NUM_RECORDS = 1000;
    private static final String NAMESPACE = "test";
    private static final String SET_NAME = "customers";
    private static final AddressGeneratorService addressGenerator = new AddressGeneratorService();
    private static final NameGeneratorService nameGenerator = new NameGeneratorService();
    private static final byte[] bytes = new byte[] {1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,0,1,2,34,5,6,7,8,9,0,1,1,1,1,1,2,2,2,2,3,3,3,3,3,4,4,4,4,4,5,5,5,5,5,6,6,6,6};
    private final AtomicLong recordCount = new AtomicLong(0);
    private final IAerospikeClient client;
    private volatile boolean done = false;
    private final NioEventLoops eventLoops;

    private final WritePolicy writePolicy;
    
    public FirstSample(IAerospikeClient client, NioEventLoops eventLoops) {
        this.eventLoops = eventLoops;
        UiOptions options = new UiOptions(8090, false, this);
        options.getSingleCallRecorderOptions().setShowBatchDetails(true);
        options.getSingleCallRecorderOptions().setShowResultSize(true);
        options.getSingleCallRecorderOptions().setShowStackTrace(true);
        this.client = new MonitoringAerospikeClient(client, EnumSet.of(RecordingType.AGGREGATE), options, new Filter().minTimeInUs(10000));
//        this.client = new MonitoringAerospikeClient(client, EnumSet.noneOf(RecordingType.class), new UiOptions(8092, false, this), new Filter().minTimeInUs(0));
        WritePolicy policy = new WritePolicy(client.getWritePolicyDefault());
        policy.recordExistsAction = RecordExistsAction.REPLACE;
        policy.maxRetries = 3;
        policy.setTimeouts(200, 5000);
        this.writePolicy = policy;
    }
    
    private void insertRecord(long id) {
        try {
            Random random = ThreadLocalRandom.current();
            Map<String, String> address = new HashMap<>(6);
            address.put("line1", addressGenerator.getAddressLine1());
            address.put("suburb", addressGenerator.getSuburb());
            address.put("state", addressGenerator.getState());
            address.put("zip", addressGenerator.getZipCode());
            client.put(this.writePolicy, new Key(NAMESPACE, SET_NAME, id), 
                    new Bin("amount", random.nextInt(2000)*5),
                    new Bin("date", new Date().getTime()),
                    new Bin("custId", "12345"),
                    new Bin("merchAddr", address),
                    new Bin("name", nameGenerator.getName()),
                    new Bin("termId", random.nextInt(100000000)),
                    new Bin("src", "INTERNET"),
                    new Bin("payload", bytes)
                );
        }
        catch (Exception e) {
            System.out.println("Exception caught: " +  e.getMessage());
            e.printStackTrace();
        }
        catch (Error ee) {
            System.out.println("ERROR" + ee);
        }
    }
    
    private void readRecord(long id) {
        client.get(null, new Key(NAMESPACE, SET_NAME, id));
    }
    
    private void testCase() {
        String s = "[graph_type: \"MIG\"\r\n"
                + "oti_file_id: 19\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-08-08 00:00:00.000\"\r\n"
                + ", graph_type: \"MIG\"\r\n"
                + "oti_file_id: 15\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2246939/obs_date=2023-07-11\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-07-11 00:00:00.000\"\r\n"
                + ", graph_type: \"MIG\"\r\n"
                + "oti_file_id: 10\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2200322/obs_date=2023-06-20\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-06-20 00:00:00.000\"\r\n"
                + ", graph_type: \"MIG\"\r\n"
                + "oti_file_id: 9\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2179179/obs_date=2023-06-06\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-06-06 00:00:00.000\"\r\n"
                + ", graph_type: \"MIG\"\r\n"
                + "oti_file_id: 4\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2149336/obs_date=2023-05-16\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-05-16 00:00:00.000\"\r\n"
                + ", graph_type: \"MIG\"\r\n"
                + "oti_file_id: 1\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2104532/obs_date=2023-04-18\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-04-18 00:00:00.000\"\r\n"
                + "]";
        Bin stringBin = new Bin("dataStr", s);
        
        List<String> strings = new ArrayList<>();
        strings.add("graph_type: \"MIG\"\r\n"
                + "oti_file_id: 19\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-08-08 00:00:00.000\r\n");
        strings.add("graph_type: \"MIG\"\r\n"
                + "oti_file_id: 15\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2246939/obs_date=2023-07-11\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-07-11 00:00:00.000\r\n");
        strings.add("graph_type: \"MIG\"\r\n"
                + "oti_file_id: 10\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2200322/obs_date=2023-06-20\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-06-20 00:00:00.000\r\n");
        strings.add("graph_type: \"MIG\"\r\n"
                + "oti_file_id: 9\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2179179/obs_date=2023-06-06\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-06-06 00:00:00.000\r\n");
        strings.add("graph_type: \"MIG\"\r\n"
                + "oti_file_id: 4\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2149336/obs_date=2023-05-16\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-05-16 00:00:00.000\r\n");
        strings.add("graph_type: \"MIG\"\r\n"
                + "oti_file_id: 1\r\n"
                + "data_asset_id: 3717\r\n"
                + "data_asset_format_id: 3717\r\n"
                + "catalog_dir_name: \"gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2104532/obs_date=2023-04-18\"\r\n"
                + "catalog_file_name: \"\"\r\n"
                + "graph_status_cd: \"A\"\r\n"
                + "file_observation_dt: \"2023-04-18 00:00:00.000\r\n");
        Bin stringListBin = new Bin("dataListStr", strings);
        
        List<Map<String, Object>> dataList = new ArrayList<>();
        Map<String, Object> map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        map = new HashMap<>();
        map.put("graph_type", "MIG");
        map.put("oti_file_id", 19);
        map.put("data_asset_id", 3717);
        map.put("data_asset_format_id", 3717);
        map.put("catalog_dir_name", "gs://aed74db8-2be6-47bc-b8a9-ab69241f5950-3717-a/version=100/instance_id=2294762/obs_date=2023-08-08");
        map.put("catalog_file_name", "");
        map.put("graph_status_cd", "A");
        map.put("file_observation_dt", "2023-08-08 00:00:00.000");
        dataList.add(map);
        Bin listMapBin = new Bin("data", dataList);
        Key key = new Key(NAMESPACE, "oti_graph", 1);
        client.put(null, key, stringBin, stringListBin, listMapBin);
        client.get(null, key);
        client.get(null, key, "dataStr");
        client.get(null, key, "dataListStr");
        client.get(null, key, "data");
    }
    
    public void loadData() {
        testCase();
        this.recordCount.set(0);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_INSERT_THREADS);
        for (int i = 0; i < NUM_INSERT_THREADS; i++) {
            executor.submit(() -> {
                while (true) {
                    try {
                        long id = recordCount.getAndIncrement();
                        if (id <= NUM_RECORDS) {
                            insertRecord(id);
                        }
                        else {
                            break;
                        }
                    }
                    catch (Exception e) {
                        System.err.println("Error inserting: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            });
        }
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        
        
        client.operate(null, new Key(NAMESPACE, SET_NAME, 10), 
                Operation.add(new Bin("date", 1)),
                Operation.get("custId"),
                MapOperation.put(MapPolicy.Default, "map", Value.get("bob"), Value.get("fred")),
                ListOperation.getByIndexRange("data", 0, 1000, ListReturnType.VALUE, new CTX[0])
//                Operation.put(new Bin("obj", done))
            );
    }
    
    private void runAsyncSingle(final NioEventLoop loop, int i) {
        Random random = ThreadLocalRandom.current();
        client.put(loop, new WriteListener() {
            
            @Override
            public void onSuccess(Key key) {
                client.get(loop, new RecordListener() {
                    
                    @Override
                    public void onSuccess(Key key, Record record) {
                        client.get(loop, new RecordArrayListener() {
                            
                            @Override
                            public void onSuccess(Key[] keys, Record[] records) {
                                // TODO Auto-generated method stub
                                
                            }
                            
                            @Override
                            public void onFailure(AerospikeException ae) {
                                // TODO Auto-generated method stub
                                
                            }
                        }, client.getBatchPolicyDefault(), new Key[] {key, key, key, key});
                    }
                    @Override
                    public void onFailure(AerospikeException exception) {
                        exception.printStackTrace();
                    }
                }, writePolicy, key);
            }
            
            @Override
            public void onFailure(AerospikeException exception) {
                exception.printStackTrace();
            }
        }, null,
        new Key(NAMESPACE, SET_NAME, NUM_RECORDS+i),
        new Bin("amount", random.nextInt(2000)*5),
        new Bin("date", new Date().getTime()),
        new Bin("custId", "12345"),
        new Bin("name", nameGenerator.getName()),
        new Bin("termId", random.nextInt(100000000)),
        new Bin("src", "INTERNET"),
        new Bin("payload", bytes));
    }
    
    private void runAsyncWorkload(int eventLoopSize) {
        for (int i = 0; i < 80; i++) {
            runAsyncSingle(eventLoops.next(), i);
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
        for (int i = 0; i < 20; i++) {
            runAsyncSingle(eventLoops.next(), i);
        }
    }

    public void finish() {
        this.done = true;
    }
    public void runWorkload(int numThreads, int runTime) {
        // Do a batch load
        Key[] keys = new Key[21];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new Key(NAMESPACE, SET_NAME, i);
        }
        keys[20] = new Key(NAMESPACE, SET_NAME, 100000000);
        Record[] results = client.get(null, keys);
        System.out.println("application received: " + results);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
//        System.exit(0);
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                Random random = ThreadLocalRandom.current();
                while (!done) {
                    try {
                        long id = random.nextInt((int)NUM_RECORDS);
                        int rand = random.nextInt(100);
                        if (rand >= 40) {
                            readRecord(id);
                        }
                        else if (rand >= 10) {
                            insertRecord(id);
                        }
                        else {
                            // Batch get
                            int count = 10;
                            Key[] batchKeys = new Key[count];
                            for (int j = 0; j < count; j++) {
                                batchKeys[j] = new Key(NAMESPACE, SET_NAME, Math.abs(random.nextLong()) % (NUM_RECORDS+20));
                            }
                            client.get(null, batchKeys);
                        }
                    }
                    catch (Exception e) {
                        System.err.println("Error inserting: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            });
        }
        executor.shutdown();
        try {
            if (runTime > 0) {
                Thread.sleep(runTime);
                finish();
            }
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String process(String request) {
        if (!done) {
            System.out.println("Stopping active workload");
            this.done = true;
        }
        else {
            this.done = false;
            System.out.println(request);
            if ("load-data".equals(request)) {
                this.loadData();
            }
            else if ("run-workload".equals(request)) {
                this.runWorkload(NUM_RW_THREADS, 0);
            }
            else if ("run-workload-lower".equals(request)) {
                this.runWorkload(10, 0);
            }
        }
        return "{\"result\" : \"ok\"}";
    }
    
    public void close() {
        this.client.close();
    }
    
    public static RecordSet getRecordsForWorker(IAerospikeClient client, int workerId) {
        int partId = workerId / 2;
        QueryPolicy queryPolicy = new QueryPolicy();
        Statement stmt = new Statement();
        stmt.setNamespace(NAMESPACE);
        stmt.setSetName(SET_NAME);
        queryPolicy.filterExp = Exp.build(Exp.eq(Exp.digestModulo(2), Exp.val(workerId%2)));
        return client.queryPartitions(queryPolicy, stmt, PartitionFilter.id(partId));
    }
    
    public static void readExp(IAerospikeClient client) {
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        Key key = new Key("test", "mapSet", 1);
        client.put(null, key, new Bin("map", map));
        System.out.println(client.operate(null, key, MapOperation.getByIndexRange("map", -3, 10, MapReturnType.KEY)));
    }
    
    public static void accountsSample(IAerospikeClient client) {
        List<Integer> accountList = Arrays.asList(1,2,3,5,7,11,13, 17, 19,23);
        /*
        Key personKey = new Key("test", "customer", 1);
        WritePolicy wp = new WritePolicy(client.getWritePolicyDefault());
        wp.recordExistsAction = RecordExistsAction.REPLACE;
        client.put(wp, personKey, new Bin("name", "Tim"), new Bin("age", 312), new Bin("Accts", accountList));
        for (int i : accountList) {
            Key acctKey = new Key("test", "account", i);
            client.put(wp, acctKey, new Bin("name", "acct-" + i), new Bin("balance", ThreadLocalRandom.current().nextInt(2000)));
        }
        
        */
        // Show accounts with balance > $1000
        Key[] keys = new Key[accountList.size()];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new Key("test", "account", accountList.get(i));
        }
        BatchPolicy batchPolicy = new BatchPolicy(client.getBatchPolicyDefault());
        batchPolicy.filterExp = Exp.build(Exp.gt(Exp.intBin("balance"), Exp.val(1000)));
        Record[] records = client.get(batchPolicy, keys);
        for (Record r : records) {
            if (r != null) {
                System.out.println(r);
            }
        }
    }
    public static void addItem(IAerospikeClient client, Key key, long itemId, String name, double price, int quantity) {
        MapPolicy mp = new MapPolicy(MapOrder.KEY_ORDERED, 0);
        Map<Object, Object> data = new HashMap<>();
        data.put("name", name);
        data.put("price", price);
        data.put("quantity", quantity);
        System.out.println(client.operate(null, key,
                MapOperation.create("items", MapOrder.KEY_ORDERED),
                Operation.get("items"),
                ExpOperation.write("items", Exp.build(
                        Exp.cond(
                            Exp.eq(MapExp.getByKey(MapReturnType.COUNT, Type.INT, Exp.val(itemId), Exp.mapBin("items")), Exp.val(0)),
                            MapExp.put(mp, Exp.val(itemId), Exp.val(data), Exp.mapBin("items")),
                            MapExp.increment(mp, Exp.val("quantity"), Exp.val(quantity), Exp.mapBin("items"), CTX.mapKey(Value.get(itemId)))
                        )
//                        
                ), ExpWriteFlags.DEFAULT),
                Operation.get("items")));
//                ExpOperation.read("items2",
//                        Exp.build(
//                                MapExp.getByKey(MapReturnType.COUNT, Type.LIST, Exp.val(itemId), Exp.mapBin("items"))
////                            Exp.let(
////                                
////                                Exp.def("existingItem", MapExp.getByKey(MapReturnType.VALUE, Type.LIST, Exp.val(itemId), Exp.mapBin("items"))),
////                                Exp.var("existingItem")
////                            )
//                        ),
//                        ExpWriteFlags.DEFAULT)));
    }
	public static void main(String[] args) throws Exception {
	    if (args.length != 1) {
	        System.out.println("Please pass IP address as the parameter");
	        System.exit(-1);
	    }
		Log.setCallbackStandard();
		
		int maxCommandsInProcess = 40;
		int eventLoopSize = Runtime.getRuntime().availableProcessors();
		int concurrentMax = eventLoopSize * maxCommandsInProcess;
		EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.minTimeout = 5000;
        NioEventLoops eventLoops = new NioEventLoops(eventPolicy, eventLoopSize);
        
		ClientPolicy cp = new ClientPolicy();
		cp.eventLoops = eventLoops;
		
		IAerospikeClient client = new AerospikeClient(cp, Host.parseHosts(args[0], 3000));
		
		Map<Integer, Integer> map = new HashMap<>();
		for (int i = 0; i < 100; i++) {
		    
		    int key = ThreadLocalRandom.current().nextInt(10000);
		    int value = ThreadLocalRandom.current().nextInt(1000000);
		    map.put(key, value);
		}
		
		Key key1 = new Key("test", "testSet", 10);
		client.delete(null, key1);
		
		Map aMap = new HashMap<>();
//		aMap.put(1, new ArrayList());
//		client.put(null, key1, new Bin("items", aMap));
		addItem(client, key1, 1, "item100", 27.05, 10);
        addItem(client, key1, 2, "item200", 10.00, 1);
        addItem(client, key1, 2, "item200", 20.00, 2);
//		client.put(null, key1, new Bin("map", map));
//		Record record = client.operate(null, key1,
////		        ExpOperation.read("out2", Exp.build(MapExp.getByIndexRange(MapReturnType.VALUE, Exp.val(0), Exp.val(20), Exp.mapBin("map"))), 0)
//		        ExpOperation.read("output",
//                    Exp.build(ListExp.getByRankRange(ListReturnType.VALUE, Exp.val(0), Exp.val(10),  
//                            MapExp.getByIndexRange(MapReturnType.VALUE, Exp.val(0), Exp.val(20), Exp.mapBin("map")))),
//                    0)
//		        );
//		
//		System.out.println(record);
		accountsSample(client);
		readExp(client);
		WritePolicy writePolicy = new WritePolicy(client.getWritePolicyDefault());
		writePolicy.durableDelete = true;
		
		client.delete(null, key1);
		client.put(null, key1, new Bin("A", 13), new Bin("B", 16), new Bin("C", 23));
		System.out.println(client.get(null, key1));
		
		FirstSample sample = new FirstSample(client, eventLoops);
		sample.loadData();
		sample.runAsyncWorkload(eventLoopSize);
		sample.runWorkload(10, 1000000);
		sample.close();
	}
}
