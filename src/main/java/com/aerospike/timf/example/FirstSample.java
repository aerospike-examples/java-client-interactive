package com.aerospike.timf.example;

import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.timf.client.Filter;
import com.aerospike.timf.client.MonitoringAerospikeClient;
import com.aerospike.timf.client.RecordingType;
import com.aerospike.timf.client.UiOptions;
import com.aerospike.timf.client.ui.CallbackNotifier;

public class FirstSample implements CallbackNotifier {

    private static final int NUM_INSERT_THREADS = 2;
    private static final int NUM_RW_THREADS = 10;
    private static final long NUM_RECORDS = 100;
    private static final String NAMESPACE = "test";
    private static final String SET_NAME = "customers";
    private static final AddressGeneratorService addressGenerator = new AddressGeneratorService();
    private static final NameGeneratorService nameGenerator = new NameGeneratorService();
    private static final byte[] bytes = new byte[] {1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,0,1,2,34,5,6,7,8,9,0,1,1,1,1,1,2,2,2,2,3,3,3,3,3,4,4,4,4,4,5,5,5,5,5,6,6,6,6};
    private final AtomicLong recordCount = new AtomicLong(0);
    private final IAerospikeClient client;
    private volatile boolean done = false;

    private final WritePolicy writePolicy;
    
    public FirstSample(IAerospikeClient client) {
        this.client = new MonitoringAerospikeClient(client, EnumSet.of(RecordingType.AGGREGATE), new UiOptions(8090, false, this), new Filter().minTimeInUs(0));
//        this.client = new MonitoringAerospikeClient(client, EnumSet.noneOf(RecordingType.class), new UiOptions(8090, false, this), new Filter().minTimeInUs(0));
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
    
    public void loadData() {
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
                                batchKeys[j] = new Key(NAMESPACE, SET_NAME, random.nextLong(NUM_RECORDS+20));
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
    
	public static void main(String[] args) throws Exception {
	    if (args.length != 1) {
	        System.out.println("Please pass IP address as the parameter");
	        System.exit(-1);
	    }
		Log.setCallbackStandard();
		
		ClientPolicy cp = new ClientPolicy();
		IAerospikeClient client = new AerospikeClient(cp, "172.17.0.2"/*args[0]*/, 3000);

		FirstSample sample = new FirstSample(client);
		sample.loadData();
		sample.runWorkload(10, 1000000);
		sample.close();
	}


}
