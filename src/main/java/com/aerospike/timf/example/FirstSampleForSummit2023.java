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
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.timf.client.MonitoringAerospikeClient;
import com.aerospike.timf.client.RecordingType;
import com.aerospike.timf.client.UiOptions;
import com.aerospike.timf.client.ui.CallbackNotifier;

public class FirstSampleForSummit2023 implements CallbackNotifier {

    private static final int NUM_INSERT_THREADS = 400;
    private static final int NUM_RW_THREADS = 32;
    private static final long NUM_RECORDS = 10_000_000;
    private static final String NAMESPACE = "test";
    private static final String SET_NAME = "txns";
    private static final AddressGeneratorService addressGenerator = new AddressGeneratorService();
    private static final NameGeneratorService nameGenerator = new NameGeneratorService();
    private static final byte[] bytes = new byte[] {1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9,0,1,2,34,5,6,7,8,9,0,1,1,1,1,1,2,2,2,2,3,3,3,3,3,4,4,4,4,4,5,5,5,5,5,6,6,6,6};
    private final AtomicLong recordCount = new AtomicLong(0);
    private final IAerospikeClient client;
    private volatile boolean done = true;

    private final WritePolicy writePolicy;
    
    public FirstSampleForSummit2023(IAerospikeClient client) {
        this.client = new MonitoringAerospikeClient(client, EnumSet.of(RecordingType.AGGREGATE), new UiOptions(8090, this));
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
        client.get(this.writePolicy, new Key(NAMESPACE, SET_NAME, id));
    }
    
    public void loadData() {
        this.recordCount.set(0);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_INSERT_THREADS);
        for (int i = 0; i < NUM_INSERT_THREADS; i++) {
            executor.submit(() -> {
                while (true) {
                    try {
                        long id = recordCount.incrementAndGet();
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
    
    public void runWorkload(int numThreads) {
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                Random random = ThreadLocalRandom.current();
                while (!done) {
                    try {
                        long id = random.nextInt((int)NUM_RECORDS);
                        if (random.nextInt(100) >= 33) {
                            readRecord(id);
                        }
                        else {
                            insertRecord(id);
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
                this.runWorkload(NUM_RW_THREADS);
            }
            else if ("run-workload-lower".equals(request)) {
                this.runWorkload(10);
            }
            else if ("end".equals(request)) {
                synchronized (this) {
                    this.notify();
                }
            }
        }
        return "{\"result\" : \"ok\"}";
    }
    
    public void run() {
        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
            }
        }
    }
    
	public static void main(String[] args) throws Exception {
	    if (args.length != 1) {
	        System.out.println("Please pass IP address as the parameter");
	        System.exit(-1);
	    }
		Log.setCallbackStandard();
		
		ClientPolicy cp = new ClientPolicy();
		cp.useServicesAlternate = false;
		cp.minConnsPerNode = 20;
		cp.maxConnsPerNode = 500;
		IAerospikeClient client = new AerospikeClient(cp, args[0], 3000);
		FirstSampleForSummit2023 sample = new FirstSampleForSummit2023(client);
		sample.loadData();
		client.close();
	}


}
