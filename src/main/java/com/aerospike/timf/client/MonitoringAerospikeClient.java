package com.aerospike.timf.client;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Calendar;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchOperateListListener;
import com.aerospike.client.listener.BatchRecordArrayListener;
import com.aerospike.client.listener.BatchRecordSequenceListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.IndexListener;
import com.aerospike.client.listener.InfoListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchDeletePolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchUDFPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.QueryListener;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.util.Util;
import com.aerospike.timf.client.listeners.AsyncMonitor;
import com.aerospike.timf.client.ui.WebRequestProcessor;

public class MonitoringAerospikeClient implements IAerospikeClient, IMonitorClient {
    private IAerospikeClient delegate;
    private final RecordingNotifier notifier;
    private volatile boolean enabled = true;
    private final PortListener listener;
    private final UiOptions uiOptions;
    private final FailureProfile failureProfile;

    private static interface Invoker<T extends Policy> {
        Object invoke(T policy);
    }

    private static class TimeoutHandler<T extends Policy> {
        private final T policy;
        private long deadline;
        private int iteration = 1;
        private int maxRetries;
        private int totalTimeout;
        private int socketTimeout;

        @SuppressWarnings("unchecked")
        private T clonePolicy(T original, Class<? extends Policy> type) {
            if (original == null) {
                try {
                    return (T) type.getDeclaredConstructor().newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException(e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(e);
                } catch (SecurityException e) {
                    throw new RuntimeException(e);
                }
            } else if (original instanceof WritePolicy) {
                return (T) new WritePolicy(original);
            } else if (original instanceof ScanPolicy) {
                return (T) new ScanPolicy(original);
            } else if (original instanceof QueryPolicy) {
                return (T) new QueryPolicy(original);
            } else if (original instanceof BatchPolicy) {
                return (T) new BatchPolicy(original);
            } else {
                return (T) new Policy(original);
            }
        }

        public TimeoutHandler(T policy, Class<? extends Policy> requiredClass) {
            this.policy = clonePolicy(policy, requiredClass);
            this.maxRetries = this.policy.maxRetries;
            this.totalTimeout = this.policy.totalTimeout;
            this.socketTimeout = this.policy.socketTimeout;
            if (this.policy.totalTimeout > 0) {
                this.deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.policy.totalTimeout);
            }
            // Override the timeout parameters on the socket as we're taking care of them
            // ourselves. This
            // allows the interceptors to behave like the real client would even in the face
            // of a faked timeout
            this.policy.maxRetries = 0;
        }

        public <R> R invoke(Invoker<T> invoker) {
            Log.debug("Pre call");
            AerospikeException exception = null;
            R result = null;
            while (true) {
                try {
                    this.policy.totalTimeout = this.totalTimeout;
                    this.policy.socketTimeout = this.socketTimeout;
                    result = (R) invoker.invoke(this.policy);
                    Log.debug("post call - successful");
                    return result;
                } catch (AerospikeException ae) {
                    exception = ae;
                    if (ae.getResultCode() != ResultCode.TIMEOUT && ae.getResultCode() != ResultCode.DEVICE_OVERLOAD) {
                        Log.debug("post call - failed with " + ae.getClass().getCanonicalName());
                        throw ae;
                    }
                }
                if (iteration > maxRetries) {
                    break;
                }
                if (totalTimeout > 0) {
                    long remainingNs = deadline - System.nanoTime()
                            - TimeUnit.MILLISECONDS.toNanos(policy.sleepBetweenRetries);
                    if (remainingNs <= 0) {
                        break;
                    }
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(remainingNs);
                    if (remainingMs < totalTimeout) {
                        totalTimeout = (int) remainingMs;
                        if (socketTimeout > totalTimeout) {
                            socketTimeout = totalTimeout;
                        }
                    }
                }
                if (policy.sleepBetweenRetries > 0) {
                    Util.sleep(policy.sleepBetweenRetries);
                }
                iteration++;
            }
            Log.debug("post call - failed after exhausting retries with " + exception.getClass().getCanonicalName());
            throw exception;
        }
    }

    public MonitoringAerospikeClient(IAerospikeClient client) {
        this.delegate = client;
        this.notifier = new RecordingNotifier(new UiOptions(8090));
        TimingUtility.setNotifier(this.notifier);
        this.listener = null;
        this.uiOptions = null;
        this.failureProfile = new FailureProfile();
    }

    public MonitoringAerospikeClient(IAerospikeClient client, EnumSet<RecordingType> recordingType,
            UiOptions uiOptions) {
        this(client, recordingType, uiOptions, null);
    }

    public MonitoringAerospikeClient(IAerospikeClient client, EnumSet<RecordingType> recordingType, UiOptions uiOptions,
            Filter filter) {
        this.delegate = client;
        this.notifier = new RecordingNotifier(uiOptions);
        TimingUtility.setNotifier(this.notifier);
        this.setRecordingType(recordingType);
        this.setFilter(filter);
        this.uiOptions = uiOptions;
        this.failureProfile = new FailureProfile(); // TODO: Pass this as a parameter

        if (uiOptions != null && uiOptions.getPort() > 0) {
            Log.info("Starting UI server on port " + uiOptions.getPort());
            listener = new PortListener();
            try {
                listener.start(uiOptions.getPort(),
                        new WebRequestProcessor(client, notifier, uiOptions.getCallbackNotifier(), this));
            } catch (Exception e) {
                Log.error(String.format("UI server failed to start. Error thrown was %s (%s)", e.getMessage(),
                        e.getClass().getName()));
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                Log.error(sw.toString());
            }
        } else {
            this.listener = null;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean setEnabled(boolean enabled) {
        boolean oldValue = this.enabled;
        this.enabled = enabled;
        this.notifier.setEnabled(enabled);
        return oldValue;
    }

    public void enable() {
        this.setEnabled(true);
        ;
    }

    public void disable() {
        this.setEnabled(false);
    }

    public void setFilter(Filter filter) {
        this.notifier.setFilter(filter);
    }

    public void setRecordingType(EnumSet<RecordingType> recordingType) {
        this.notifier.setRecordingType(recordingType);
    }

    @Override
    public EnumSet<RecordingType> getRecordingType() {
        return this.notifier.getRecordingType();
    }

    @Override
    public void setRecordingTypeOptions(RecordingType recordingType, Map<String, String> options) {
        this.notifier.setRecordingTypeOptions(recordingType, options);
    }

    /* --------------- Proxied Methods ----------------------- */

    public Policy getReadPolicyDefault() {
        return delegate.getReadPolicyDefault();
    }

    public WritePolicy getWritePolicyDefault() {
        return delegate.getWritePolicyDefault();
    }

    public ScanPolicy getScanPolicyDefault() {
        return delegate.getScanPolicyDefault();
    }

    public QueryPolicy getQueryPolicyDefault() {
        return delegate.getQueryPolicyDefault();
    }

    public BatchPolicy getBatchPolicyDefault() {
        return delegate.getBatchPolicyDefault();
    }

    public BatchPolicy getBatchParentPolicyWriteDefault() {
        return getBatchParentPolicyWriteDefault();
    }

    public BatchWritePolicy getBatchWritePolicyDefault() {
        return delegate.getBatchWritePolicyDefault();
    }

    public BatchDeletePolicy getBatchDeletePolicyDefault() {
        return delegate.getBatchDeletePolicyDefault();
    }

    public BatchUDFPolicy getBatchUDFPolicyDefault() {
        return delegate.getBatchUDFPolicyDefault();
    }

    public InfoPolicy getInfoPolicyDefault() {
        return delegate.getInfoPolicyDefault();
    }

    public void close() {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("close()");
            try {
                delegate.close();
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.close();
        }
        if (this.listener != null && this.uiOptions != null && this.uiOptions.isTerminateOnClose()) {
            this.listener.close();
        }
    }

    public boolean isConnected() {
        return delegate.isConnected();
    }

    public Node[] getNodes() {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getNodes()");
            try {
                return utility.end(delegate.getNodes());
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getNodes();
        }
    }

    public List<String> getNodeNames() {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getNodeNames()");
            try {
                return utility.end(delegate.getNodeNames());
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getNodeNames();
        }
    }

    public Node getNode(String nodeName) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getNode(String nodeName)");
            try {
                return utility.end(delegate.getNode(nodeName));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getNode(nodeName);
        }
    }

    public ClusterStats getClusterStats() {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getClusterStats()");
            try {
                return utility.end(delegate.getClusterStats());
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getClusterStats();
        }
    }

    public Cluster getCluster() {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getCluster()");
            try {
                return utility.end(delegate.getCluster());
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getCluster();
        }
    }

    public void put(WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("put(WritePolicy policy, Key key, Bin... bins)", policy,
                    key, bins);
            try {
                delegate.put(policy, key, bins);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.put(policy, key, bins);
        }
    }

    public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)",
                    eventLoop, listener, policy, key, bins);
            try {
                delegate.put(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, bins);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.put(eventLoop, listener, policy, key, bins);
        }
    }

    public void append(WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("append(WritePolicy policy, Key key, Bin... bins)",
                    policy, key, bins);
            try {
                delegate.append(policy, key, bins);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.append(policy, key, bins);
        }
    }

    public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)",
                    eventLoop, listener, policy, key, bins);
            try {
                delegate.append(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, bins);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.append(eventLoop, listener, policy, key, bins);
        }
    }

    public void prepend(WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("prepend(WritePolicy policy, Key key, Bin... bins)",
                    policy, key, bins);
            try {
                delegate.prepend(policy, key, bins);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.prepend(policy, key, bins);
        }
    }

    public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)",
                    eventLoop, listener, policy, key, bins);
            try {
                delegate.prepend(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, bins);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.prepend(eventLoop, listener, policy, key, bins);
        }
    }

    public void add(WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("add(WritePolicy policy, Key key, Bin... bins)", policy,
                    key, bins);
            try {
                if (failureProfile.isEnabled()) {
                    delegate.add(policy, key, bins);
                } else {
                    new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
                        failureProfile.preWriteTxn();
                        delegate.add(newPolicy, key, bins);
                        failureProfile.postWriteTxn();
                        return null;
                    });
                }
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.add(policy, key, bins);
        }
    }

    public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins)",
                    eventLoop, listener, policy, key, bins);
            try {
                delegate.add(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, bins);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.add(eventLoop, listener, policy, key, bins);
        }
    }

    public boolean delete(WritePolicy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("delete(WritePolicy policy, Key key)", policy, key);
            try {
                return utility.end(delegate.delete(policy, key));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.delete(policy, key);
        }
    }

    public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key)",
                            eventLoop, listener, policy, key);
            try {
                delegate.delete(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.delete(eventLoop, listener, policy, key);
        }
    }

    public BatchResults delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "delete(BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)", 
                    batchPolicy, deletePolicy, keys);
            try {
                return utility.end(delegate.delete(batchPolicy, deletePolicy, keys));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.delete(batchPolicy, deletePolicy, keys);
        }
    }

    public void delete(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
            BatchDeletePolicy deletePolicy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "delete(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)",
                    eventLoop, listener, batchPolicy, deletePolicy, keys);
            try {
                delegate.delete(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), batchPolicy, deletePolicy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.delete(eventLoop, listener, batchPolicy, deletePolicy, keys);
        }
    }

    public void delete(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
            BatchDeletePolicy deletePolicy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "delete(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy, BatchDeletePolicy deletePolicy, Key[] keys)",
                    eventLoop, listener, batchPolicy, deletePolicy, keys);
            try {
                delegate.delete(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), batchPolicy, deletePolicy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.delete(eventLoop, listener, batchPolicy, deletePolicy, keys);
        }
    }

    public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate)", policy, ns, set,
                    beforeLastUpdate);
            try {
                delegate.truncate(policy, ns, set, beforeLastUpdate);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.truncate(policy, ns, set, beforeLastUpdate);
        }
    }

    public void touch(WritePolicy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("touch(WritePolicy policy, Key key)", policy, key);
            try {
                delegate.touch(policy, key);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.touch(policy, key);
        }
    }

    public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key)",
                            eventLoop, listener, policy, key);
            try {
                delegate.touch(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.touch(eventLoop, listener, policy, key);
        }
    }

    public boolean exists(Policy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("exists(Policy policy, Key key)", policy, key);
            try {
                return utility.end(delegate.exists(policy, key));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.exists(policy, key);
        }
    }

    public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key)",
                            eventLoop, listener, policy, key);
            try {
                delegate.exists(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.exists(eventLoop, listener, policy, key);
        }
    }

    public boolean[] exists(BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("exists(BatchPolicy policy, Key[] keys)", policy, keys);
            try {
                return utility.end(delegate.exists(policy, keys));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.exists(policy, keys);
        }
    }

    public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys)",
                            eventLoop, listener, policy, keys);
            try {
                delegate.exists(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.exists(eventLoop, listener, policy, keys);
        }
    }

    public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys)",
                    eventLoop, listener, policy, keys);
            try {
                delegate.exists(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.exists(eventLoop, listener, policy, keys);
        }
    }

    public Record get(Policy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("get(Policy policy, Key key)", policy, key);
            try {
                return utility.end(delegate.get(policy, key));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.get(policy, key);
        }
    }

    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)",
                            eventLoop, listener, policy, key);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, key);
        }
    }

    public Record get(Policy policy, Key key, String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("get(Policy policy, Key key, String... binNames)", policy,
                    key, binNames);
            try {
                return utility.end(delegate.get(policy, key, binNames));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.get(policy, key, binNames);
        }
    }

    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames)",
                    eventLoop, listener, policy, key, binNames);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, binNames);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, key, binNames);
        }
    }

    public Record getHeader(Policy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getHeader(Policy policy, Key key)", policy, key);
            try {
                return utility.end(delegate.getHeader(policy, key));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getHeader(policy, key);
        }
    }

    public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key)",
                            eventLoop, listener, policy, key);
            try {
                delegate.getHeader(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.getHeader(eventLoop, listener, policy, key);
        }
    }

    public boolean get(BatchPolicy policy, List<BatchRead> records) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("get(BatchPolicy policy, List<BatchRead> records)",
                    policy, records);
            try {
                return utility.end(delegate.get(policy, records));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.get(policy, records);
        }
    }

    public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records)",
                    eventLoop, listener, policy, records);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, records);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, records);
        }
    }

    public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records)",
                    eventLoop, listener, policy, records);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, records);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, records);
        }
    }

    public Record[] get(BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("get(BatchPolicy policy, Key[] keys)", policy, keys);
            try {
                return utility.end(delegate.get(policy, keys));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.get(policy, keys);
        }
    }

    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)",
                            eventLoop, listener, policy, keys);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, keys);
        }
    }

    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)",
                            eventLoop, listener, policy, keys);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, keys);
        }
    }

    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("get(BatchPolicy policy, Key[] keys, String... binNames)",
                    policy, keys, binNames);
            try {
                return utility.end(delegate.get(policy, keys, binNames));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.get(policy, keys, binNames);
        }
    }

    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys,
            String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames)",
                    eventLoop, listener, policy, keys, binNames);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys, binNames);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, keys, binNames);
        }
    }

    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
            String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames)",
                    eventLoop, listener, policy, keys, binNames);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys, binNames);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, keys, binNames);
        }
    }

    public Record[] get(BatchPolicy policy, Key[] keys, Operation... ops) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("get(BatchPolicy policy, Key[] keys, Operation... ops)",
                    policy, keys, ops);
            try {
                return utility.end(delegate.get(policy, keys, ops));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.get(policy, keys, ops);
        }
    }

    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys,
            Operation... ops) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, Operation... ops)",
                    eventLoop, listener, policy, keys, ops);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys, ops);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, keys, ops);
        }
    }

    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys,
            Operation... ops) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, Operation... ops)",
                    eventLoop, listener, policy, keys, ops);
            try {
                delegate.get(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys, ops);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.get(eventLoop, listener, policy, keys, ops);
        }
    }

    public Record[] getHeader(BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("getHeader(BatchPolicy policy, Key[] keys)", policy,
                    keys);
            try {
                return utility.end(delegate.getHeader(policy, keys));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.getHeader(policy, keys);
        }
    }

    public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys)",
                    eventLoop, listener, policy, keys);
            try {
                delegate.getHeader(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.getHeader(eventLoop, listener, policy, keys);
        }
    }

    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys)",
                    eventLoop, listener, policy, keys);
            try {
                delegate.getHeader(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, keys);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.getHeader(eventLoop, listener, policy, keys);
        }
    }

    public Record operate(WritePolicy policy, Key key, Operation... operations) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("operate(WritePolicy policy, Key key, Operation... operations)", policy, key, operations);
            try {
                if (!failureProfile.isEnabled()) {
                    return utility.end(delegate.operate(policy, key, operations));
                }
                Record r = new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
                    failureProfile.preWriteTxn();
                    Record result = delegate.operate(newPolicy, key, operations);
                    failureProfile.postWriteTxn();
                    return result;
                });
                return utility.end(r);
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.operate(policy, key, operations);
        }
    }

    public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key,
            Operation... operations) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations)",
                    eventLoop, listener, policy, key, operations);
            try {
                if (!failureProfile.isEnabled()) {
                    delegate.operate(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, operations);
                    utility.markSubmissionTime();
                }
                else {
                    new TimeoutHandler<WritePolicy>(policy, WritePolicy.class).invoke((newPolicy) -> {
                        failureProfile.preWriteTxn();
                        delegate.operate(eventLoop, listener, policy, key, operations);
                        failureProfile.postWriteTxn();
                        return null;
                    });
                }
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.operate(eventLoop, listener, policy, key, operations);
        }
    }

    public boolean operate(BatchPolicy policy, List<BatchRecord> records) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("operate(BatchPolicy policy, List<BatchRecord> records)",
                    policy, records);
            try {
                return utility.end(delegate.operate(policy, records));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.operate(policy, records);
        }
    }

    public void operate(EventLoop eventLoop, BatchOperateListListener listener, BatchPolicy policy,
            List<BatchRecord> records) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "operate(EventLoop eventLoop, BatchOperateListListener listener, BatchPolicy policy, List<BatchRecord> records)",
                    eventLoop, listener, policy, records);
            try {
                delegate.operate(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, records);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.operate(eventLoop, listener, policy, records);
        }
    }

    public void operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy policy,
            List<BatchRecord> records) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy policy, List<BatchRecord> records)",
                    eventLoop, listener, policy, records);
            try {
                delegate.operate(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, records);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.operate(eventLoop, listener, policy, records);
        }
    }

    public BatchResults operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, Operation... ops) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "operate(BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, Operation... ops)",
                    batchPolicy, writePolicy, keys, ops);
            try {
                return utility.end(delegate.operate(batchPolicy, writePolicy, keys, ops));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.operate(batchPolicy, writePolicy, keys, ops);
        }
    }

    public void operate(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
            BatchWritePolicy writePolicy, Key[] keys, Operation... ops) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "operate(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, Operation... ops)",
                    eventLoop, listener, batchPolicy, writePolicy, keys, ops);
            try {
                delegate.operate(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), batchPolicy, writePolicy, keys, ops);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.operate(eventLoop, listener, batchPolicy, writePolicy, keys, ops);
        }
    }

    public void operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
            BatchWritePolicy writePolicy, Key[] keys, Operation... ops) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "operate(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy, BatchWritePolicy writePolicy, Key[] keys, Operation... ops)",
                    eventLoop, listener, batchPolicy, writePolicy, keys, ops);
            try {
                delegate.operate(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), batchPolicy, writePolicy, keys, ops);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.operate(eventLoop, listener, batchPolicy, writePolicy, keys, ops);
        }
    }

    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback,
            String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames)",
                    policy, namespace, setName, callback, binNames);
            try {
                delegate.scanAll(policy, namespace, setName, callback, binNames);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.scanAll(policy, namespace, setName, callback, binNames);
        }
    }

    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace,
            String setName, String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames)",
                    eventLoop, listener, policy, namespace, setName, binNames);
            try {
                delegate.scanAll(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, namespace, setName, binNames);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.scanAll(eventLoop, listener, policy, namespace, setName, binNames);
        }
    }

    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback,
            String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames)",
                    policy, nodeName, namespace, setName, callback, binNames);
            try {
                delegate.scanNode(policy, nodeName, namespace, setName, callback, binNames);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.scanNode(policy, nodeName, namespace, setName, callback, binNames);
        }
    }

    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback,
            String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames)",
                    policy, node, namespace, setName, callback, binNames);
            try {
                delegate.scanNode(policy, node, namespace, setName, callback, binNames);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.scanNode(policy, node, namespace, setName, callback, binNames);
        }
    }

    public void scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName,
            ScanCallback callback, String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "scanPartitions(ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName, ScanCallback callback, String... binNames)",
                    policy, partitionFilter, namespace, setName, callback, binNames);
            try {
                delegate.scanPartitions(policy, partitionFilter, namespace, setName, callback, binNames);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.scanPartitions(policy, partitionFilter, namespace, setName, callback, binNames);
        }
    }

    public void scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy,
            PartitionFilter partitionFilter, String namespace, String setName, String... binNames) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "scanPartitions(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, PartitionFilter partitionFilter, String namespace, String setName, String... binNames)",
                    eventLoop, listener, policy, partitionFilter, namespace, setName, binNames);
            try {
                delegate.scanPartitions(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, partitionFilter, namespace, setName, binNames);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.scanPartitions(eventLoop, listener, policy, partitionFilter, namespace, setName, binNames);
        }
    }

    public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "register(Policy policy, String clientPath, String serverPath, Language language)", policy,
                    clientPath, serverPath, language);
            try {
                return utility.end(delegate.register(policy, clientPath, serverPath, language));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.register(policy, clientPath, serverPath, language);
        }
    }

    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath,
            Language language) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language)",
                    policy, resourceLoader, resourcePath, serverPath, language);
            try {
                return utility.end(delegate.register(policy, resourceLoader, resourcePath, serverPath, language));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.register(policy, resourceLoader, resourcePath, serverPath, language);
        }
    }

    public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "registerUdfString(Policy policy, String code, String serverPath, Language language)", policy, code,
                    serverPath, language);
            try {
                return utility.end(delegate.registerUdfString(policy, code, serverPath, language));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.registerUdfString(policy, code, serverPath, language);
        }
    }

    public void removeUdf(InfoPolicy policy, String serverPath) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("removeUdf(InfoPolicy policy, String serverPath)", policy,
                    serverPath);
            try {
                delegate.removeUdf(policy, serverPath);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.removeUdf(policy, serverPath);
        }
    }

    public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args)",
                    policy, key, packageName, functionName, args);
            try {
                return utility.end(delegate.execute(policy, key, packageName, functionName, args));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.execute(policy, key, packageName, functionName, args);
        }
    }

    public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName,
            String functionName, Value... functionArgs) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs)",
                    eventLoop, listener, policy, key, packageName, functionName, functionArgs);
            try {
                delegate.execute(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, key, packageName, functionName, functionArgs);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.execute(eventLoop, listener, policy, key, packageName, functionName, functionArgs);
        }
    }

    public BatchResults execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, String packageName,
            String functionName, Value... functionArgs) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs)");
            try {
                return utility
                        .end(delegate.execute(batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.execute(batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
        }
    }

    public void execute(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy,
            BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(EventLoop eventLoop, BatchRecordArrayListener listener, BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs)",
                    eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
            try {
                delegate.execute(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), batchPolicy, udfPolicy, keys, packageName, functionName,
                        functionArgs);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.execute(eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName,
                    functionArgs);
        }
    }

    public void execute(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy,
            BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(EventLoop eventLoop, BatchRecordSequenceListener listener, BatchPolicy batchPolicy, BatchUDFPolicy udfPolicy, Key[] keys, String packageName, String functionName, Value... functionArgs)",
                    eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName, functionArgs);
            try {
                delegate.execute(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), batchPolicy, udfPolicy, keys, packageName, functionName,
                        functionArgs);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.execute(eventLoop, listener, batchPolicy, udfPolicy, keys, packageName, functionName,
                    functionArgs);
        }
    }

    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName,
            Value... functionArgs) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)",
                    policy, statement, packageName, functionArgs, functionArgs);
            try {
                return utility.end(delegate.execute(policy, statement, packageName, functionName, functionArgs));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.execute(policy, statement, packageName, functionName, functionArgs);
        }
    }

    public ExecuteTask execute(WritePolicy policy, Statement statement, Operation... operations) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "execute(WritePolicy policy, Statement statement, Operation... operations)", policy, statement,
                    operations);
            try {
                return utility.end(delegate.execute(policy, statement, operations));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.execute(policy, statement, operations);
        }
    }

    public RecordSet query(QueryPolicy policy, Statement statement) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("query(QueryPolicy policy, Statement statement)", policy,
                    statement);
            try {
                return utility.end(delegate.query(policy, statement));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.query(policy, statement);
        }
    }

    public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement)",
                    eventLoop, listener, policy, statement);
            try {
                delegate.query(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, statement);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.query(eventLoop, listener, policy, statement);
        }
    }

    public void query(QueryPolicy policy, Statement statement, QueryListener listener) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "query(QueryPolicy policy, Statement statement, QueryListener listener)", policy, statement,
                    listener);
            try {
                delegate.query(policy, statement, listener);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.query(policy, statement, listener);
        }
    }

    public void query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter,
            QueryListener listener) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "query(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter, QueryListener listener)",
                    policy, statement, partitionFilter, listener);
            try {
                delegate.query(policy, statement, partitionFilter, listener);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.query(policy, statement, partitionFilter, listener);
        }
    }

    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("queryNode(QueryPolicy policy, Statement statement, Node node)", policy, statement, node);
            try {
                return utility.end(delegate.queryNode(policy, statement, node));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryNode(policy, statement, node);
        }
    }

    public RecordSet queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "queryPartitions(QueryPolicy policy, Statement statement, PartitionFilter partitionFilter)", policy,
                    statement, partitionFilter);
            try {
                return utility.end(delegate.queryPartitions(policy, statement, partitionFilter));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryPartitions(policy, statement, partitionFilter);
        }
    }

    public void queryPartitions(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy,
            Statement statement, PartitionFilter partitionFilter) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "queryPartitions(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement, PartitionFilter partitionFilter)",
                    eventLoop, listener, policy, statement, partitionFilter);
            try {
                delegate.queryPartitions(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, statement, partitionFilter);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.queryPartitions(eventLoop, listener, policy, statement, partitionFilter);
        }
    }

    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName,
            Value... functionArgs) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs)",
                    policy, statement, packageName, functionName, functionArgs);
            try {
                return utility.end(delegate.queryAggregate(policy, statement, packageName, functionName, functionArgs));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryAggregate(policy, statement, packageName, functionName, functionArgs);
        }
    }

    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("queryAggregate(QueryPolicy policy, Statement statement)",
                    policy, statement);
            try {
                return utility.end(delegate.queryAggregate(policy, statement));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryAggregate(policy, statement);
        }
    }

    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "queryAggregateNode(QueryPolicy policy, Statement statement, Node node)", policy, statement, node);
            try {
                return utility.end(delegate.queryAggregateNode(policy, statement, node));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryAggregateNode(policy, statement, node);
        }
    }

    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
            IndexType indexType) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType)",
                    policy, namespace, setName, indexName, binName, indexType);
            try {
                return utility.end(delegate.createIndex(policy, namespace, setName, indexName, binName, indexType));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType);
        }
    }

    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName,
            IndexType indexType, IndexCollectionType indexCollectionType) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType indexCollectionType)",
                    policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
            try {
                return utility.end(delegate.createIndex(policy, namespace, setName, indexName, binName, indexType,
                        indexCollectionType));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
        }
    }

    public void createIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace,
            String setName, String indexName, String binName, IndexType indexType,
            IndexCollectionType indexCollectionType) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType indexCollectionType)",
                    eventLoop, listener, policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
            try {
                delegate.createIndex(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, namespace, setName, indexName, binName, indexType,
                        indexCollectionType);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.createIndex(eventLoop, listener, policy, namespace, setName, indexName, binName, indexType,
                    indexCollectionType);
        }
    }

    public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "dropIndex(Policy policy, String namespace, String setName, String indexName)", policy, namespace,
                    setName, indexName);
            try {
                return utility.end(delegate.dropIndex(policy, namespace, setName, indexName));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.dropIndex(policy, namespace, setName, indexName);
        }
    }

    public void dropIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace, String setName,
            String indexName) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "dropIndex(EventLoop eventLoop, IndexListener listener, Policy policy, String namespace, String setName, String indexName)",
                    eventLoop, listener, policy, namespace, setName, indexName);
            try {
                delegate.dropIndex(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, namespace, setName, indexName);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.dropIndex(eventLoop, listener, policy, namespace, setName, indexName);
        }
    }

    public void info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "info(EventLoop eventLoop, InfoListener listener, InfoPolicy policy, Node node, String... commands)",
                    eventLoop, listener, policy, node, commands);
            try {
                delegate.info(eventLoop, AsyncMonitor.getInstance().wrap(listener, utility), policy, node, commands);
                utility.markSubmissionTime();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.info(eventLoop, listener, policy, node, commands);
        }
    }

    public void setXDRFilter(InfoPolicy policy, String datacenter, String namespace, Expression filter) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "setXDRFilter(InfoPolicy policy, String datacenter, String namespace, Expression filter)", policy,
                    datacenter, namespace, filter);
            try {
                delegate.setXDRFilter(policy, datacenter, namespace, filter);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.setXDRFilter(policy, datacenter, namespace, filter);
        }
    }

    public void createUser(AdminPolicy policy, String user, String password, List<String> roles) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createUser(AdminPolicy policy, String user, String password, List<String> roles)", policy, user,
                    password, roles);
            try {
                delegate.createUser(policy, user, password, roles);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.createUser(policy, user, password, roles);
        }
    }

    public void dropUser(AdminPolicy policy, String user) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("dropUser(AdminPolicy policy, String user)", policy,
                    user);
            try {
                delegate.dropUser(policy, user);
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.dropUser(policy, user);
        }
    }

    public void changePassword(AdminPolicy policy, String user, String password) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("changePassword(AdminPolicy policy, String user, String password)", policy, user, password);
            try {
                delegate.changePassword(policy, user, password);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.changePassword(policy, user, password);
        }
    }

    public void grantRoles(AdminPolicy policy, String user, List<String> roles) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("grantRoles(AdminPolicy policy, String user, List<String> roles)", policy, user, roles);
            try {
                delegate.grantRoles(policy, user, roles);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.grantRoles(policy, user, roles);
        }
    }

    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) {
        if (enabled) {
            TimingUtility utility = new TimingUtility()
                    .start("revokeRoles(AdminPolicy policy, String user, List<String> roles)", policy, user, roles);
            try {
                delegate.revokeRoles(policy, user, roles);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.revokeRoles(policy, user, roles);
        }
    }

    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createRole(AdminPolicy policy, String roleName, List<Privilege> privileges)", policy, roleName,
                    privileges);
            try {
                delegate.createRole(policy, roleName, privileges);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.createRole(policy, roleName, privileges);
        }
    }

    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist)",
                    policy, roleName, privileges, whitelist);
            try {
                delegate.createRole(policy, roleName, privileges, whitelist);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.createRole(policy, roleName, privileges, whitelist);
        }
    }

    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist,
            int readQuota, int writeQuota) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "createRole(AdminPolicy policy, String roleName, List<Privilege> privileges, List<String> whitelist, int readQuota, int writeQuota)",
                    policy, roleName, privileges, whitelist, readQuota, writeQuota);
            try {
                delegate.createRole(policy, roleName, privileges, whitelist, readQuota, writeQuota);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.createRole(policy, roleName, privileges, whitelist, readQuota, writeQuota);
        }
    }

    public void dropRole(AdminPolicy policy, String roleName) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("dropRole(AdminPolicy policy, String roleName)", policy,
                    roleName);
            try {
                delegate.dropRole(policy, roleName);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.dropRole(policy, roleName);
        }
    }

    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)", policy,
                    roleName, privileges);
            try {
                delegate.grantPrivileges(policy, roleName, privileges);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.grantPrivileges(policy, roleName, privileges);
        }
    }

    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges)", policy,
                    roleName, privileges);
            try {
                delegate.revokePrivileges(policy, roleName, privileges);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.revokePrivileges(policy, roleName, privileges);
        }
    }

    public void setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "setWhitelist(AdminPolicy policy, String roleName, List<String> whitelist)", policy, roleName,
                    whitelist);
            try {
                delegate.setWhitelist(policy, roleName, whitelist);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.setWhitelist(policy, roleName, whitelist);
        }
    }

    public void setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start(
                    "setQuotas(AdminPolicy policy, String roleName, int readQuota, int writeQuota)", policy, roleName,
                    readQuota, writeQuota);
            try {
                delegate.setQuotas(policy, roleName, readQuota, writeQuota);
                utility.end();
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            delegate.setQuotas(policy, roleName, readQuota, writeQuota);
        }
    }

    public User queryUser(AdminPolicy policy, String user) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("queryUser(AdminPolicy policy, String user)", policy,
                    user);
            try {
                return utility.end(delegate.queryUser(policy, user));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryUser(policy, user);
        }
    }

    public List<User> queryUsers(AdminPolicy policy) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("queryUsers(AdminPolicy policy)", policy);
            try {
                return utility.end(delegate.queryUsers(policy));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryUsers(policy);
        }
    }

    public Role queryRole(AdminPolicy policy, String roleName) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("queryRole(AdminPolicy policy, String roleName)", policy,
                    roleName);
            try {
                return utility.end(delegate.queryRole(policy, roleName));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryRole(policy, roleName);
        }
    }

    public List<Role> queryRoles(AdminPolicy policy) {
        if (enabled) {
            TimingUtility utility = new TimingUtility().start("queryRoles(AdminPolicy policy)", policy);
            try {
                return utility.end(delegate.queryRoles(policy));
            } catch (RuntimeException e) {
                throw utility.end(e);
            }
        } else {
            return delegate.queryRoles(policy);
        }
    }

}
