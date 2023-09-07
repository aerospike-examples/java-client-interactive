package com.aerospike.timf.client;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.policy.WritePolicy;

public class Utils {
    
    private static long estimateSize(List<?> list, Set<Object> visitedObjects, int depth) {
        long size = 0;
        for (Object o : list) {
            size += estimateSize(o, visitedObjects, depth);
        }
        return size;
    }
    private static long estimateSize(Map<?,?> map, Set<Object> visitedObjects, int depth) {
        long size = 0;
        for (Object o : map.keySet()) {
            size += estimateSize(o, visitedObjects, depth) + estimateSize(map.get(o), visitedObjects, depth);
        }
        return size;
    }
    private static long estimateSize(Object[] objs, Set<Object> visitedObjects, int depth) {
        long size = 0;
        for (Object obj : objs) {
            size += estimateSize(obj, visitedObjects, depth);
        }
        return size;
    }
    private static long estimateSizeFromClass(Class<?> clazz) {
        if (Byte.class.equals(clazz) || Character.class.equals(clazz) || Boolean.class.equals(clazz)) {
            return 1;
        }
        else if (Short.class.equals(clazz)) {
            return 2;
        }
        else if (Integer.class.equals(clazz) || Float.class.equals(clazz)) {
            return 4;
        }
        else if (Long.class.equals(clazz) || Double.class.equals(clazz)) {
            return 8;
        }
        return -1;
    }
    
    private static final int RECURSION_LIMIT = 800;
    private static boolean validateRecursionLimitExceeded(int depth, Object obj, Set<Object> visitedObjects) {
        if (depth > RECURSION_LIMIT) {
            System.out.printf("ERROR: Aborting size estimation due to recursion limit! obj=%s, class=%s, visitedObjects.size=%d\n",
                    obj, obj == null ? "null" : obj.getClass().getName(), visitedObjects.size());
            Thread.dumpStack();
            return true;
        }
        return false;
    }
    
    private static long reflectivelyEstimateSize(Object obj, Class<?> clazz, Set<Object> visitedObjects, int depth) {
        if (validateRecursionLimitExceeded(++depth, obj, visitedObjects)) {
            return 0;
        }
        Field[] fields;
        try  {
            fields = clazz.getDeclaredFields();
        }
        catch (Error e) {
            // TODO: Flag a warning here?
            // For some reason, ClassNotFoundException is not caught by a normal exception handler
            // so we have to trap error.
            return 0;
        }
        long size = 0;
        for (Field f : fields) {
            String name = f.getName();
            int modifiers = f.getModifiers();
            if (Modifier.isTransient(modifiers) || Modifier.isStatic(modifiers)) {
                continue;
            }
            try {
                f.setAccessible(true);
                Object child = f.get(obj);
                size += estimateSize(child, visitedObjects, depth);
            }
            catch (Exception ignored) {
            }
        }
        Class<?> superClazz = clazz.getSuperclass();
        if (!(superClazz == null || Object.class.equals(superClazz))) {
            size = reflectivelyEstimateSize(obj, superClazz, visitedObjects, depth);
        }
        return size;
    }
    public static long estimateSize(Object obj) {
        return estimateSize(obj, new HashSet<>(1000), 1);
    }
    
    private static long estimateSize(Object obj, Set<Object> visitedObjects, int depth) {
        if (validateRecursionLimitExceeded(++depth, obj, visitedObjects)) {
            return 0;
        }
        if (obj == null || visitedObjects.contains(obj)) {
            return 0;
        }
        else if (obj instanceof String) {
            // Note that java uses UTF-16 to store bytes, but messagePack used a more
            // concise format. We will use the length as a rough estimate here.
            return ((String)obj).length();
        }
        else if (obj instanceof List) {
            visitedObjects.add(obj);
            return estimateSize((List<?>)obj, visitedObjects, depth);
        }
        else if (obj instanceof Map) {
            visitedObjects.add(obj);
            return estimateSize((Map<?,?>)obj, visitedObjects, depth);
        }
        else if (obj instanceof Byte || obj instanceof Character || obj instanceof Boolean) {
            return 1;
        }
        else if (obj instanceof Short) {
            return (((Short)obj).shortValue() == 0) ? 1 : 2;
        }
        else if (obj instanceof Integer) {
            return (((Integer)obj).intValue() == 0) ? 1 : 4;
        }
        else if (obj instanceof Long) {
            return (((Long)obj).longValue() == 0) ? 1 : 8;
        }
        else if (obj instanceof Float) {
            return (((Float)obj).longValue() == 0) ? 1 : 4;
        }
        else if (obj instanceof Double) {
            return (((Float)obj).longValue() == 0) ? 1 : 8;
        }
        else if (obj instanceof Bin) {
            Bin bin = (Bin)obj;
            return bin.name.length() + bin.value.estimateSize();
        }
        else if (obj instanceof Value) {
            return ((Value)obj).estimateSize();
        }
        else if (obj instanceof Record) {
            Record record = (Record)obj;
            return estimateSize(record.bins, visitedObjects, depth) 
                    + estimateSize(record.expiration, visitedObjects, depth)
                    + estimateSize(record.generation, visitedObjects, depth);
        }
        else {
            Class<?> clazz = obj.getClass();
            if (clazz.isEnum()) {
                // The size of an enum is 4 as it's stored as an integer
                return 4;
            }
            if (clazz.isArray()) {
                Class<?> componentType = clazz.getComponentType();
                if (Byte.TYPE.equals(componentType)) {
                    return ((byte[])obj).length;
                }
                Object[] array = (Object[])obj;
                if (componentType.isPrimitive()) {
                    long instanceSize = estimateSizeFromClass(componentType);
                    if (instanceSize >= 0) {
                        return array.length * instanceSize;
                    }
                    return estimateSize(array, visitedObjects, depth);
                }
                else {
                    long size = 0;
                    for (int i = 0; i < array.length; i++) {
                        size += estimateSize(array[i], visitedObjects, depth);
                    }
                    return size;
                }
            }
            else {
                // Have to use introspection on the object
                visitedObjects.add(obj);
                return reflectivelyEstimateSize(obj, clazz, visitedObjects, depth);
            }
        }
    }
    
    public static String toString(Policy policy) {
        if (policy == null) {
            return "null";
        }
        Policy reference;
        StringBuffer sb = new StringBuffer();
        if (policy instanceof WritePolicy) {
            reference = new WritePolicy();
            sb.append("[Write]");
        }
        else if (policy instanceof BatchPolicy) {
            reference = new BatchPolicy();
            sb.append("[Batch]");
        }
        else if (policy instanceof ScanPolicy) {
            reference = new ScanPolicy();
            sb.append("[Scan]");
        }
        else if (policy instanceof QueryPolicy) {
            reference = new QueryPolicy();
            sb.append("[Query]");
        }
        else {
            reference = new Policy();
        }
        sb.append('{');
        if (policy.connectTimeout != reference.connectTimeout) {
            sb.append(",ct:").append(policy.connectTimeout);
        }
        if (policy.socketTimeout != reference.socketTimeout) {
            sb.append(",st:").append(policy.socketTimeout);
        }
        if (policy.totalTimeout != reference.totalTimeout) {
            sb.append(",tt:").append(policy.totalTimeout);
        }
        if (policy.maxRetries != reference.maxRetries) {
            sb.append(",mr:").append(policy.maxRetries);
        }
        if (policy.timeoutDelay != reference.timeoutDelay) {
            sb.append(",td:").append(policy.timeoutDelay);
        }
        if (policy.sleepBetweenRetries != reference.sleepBetweenRetries) {
            sb.append(",sbr:").append(policy.sleepBetweenRetries);
        }
        if (policy.sendKey != reference.sendKey) {
            sb.append(",sk:").append(policy.sendKey);
        }
        if (policy.compress != reference.compress) {
            sb.append(",compress:").append(policy.compress);
        }
        if (policy.readModeAP != reference.readModeAP) {
            sb.append(",rm-ap:").append(policy.readModeAP);
        }
        if (policy.readModeSC != reference.readModeSC) {
            sb.append(",rm-sc:").append(policy.readModeSC);
        }
        if (policy.replica != reference.replica) {
            sb.append(",replica:").append(policy.replica);
        }
        if (policy.filterExp != null) {
            sb.append(",filterExp:<filter>");
        }
        if (policy instanceof WritePolicy) {
            WritePolicy wp = (WritePolicy)policy;
            WritePolicy wpRef = (WritePolicy)reference;
            if (wpRef.recordExistsAction != wp.recordExistsAction) {
                sb.append(",rea:").append(wp.recordExistsAction);
            }
            if (wpRef.generationPolicy != wp.generationPolicy) {
                sb.append(",genPol:").append(wp.generationPolicy);
            }
            if (wpRef.generation != wp.generation) {
                sb.append(",gen:").append(wp.generation);
            }
            if (wp.commitLevel != wpRef.commitLevel) {
                sb.append(",commit:").append(wp.commitLevel);
            }
            if (wp.expiration != wpRef.expiration) {
                sb.append(",expiry:").append(wp.expiration);
            }
            if (wp.respondAllOps != wpRef.respondAllOps) {
                sb.append(",respAllOps:").append(wp.respondAllOps);
            }
            if (wp.durableDelete != wpRef.durableDelete) {
                sb.append(",dd:").append(wp.durableDelete);
            }
        }
        else if (policy instanceof BatchPolicy) {
            BatchPolicy bp = (BatchPolicy)policy;
            BatchPolicy bpRef = (BatchPolicy)reference;
            if (bp.maxConcurrentThreads != bpRef.maxConcurrentThreads) {
                sb.append(",max_ct:").append(bp.maxConcurrentThreads);
            }
            if (bp.allowInline != bpRef.allowInline) {
                sb.append(",allowInline:").append(bp.allowInline);
            }
            if (bp.allowInlineSSD != bpRef.allowInlineSSD) {
                sb.append(",allowInlineSSD:").append(bp.allowInlineSSD);
            }
            if (bp.respondAllKeys != bpRef.respondAllKeys) {
                sb.append(",respAllKys:").append(bp.respondAllKeys);
            }
        }
        else if (policy instanceof QueryPolicy) {
            QueryPolicy qp = (QueryPolicy)policy;
            QueryPolicy qpRef = (QueryPolicy)reference;
            if (qp.maxConcurrentNodes != qpRef.maxConcurrentNodes) {
                sb.append(",maxConcNodes:").append(qp.maxConcurrentNodes);
            }
            if (qp.recordQueueSize != qpRef.recordQueueSize) {
                sb.append(",recQueSize:").append(qp.recordQueueSize);
            }
            if (qp.includeBinData != qpRef.includeBinData) {
                sb.append(",incBinData:").append(qp.includeBinData);
            }
            if (qp.failOnClusterChange != qpRef.failOnClusterChange) {
                sb.append(",failonCC:").append(qp.failOnClusterChange);
            }
            if (qp.shortQuery != qpRef.shortQuery) {
                sb.append(",shortQuery:").append(qp.shortQuery);
            }
        }
        else if (policy instanceof ScanPolicy) {
            ScanPolicy sp = (ScanPolicy)policy;
            ScanPolicy spRef = (ScanPolicy)reference;
            if (sp.maxRecords != spRef.maxRecords) {
                sb.append(",maxRecs:").append(sp.maxRecords);
            }
            if (sp.maxConcurrentNodes != spRef.maxConcurrentNodes) {
                sb.append(",maxConcNodes:").append(sp.maxConcurrentNodes);
            }
            if (sp.concurrentNodes != spRef.concurrentNodes) {
                sb.append(",concNodes:").append(sp.concurrentNodes);
            }
            if (sp.includeBinData != spRef.includeBinData) {
                sb.append(",incBinData:").append(sp.includeBinData);
            }
        }
        return sb.append('}').toString().replaceAll("\\{,", "{");
    }
    
    /**
     * Format the passed number as a humanly readable byte value. 
     * @param bytes
     * @return
     */
    public static String humanReadableByteCountBinary(long bytes) {
        String sign = bytes < 0 ? "-" : "";
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return sign + bytes + "B";
        }
        long fraction = absB & 0x3FF; // 10 bit between the powers
        absB >>= 10;
        String[] suffixes = "kiB,MiB,GiB,TiB,PiB,EiB,ZiB,YiB".split(",");
        for (String currentSuffix : suffixes) {
            if (absB < 1024) {
                // This is the right suffix, work out how may DP we need
                if (fraction == 0) {
                    return String.format("%s%,d%s", sign,absB,currentSuffix);
                }
                else {
                    int roundingNum = 10;
                    String stringFmt = "%s%,d.%01d%s";
                    if (absB < 10) {
                        roundingNum = 1000;
                        stringFmt = "%s%,d.%03d%s";
                    }
                    else if (absB < 100) {
                        roundingNum = 100;
                        stringFmt = "%s%,d.%02d%s";
                    }
                    return String.format(stringFmt, sign,absB,(512 + roundingNum*fraction)/1024,currentSuffix);
                }
            }
            else {
                fraction = absB & 0x3FF;
                absB >>= 10;
            }
        }
        throw new IllegalArgumentException("Number is too big to be shown in corect format: " + bytes);
    }
    
    public static void main(String[] args) {
        ClientPolicy policy = new ClientPolicy();
        policy.clusterName = "Some long cluster name";
        policy.asyncMaxConnsPerNode = 27;
        policy.forceSingleNode = true;
        policy.tlsPolicy = new TlsPolicy();
        System.out.println(Utils.estimateSize(policy));
        System.out.println(humanReadableByteCountBinary(1024));
        System.out.println(humanReadableByteCountBinary(1025));
        System.out.println(humanReadableByteCountBinary(1536));
        System.out.println(humanReadableByteCountBinary(2000));
        System.out.println(humanReadableByteCountBinary(10000));
        System.out.println(humanReadableByteCountBinary(204800));
        System.out.println(humanReadableByteCountBinary(102500));
        System.out.println(humanReadableByteCountBinary(1463728913));
    }
}
