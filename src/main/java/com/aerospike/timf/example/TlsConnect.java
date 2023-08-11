package com.aerospike.timf.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.pem.util.PemUtils;

public class TlsConnect {
    private String getCert(String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        String ls = System.getProperty("line.separator");
        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }
        // delete the last new line separator
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        reader.close();

        String content = stringBuilder.toString();
        return content;
    }
    
    public static void main(String[] args) throws Exception {
        ClientPolicy clientPolicy = new ClientPolicy();
//        SSLContext sslContext = SSLContext.getInstance("TLS");
//        sslContext.init(null, null, null);
//        CertificateFactory cf = CertificateFactory.getInstance("X.509");

        InputStream certFile = new FileInputStream(new File( "/Users/tfaulkes/Documents/Secrets/Aerolab/CA/cert.pem" ));
        InputStream keyFile = new FileInputStream(new File("/Users/tfaulkes/Documents/Secrets/Aerolab/CA/key.pem"));
        InputStream caFile = new FileInputStream(new File("/Users/tfaulkes/Documents/Secrets/Aerolab/CA/cacert.pem"));
        X509ExtendedKeyManager keyManager = PemUtils.loadIdentityMaterial(certFile, keyFile, null);
        X509ExtendedTrustManager trustManager = PemUtils.loadTrustMaterial(caFile);

        SSLFactory sslFactory = SSLFactory.builder()
                .withIdentityMaterial(keyManager)
                .withTrustMaterial(trustManager)
                .build();

        clientPolicy.tlsPolicy = new TlsPolicy();
        clientPolicy.tlsPolicy.context = sslFactory.getSslContext();
        IAerospikeClient client = new AerospikeClient(clientPolicy, new Host("172.17.0.7", "tls1", 4333));
        client.put(null, new Key("test", "testSet", 1), new Bin("name", "Tim"));
        System.out.println(client.get(null, new Key("test", "testSet", 1)));
        client.close();
        
    }
}
