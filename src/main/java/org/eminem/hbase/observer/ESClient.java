package org.eminem.hbase.observer;



import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.Client;


import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * ES Cleint class
 */
public class ESClient {

    // ElasticSearch的集群名称
    public static String clusterName;
    // ElasticSearch的host
    public static String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    public static int nodePort;
    // ElasticSearch的索引名称
    public static String indexName;
    // ElasticSearch的类型名称
    public static String typeName;
    // ElasticSearch Client
    public static Client client;

    /**
     * get Es config
     *
     * @return
     */
    public static String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : ESClient.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(null));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

    /**
     * init ES client
     */
    public static void initEsClient() {
        Settings settings = Settings.builder()
                .put("cluster.name", ESClient.clusterName).build();
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(ESClient.nodeHost),ESClient.nodePort))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.31.203"),ESClient.nodePort))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.31.204"),ESClient.nodePort))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.31.205"),ESClient.nodePort))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"),ESClient.nodePort));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close ES client
     */
    public static void closeEsClient() {
        client.close();
    }

    public static void main(String[] args) {






        System.out.println("nihao");
    }
}
