package org.eminem.hbase.observer;



import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.StringUtils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ES Cleint class
 */

public class EsClient {
    @PostConstruct
    void init() {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }
    // ElasticSearch的集群名称
    private String clusterName;
    // ElasticSearch的host
    private String nodeHost;
    // ElasticSearch的端口（Java API用的是Transport端口，也就是TCP）
    private int nodePort;
    private TransportClient client = null;

    private static final Log LOG = LogFactory.getLog(EsClient.class);

    /**
     * get Es config
     *
     * @return
     */
    public EsClient(String clusterName, String nodeHost, int nodePort) {
        this.clusterName = clusterName;
        this.nodeHost = nodeHost;
        this.nodePort = nodePort;
        this.client = initEsClient();

    }

    public String getInfo() {
        List<String> fields = new ArrayList<String>();
        try {
            for (Field f : EsClient.class.getDeclaredFields()) {
                fields.add(f.getName() + "=" + f.get(this));
            }
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        }
        return StringUtils.join(fields, ", ");
    }

  /*  public String getOneNodeHost() {
        if (this.nodeHost == null || this.nodeHost.length == 0) {
            return "";
        }
        Random rand = new Random();
        return nodeHost[rand.nextInt(this.nodeHost.length)];

    }
*/
    /**
     * init ES client
     */
    public TransportClient initEsClient() {
        LOG.info("---------- Init ES Client " + this.clusterName + " -----------");
        TransportClient client = null;
        Settings settings = Settings.builder().put("cluster.name", this.clusterName).put("client.transport.sniff", true).build();

        try {
            client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName(this.nodeHost), this.nodePort))
            .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.31.203"), this.nodePort));;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            LOG.error("---------- Init ES Client jieshu " + this.clusterName + " -----------");
        }
        return client;
    }

    public void repeatInitEsClient() {
        this.client = initEsClient();
    }

    /**
     * @return the clusterName
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * @param clusterName the clusterName to set
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * @return the nodePort
     */
    public int getNodePort() {
        return nodePort;
    }

    /**
     * @param nodePort the nodePort to set
     */
    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    /**
     * @return the client
     */
    public TransportClient getClient() {
        return client;
    }

    /**
     * @param client the client to set
     */
    public void setClient(TransportClient client) {
        this.client = client;
    }

    public static void main(String[] args) {
        System.out.println("nihaoo");
    }

}
