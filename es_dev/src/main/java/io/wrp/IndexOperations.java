package io.wrp;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author LYleonard
 * @date 2020/7/30 11:21
 * @description ES 索引文件操作
 */
public class IndexOperations {

    private static void createIndex() {
        Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(settings).addTransportAddress(
                    new TransportAddress(InetAddress.getByName("192.168.20.8"),19300));
        } catch (UnknownHostException e) {
            System.out.println("Host is not reachable!");
            e.printStackTrace();
        }

        String json = "{" +
                "\"user\":\"lily\"," +
                "\"postDate\":\"2020-07-30\"," +
                "\"message\":\"travelying out Elasticsearch\"" +
                "}";
        IndexResponse indexResponse = client.prepareIndex("indexjavaapi", "article","4")
                .setSource(json, XContentType.JSON).get();
        client.close();
    }

    public static void main(String[] args) {
        createIndex();
    }
}
