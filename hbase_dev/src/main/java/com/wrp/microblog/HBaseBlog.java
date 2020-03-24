package com.wrp.microblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName HBaseBlog
 * @Author LYleonard
 * @Date 2020/3/24 22:08
 * @Description 微博案例
 * Version 1.0
 **/
public class HBaseBlog {
    // 命名空间：weibo
    /**
     * 微博内容表
     */
    private static final byte[] WEIBO_CONTENT = "weibo:content".getBytes();
    /**
     * 微博关系表
     */
    private static final byte[] WEIBO_RELATION = "weibo:relation".getBytes();
    /**
     * 收件箱表
     */
    private static final byte[] WEIBO_RECEIVE_CONTENT_EMAIL = "weibo:receive_content_email".getBytes();

    /**
     * 设置配置，创建连接
     * @return 返回 HBase 连接对象
     * @throws IOException
     */
    public Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    /**
     * 创建命名空间
     * @throws IOException
     */
    public void createNameSpace() throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("weibo")
                .addConfiguration("creator", "LYleonard").build();
        admin.createNamespace(namespaceDescriptor);
        admin.close();
        connection.close();
    }


}
