package com.wrp.microblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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

    /**
     * 创建微博内容表
     * Table Name   weibo:content
     * RowKey   用户ID_时间戳
     * ColumnFamily     info
     * ColumnLabel      标题,内容,图片
     * Version     1个版本
     * @throws IOException
     */
    public void createTableContent() throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(WEIBO_CONTENT))) {
            HTableDescriptor weiboContent = new HTableDescriptor(TableName.valueOf(WEIBO_CONTENT));
            HColumnDescriptor info = new HColumnDescriptor("info");
            //指定最小最大版本
            info.setMinVersions(1);
            info.setMaxVersions(1);
            info.setBlockCacheEnabled(true);

            weiboContent.addFamily(info);
            admin.createTable(weiboContent);
        }
        admin.close();
        connection.close();
    }

    /**
     * 创建用户关系表
     *  Table Name    weibo:relations
     *  RowKey    用户ID
     *  ColumnFamily  attends、fans
     *  ColumnLabel   关注用户ID，粉丝用户ID
     *  ColumnValue   用户ID
     *  Version   1个版本
     * @throws IOException
     */
    public void createTableRelations() throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(WEIBO_RELATION))) {
            HTableDescriptor weiboRelations = new HTableDescriptor(TableName.valueOf(WEIBO_RELATION));

            HColumnDescriptor attends = new HColumnDescriptor("attends");
            attends.setMinVersions(1);
            attends.setMaxVersions(1);
            attends.setBlockCacheEnabled(true);

            HColumnDescriptor fans = new HColumnDescriptor("fans");
            fans.setMinVersions(1);
            fans.setMaxVersions(1);
            fans.setBlockCacheEnabled(true);

            weiboRelations.addFamily(attends);
            weiboRelations.addFamily(fans);
            admin.createTable(weiboRelations);
        }
    }

    /**
     *  创建微博收件箱表
     *  Table Name    weibo:receive_content_email
     *  RowKey    用户ID
     *  ColumnFamily  info
     *  ColumnLabel   用户ID
     *  ColumnValue   取微博内容的RowKey
     *  Version   1000
     * @throws IOException
     */
    public void createTabelReceiveContentEmails() throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();

        if (!admin.tableExists(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL))) {
            HTableDescriptor weiboReceiveContentEmail = new HTableDescriptor(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
            HColumnDescriptor info = new HColumnDescriptor("info");
            info.setMinVersions(1000);
            info.setMaxVersions(1000);
            info.setBlockCacheEnabled(true);

            weiboReceiveContentEmail.addFamily(info);
            admin.createTable(weiboReceiveContentEmail);
        }
        admin.close();
        connection.close();
    }
}
