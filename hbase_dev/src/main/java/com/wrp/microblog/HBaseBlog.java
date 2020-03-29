package com.wrp.microblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * 发送微博内容
     * 1. 将uid微博内容添加到content表
     * 2. 从relation表中，获得uid的粉丝有哪些fan_uids
     * 3. fan_uids中，每个fan_uid插入数据，uid发送微博时的rowkey
     */
    public void publishWeibo(String uid, String centent) throws IOException {
        //第一步，添加uid的微博到content表
        Connection connection = getConnection();
        Table weiboContent = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        long timeStamp = System.currentTimeMillis();

        // put 内容到content表，rowkey由uid_timestamp构成
        String rowkey = uid + "_" + timeStamp;
        Put put = new Put(rowkey.getBytes());
        put.addColumn("info".getBytes(), "content".getBytes(), timeStamp, centent.getBytes());
        weiboContent.put(put);

        // 第二步，从relation表中获取uid的粉丝的fan_uids
        Table relationTable = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        Get get = new Get(uid.getBytes());
        get.addFamily("fans".getBytes());
        Result result = relationTable.get(get);
        if (result.isEmpty()) {
            weiboContent.close();
            relationTable.close();
            connection.close();
        }
        Cell[] cells = result.rawCells();
        List<byte[]> fan_uids = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] fan_uid = CellUtil.cloneQualifier(cell);
            fan_uids.add(fan_uid);
        }

        //第三步，向fan_uids中每个fan_uid插入uid发送微博内容的rowkey（微博的rowkey）
        Table receiveTable = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        List<Put> putList = new ArrayList<>();
        for (byte[] fan_uid : fan_uids) {
            // 每个fan_uid put一次
            Put put1 = new Put(fan_uid);
            put1.addColumn("info".getBytes(), uid.getBytes(), timeStamp, rowkey.getBytes());
            putList.add(put1);
        }
        receiveTable.put(putList);
        //释放资源
        weiboContent.close();
        receiveTable.close();
        receiveTable.close();
        connection.close();
    }

    /**
     * 添加关注用户，一次可能添加多个关注用户
     * A 关注一批用户 B,C ,D
     * 第一步：A是B,C,D的关注者   在weibo:relations 当中attend列族当中以A作为rowkey，
     *         B,C,D作为列名，B,C,D作为列值，保存起来
     * 第二步：B,C,D都会多一个粉丝A  在weibo:relations 当中fans列族当中分别以B,C,D作为rowkey，
     *         A作为列名，A作为列值，保存起来
     * 第三步：A需要获取B,C,D 的微博内容存放到 receive_content_email 表当中去，以A作为rowkey，
     *         B,C,D作为列名，获取B,C,D发布的微博rowkey，放到对应的列值里面去
     * @param uid
     * @param attends
     * @throws IOException
     */
    public void addAttends(String uid, String... attends) throws IOException {
        // 1. 把uid关注别人的行为写入relation表的attend列簇下，被关注人的uid直接作为列名
        Connection connection = getConnection();
        Table relationTable = connection.getTable(TableName.valueOf(WEIBO_RELATION));

        Put put = new Put(uid.getBytes());
        for (String attend : attends) {
            Put put1 = new Put(uid.getBytes());
            put.addColumn("attends".getBytes(), attend.getBytes(), attend.getBytes());
        }
        relationTable.put(put);

        // 2. 将attends获得一个粉丝的业务逻辑，在relation表fans列簇中，添加uid列和列值uid
        for (String attend : attends) {
            Put put1 = new Put(attend.getBytes());
            put1.addColumn("fans".getBytes(), uid.getBytes(), uid.getBytes());
            relationTable.put(put1);
        }

        // 3. 在content表中查询attends中被关注的每个人发微博的rowkey
        Table contentTable = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        Scan scan = new Scan();
        ArrayList<byte[]> rowkeyBytes = new ArrayList<>();
        for (String attend : attends) {
            // attend -> 被关注人的uid
            PrefixFilter prefixFilter = new PrefixFilter((attend + "_").getBytes());
            scan.setFilter(prefixFilter);
            ResultScanner scanner = contentTable.getScanner(scan);
            if (null == scanner) {
                continue;
            }
            for (Result result : scanner) {
                byte[] rowkeyWeiboContent = result.getRow();
                rowkeyBytes.add(rowkeyWeiboContent);
            }
        }

        // 4. 将被关注的每个人发微博的rowkey写入email表
        Table receiveEmailTable = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        if (rowkeyBytes.size() > 0) {
            Put put2 = new Put(uid.getBytes());
            for (byte[] rowkeyWeiboContent2 : rowkeyBytes) {
                String rowkeyContent = rowkeyWeiboContent2.toString();
                String[] split = rowkeyContent.split("_");
                put2.addColumn("info".getBytes(), split[0].getBytes(), Long.parseLong(split[1]), rowkeyWeiboContent2);
            }
            receiveEmailTable.put(put2);
        }
        contentTable.close();
        relationTable.close();
        receiveEmailTable.close();
        connection.close();
    }

    /**
     * 取消关注: 如A取消关注 B,C,D这三个用户
     * 其实逻辑与关注B,C,D相反即可
     * 第一步：在weibo:relation关系表当中，在attends列族当中删除B,C,D这三个列
     * 第二步：在weibo:relation关系表当中，在fans列族当中，以B,C,D为rowkey，查找fans列族当中A这个粉丝，并删除掉
     * 第三步：A取消关注B,C,D,在收件箱中，删除取关的人的微博的rowkey
     */
    public void cancelAttend(String uid, String... attends) throws IOException {
        // 在relation表中删除关注的人
        Connection connection = getConnection();
        Table relationTable = connection.getTable(TableName.valueOf(WEIBO_RELATION));
        Delete delete = new Delete(uid.getBytes());

        for (String cancelAttend : attends) {
            // 删除列的最新版本：该表的列只有一个版本
            delete.addColumn("attends".getBytes(), cancelAttend.getBytes());
        }
        relationTable.delete(delete);

        // 在relation表中删除attends的fans的uid
        for (String cancelAttend : attends) {
            Delete delete1 = new Delete(cancelAttend.getBytes());
            delete1.addColumn("fans".getBytes(), uid.getBytes());
            relationTable.delete(delete1);
        }

        // 在receiveEmail表中删除uid的attends相关的列
        Table receiveEmailTable = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        Delete delete2 = new Delete(uid.getBytes());
        for (String attend : attends) {
            // 删除列的所有版本
            delete.addColumns("info".getBytes(), attend.getBytes());
        }
        receiveEmailTable.delete(delete2);

        relationTable.close();
        receiveEmailTable.close();
        connection.close();
    }

    /**
     * 获取关注的人的微博内容
     * 例如A用户刷新微博，拉取他所有关注人的微博内容
     * A 从 weibo:receive_content_email  表当中获取所有关注人的rowkey
     * 通过rowkey从weibo:content表当中获取微博内容
     */
    public void getContent(String uid) throws IOException {
        // 从ReceiveEmail表中获取uid所有的值，即uid关注的人的所有微博的rowkey
        Connection connection = getConnection();
        Table receiveEmailTable = connection.getTable(TableName.valueOf(WEIBO_RECEIVE_CONTENT_EMAIL));
        Get get = new Get(uid.getBytes());
        get.setMaxVersions(5);

        Result result = receiveEmailTable.get(get);
        Cell[] cells = result.rawCells();
        ArrayList<Get> gets = new ArrayList<>();
        for (Cell cell : cells) {
            byte[] bytes = CellUtil.cloneValue(cell);
            Get get1 = new Get(bytes);
            gets.add(get1);
        }

        // 根据rowkey，去content表中拉取微博内容
        Table contentTable = connection.getTable(TableName.valueOf(WEIBO_CONTENT));
        Result[] results = contentTable.get(gets);
        for (Result result1 : results) {
            byte[] content = result1.getValue("info".getBytes(), "content".getBytes());
            System.out.println(Bytes.toString(content));
        }
    }

    public static void main(String[] args) throws IOException {
        HBaseBlog hBaseBlog = new HBaseBlog();
        // 创建命名空间
        hBaseBlog.createNameSpace();

        // 创建微博内容表
        hBaseBlog.createTableContent();

        // 创建微博关系表
        hBaseBlog.createTableRelations();

        // 创建收件箱表
        hBaseBlog.createTabelReceiveContentEmails();

        // 发微博
        hBaseBlog.publishWeibo("2", "今天测试了一天代码，终于测通了");

        // 关注别人: 如1 关注 2, 3, M等人
        hBaseBlog.addAttends("1", "2", "3", "M");

        // 取消关注
        hBaseBlog.cancelAttend("1", "M");

        // 获取某人关注的人发的微博
        hBaseBlog.getContent("1");
    }
}
