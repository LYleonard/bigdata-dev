package com.wrp.sinks;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName MysqlSink
 * @Author LYleonard
 * @Date 2020/4/24 19:59
 * @Description 自定义sink类：把接收的数据按规则过滤并写入mysql表
 * Version 1.0
 **/
public class MysqlSink extends AbstractSink implements Configurable {
    private String mysqlUrl = "";
    private String userName = "";
    private String password = "";
    private String tableName = "";

    Connection conn = null;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if (event != null) {
                //获取body中数据
                String body = new String(event.getBody(), StandardCharsets.UTF_8);
                //若日志中有以下关键字的不需要保存，过滤掉
                if (body.contains("delete") || body.contains("drop") || body.contains("alter")) {
                    status = Status.BACKOFF;
                } else {
                    //保存到mysql
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String createtime = dateFormat.format(new Date());
                    PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName
                            + " (createtime, content) VALUES (?, ?)");
                    ps.setString(1, createtime);
                    ps.setString(2, body);
                    ps.execute();
                    ps.close();
                    status = Status.READY;
                }
            } else {
                status = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Throwable t) {
            transaction.rollback();
            t.getCause().printStackTrace();
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        return status;
    }

    /**
     * 获取配置文件中指定的参数
     * @param context
     */
    @Override
    public void configure(Context context) {
        mysqlUrl = context.getString("mysqlurl");
        userName = context.getString("username");
        password = context.getString("password");
        tableName = context.getString("tablename");
    }

    @Override
    public synchronized void start() {
        //初始化数据库连接
        try {
            conn = DriverManager.getConnection(mysqlUrl, userName, password);
            super.start();
            System.out.println("finished start！");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void stop() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        super.stop();
    }
}
