package com.wrp.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;

import java.sql.*;

/**
 *
 * @ClassName HiveJDBCTest
 * @Author LYleonard
 * @Date 2020/3/4 10:52
 * @Description JDBC操作Hive
 * Version 1.0
 **/
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HiveJDBCTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.29.102:10000/hivedb";
    private static String user = "hadoop";
    private static String password = "";

    private final Logger logger = LogManager.getLogger(this.getClass());
    private static Connection connection = null;

    @BeforeAll
    public void init(){
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            logger.error("创建Hive连接失败, Hive驱动类无效", e);
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("创建Hive连接失败, SQl语句有误", e);
            e.printStackTrace();
        }
    }

    @AfterAll
    public void destory() throws SQLException {
        if (connection != null){
            connection.close();
        }
    }

    @Order(1)
    @Test
    public void craeteDatabase() {
        String sql = "create database test_db";
        logger.info("创建数据库，sql语句:{}", sql);
        try {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            logger.info("创建数据库成功！");
        } catch (SQLException e) {
            logger.error("创建数据库出错！");
            e.printStackTrace();
        }
    }

    @Order(2)
    @Test
    public void showDatabase() {
        String sql = "show databases";
        logger.info("查看数据库：", sql);
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()){
                logger.info("数据库有：{}", resultSet.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Order(3)
    @Test
    public void selectData() {
        String sql = "select * from tb_test";
        logger.info("查询数据，脚本：{}", sql);
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                logger.info("id={},name={}", rs.getInt("id"), rs.getString("name"));
            }
        } catch (SQLException e) {
            logger.error("查询数据出错", e);
        }
    }
}
