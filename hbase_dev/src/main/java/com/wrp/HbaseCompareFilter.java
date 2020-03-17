package com.wrp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName HbaseCompareFilter
 * @Author LYleonard
 * @Date 2020/3/13 14:30
 * @Description hbase比较过滤器
 * Version 1.0
 **/
public class HbaseCompareFilter {
    private static Connection connection;
    private static final String TABLE_NAME = "myuser";

    /**
     * 查询所有rowkey小于0003的所有数据
     */
    public static void rowFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        // 获取比较对象
        BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());

        // rowFilter需要传入两个参数
        // 1：比较规则
        // 2: 比较对象
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);

        // 为scan对象设置过滤器
        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResults(scanner);
    }

    /**
     * 列簇过滤器：通过familyFilter来实现列族的过滤
     * * 需要过滤，列族名中包含f2子串的列簇
     * * f1  f2   hello   world
     */
    public static void familyFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        SubstringComparator substringComparator = new SubstringComparator("f2");
        //通过familyfilter来设置列族的过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(familyFilter);
        ResultScanner results = table.getScanner(scan);
        printResults(results);
    }

    /**
     * 列名过滤器 只查询包含name列的值
     * @throws IOException
     */
    public static void qualifierFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        SubstringComparator substringComparator = new SubstringComparator("name");
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(qualifierFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResults(scanner);

    }

    /**
     * 查询字段值含数字8的数据
     */
    public static void contain8() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        SubstringComparator substringComparator = new SubstringComparator("8");
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
        scan.setFilter(valueFilter);

        ResultScanner scanner = table.getScanner(scan);
        printResults(scanner);
    }

    /**
     * 单列值过滤器 SingleColumnValueFilter, 相反SingleColumnValueExcludeFilter列值排除过滤器
     */
    public static void singleColumnValueFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        // SingleColumnValueFilter 传参：列簇名 列名 比较过滤操作符 值
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                "f1".getBytes(),"name".getBytes(), CompareFilter.CompareOp.EQUAL ,"zhuge".getBytes());
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);

        printResults(scanner);
    }

    /**
     * rowkey前缀过滤器PrefixFilter
     * @throws IOException
     */
    public static void prefixFilter() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        // 过滤rowkey以 00开头的数据
        PrefixFilter filter = new PrefixFilter("00".getBytes());
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        printResults(scanner);
    }

    /**
     * 通过pageFilter实现分页
     * @throws IOException
     */
    public static void hbasePageFilter() throws IOException {
        int pageNum = 3;
        int pageSize = 2;
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        if (pageNum == 1){
            // 获取第1页的数据
            scan.setMaxResultSize(pageSize);
            scan.setStopRow("".getBytes());
            // 使用分页过滤器实现数据分页
            PageFilter filter = new PageFilter(pageSize);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            printResults(scanner);
        } else {
            String startRow = "";
            //扫描数据的调试 扫描五条数据
            int scanDatas = (pageNum - 1) * pageSize + 1;
            //设置一步往前扫描多少条数据
            scan.setMaxResultSize(scanDatas);
            PageFilter filter = new PageFilter(scanDatas);
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);

            for (Result result: scanner){
                // 获取rowkey
                byte[] row = result.getRow();
                startRow = Bytes.toString(row);
                //最后一条数据的rowkey就是我们需要的起始的rowkey
            }
            scan.setStartRow(startRow.getBytes());
            scan.setMaxResultSize(pageSize);
            PageFilter filter1 = new PageFilter(pageSize);
            scan.setFilter(filter1);
            ResultScanner scanner1 = table.getScanner(scan);
            printResults(scanner1);
        }
    }

    public static void filterList() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                "f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "zhuge".getBytes());
        PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(prefixFilter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        printResults(scanner);
    }

    /**
     * 按rowkey删除数据
     * @throws IOException
     */
    public static void deleteData() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Delete delete = new Delete("0003".getBytes());

        table.delete(delete);
    }

    /**
     * 删除表
     * @throws IOException
     */
    public static void deleteTable() throws IOException {
        //获取管理员对象，用于表的删除
        Admin admin = connection.getAdmin();
        //删除一张表之前，需要先禁用表
        admin.disableTable(TableName.valueOf(TABLE_NAME));
        admin.deleteTable(TableName.valueOf(TABLE_NAME));
    }

    /**
     * 打印查询结果数据
     * @param results
     */
    public static void printResults(ResultScanner results) {
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] familyName = CellUtil.cloneFamily(cell);
                byte[] qualifierName = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] value = CellUtil.cloneValue(cell);

                if ("age".equals(Bytes.toString(qualifierName)) || "id".equals(Bytes.toString(qualifierName))) {
                    System.out.println("数据rowkey为:" + Bytes.toString(rowkey) + "||" +
                            "列簇为:" + Bytes.toString(familyName) + "||" +
                            "列名为:" + Bytes.toString(qualifierName) + "||" + "值为:" + Bytes.toInt(value));
                } else {
                    System.out.println("数据rowkey为:" + Bytes.toString(rowkey) + "||" +
                            "列簇为:" + Bytes.toString(familyName) + "||" +
                            "列名为:" + Bytes.toString(qualifierName) + "||" + "值为:" + Bytes.toString(value));
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",
                    "master:2181,slave1:2181,slave2:2181");
            connection= ConnectionFactory.createConnection(configuration);

            rowFilter();
            familyFilter();
            qualifierFilter();
            contain8();
            singleColumnValueFilter();
            hbasePageFilter();
            prefixFilter();
            filterList();
//            deleteData();
//            deleteTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
