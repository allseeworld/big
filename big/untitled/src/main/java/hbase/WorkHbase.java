

package hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class WorkHbase {
    private static final Logger logger = LoggerFactory.getLogger(WorkHbase.class);
    public static final String OP_ROW_KEY = "lijiangyong";
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
//            创建Hbase的连接
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();
        } catch (IOException e) {
//            记录log日志
            logger.error("init failed", e);
        }
    }

    public static boolean isTableExist(String tableName) {
        try {
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("isTableExist failed, tableName: {}", tableName, e);
        }
        return false;
    }

    public static boolean createTable(String tableName, String... columnFamilies) {
        return IsTable_Empty(tableName, admin, columnFamilies);
    }

    static boolean IsTable_Empty(String tableName, Admin admin, String[] columnFamilies) {
        if (StringUtils.isEmpty(tableName) || columnFamilies.length < 1) {
            throw new IllegalArgumentException("tableName or columnFamilies is null");
        }
//        创建一个新表(newBuilder),tableName
        TableDescriptorBuilder tDescBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor descriptor =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
            tDescBuilder.setColumnFamily(descriptor);
        }

        try {
//            admin尝试创建表,成功返回True
            admin.createTable(tDescBuilder.build());
            WorkHbase.logger.info("createTable success, tableName: {}", tableName);
            return true;
        } catch (IOException e) {
            WorkHbase.logger.error("createTable failed, tableName: {}", tableName, e);
        }
//        失败返回false
        return false;
    }

    public static void deleteTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        logger.info("deleteTable success, tableName: {}", tableName);
    }

    public static void putData(String tableName, String rowKey, String colFamily, String colKey, String colValue) throws IOException {
//        获取表进行put数据
        GetTable(tableName, rowKey, colFamily, colKey, colValue, connection);
        return;
    }

    static void GetTable(String tableName, String rowKey, String colFamily, String colKey, String colValue, Connection connection) throws IOException {
//        获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
//        创建put(rowKey)
        Put put = new Put(Bytes.toBytes(rowKey));
//        族,column,值
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), Bytes.toBytes(colValue));
//        put数据
        table.put(put);
//        关闭连接
        table.close();
    }

    public static void getData(String tableName, String rowKey, String colFamily, String colKey) throws IOException {
//        创建连接
        Table table = connection.getTable(TableName.valueOf(tableName));
//        创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
//        判断colKey是否是nil
        if (StringUtils.isEmpty(colKey)) {
//            使用族查询
            get.addFamily(Bytes.toBytes(colFamily));
        } else {
//            使用族加列名查询
            get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));
        }
//        get数据
        Result result = table.get(get);
//        遍历数据
        for (Cell cell : result.rawCells()) {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String cq = Bytes.toString(CellUtil.cloneQualifier(cell));
            String cv = Bytes.toString(CellUtil.cloneValue(cell));
            logger.info("Family:{}, Qualifier:{}, Value:{}", cf, cq, cv);
        }

        table.close();
    }

    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                String cr = Bytes.toString(CellUtil.cloneRow(cell));
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String cq = Bytes.toString(CellUtil.cloneQualifier(cell));
                String cv = Bytes.toString(CellUtil.cloneValue(cell));
                logger.info("Name:{}, Family:{}, Qualifier:{}, Value:{}", cr, cf, cq, cv);
            }
        }
    }

    public static void deleteData(String tableName, String rowKey, String colFamily, String colKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
//        创建删除
        Delete delete = new Delete(Bytes.toBytes(rowKey));
//        按族和列名删除
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));

        table.delete(delete);
    }


    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            logger.warn("close connection failed", e);
        }
    }

    public static void main(String[] args) throws Exception {
        String tableName = "lijianyong:student";
//        判断表是否存在,如果存在就删除
        if (isTableExist(tableName)) {
//            删除表名
            deleteTable(tableName);
        }

        // 创建表
        createTable(tableName, "info", "score");

        // 构造数据
        Map<String, List<Long>> dataMap = new HashMap<>();
        dataMap.put("Tom", Arrays.asList(20210000000001L, 1L, 75L,82L));
        dataMap.put("Jerry", Arrays.asList(20210000000002L, 1L, 85L,67L));
        dataMap.put("Jack", Arrays.asList(20210000000003L, 2L, 80L,80L));
        dataMap.put("Rose", Arrays.asList(20210000000004L, 2L, 60L,61L));
        dataMap.put(OP_ROW_KEY, Arrays.asList(20200388011234L, 3L, 66L,77L));

        // 插入数据
        logger.info("put all data");
        dataMap.forEach((k, v) -> {
            try {
//                对数据进行挨个put,因为数据存储是列存储,不同的列需要各存一次
                putData(tableName, k,"info", "student_id", v.get(0).toString());
                putData(tableName, k,"info", "class", v.get(1).toString());
                putData(tableName, k,"score", "understanding", v.get(2).toString());
                putData(tableName, k,"score", "programming", v.get(3).toString());
            } catch (Exception e) {
                logger.error("putData failed", e);
            }
        } );

        // 查询
        logger.info("get data");

        getData(tableName,OP_ROW_KEY, "info", "student_id");
        getData(tableName,OP_ROW_KEY, "score", null);

        // 扫全表，大数据量慎用
        logger.info("scan table");
        scanTable(tableName);

        // 删除数据
        logger.info("delete data");
        deleteData(tableName, OP_ROW_KEY, "info", "student_id");
        deleteData(tableName, OP_ROW_KEY, "score", "programming");

        // 查询
        logger.info("get data");
        getData(tableName,OP_ROW_KEY, "info", null);
        getData(tableName,OP_ROW_KEY, "score", null);

        // 关闭
        close();
    }
}

