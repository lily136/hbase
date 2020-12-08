package com.txy;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class students {
	
	
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;


    static {
        
        configuration=HBaseConfiguration.create();
        //configuration.addResource(Resources.getResource("hbase-site.xml"));

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
			
        } catch (IOException e) {e.printStackTrace();}
    }


	// 创建表
	
    public static void creatTable(String tableName, String[] familys) throws IOException {
		
        if (admin.tableExists(org.apache.hadoop.hbase.TableName.valueOf(tableName))) {
			
            System.out.println("table "+ tableName +" already exists");
			
        } 
        else {
            
			HTableDescriptor tableDesc = new HTableDescriptor(org.apache.hadoop.hbase.TableName.valueOf(tableName));
			
            for (int i=0;i<familys.length;i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
			System.out.println("create table "+ tableName +" success");
			
        }
    }


	// 插入数据
	
	public static void addData(String tableName, String rowKey, String family, String qualifier, String value) throws Exception{
		
		Table table=connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
		table.put(put);
		System.out.println("insert record success");
		
	}


	// 扫描
	
	public static void scan(String tableName) throws IOException{
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		
		for(Result r:ss){
				
				byte[] row=r.getRow();
				System.out.println("Row key："+new String(row));
				
				List<Cell> listCells=r.listCells();
				
				for(Cell cell : listCells){
					
					byte[] familyArray = cell.getFamilyArray();
					byte[] qualifierArray = cell.getQualifierArray();
					byte[] valueArray = cell.getValueArray();
					
					System.out.print("row: "+new String(row)+" ");
					System.out.print("family: "+new String(familyArray,cell.getFamilyOffset(),cell.getFamilyLength())+" ");
					System.out.print("qualifier: "+new String(qualifierArray,cell.getQualifierOffset(),cell.getQualifierLength())+" ");
					System.out.print("timestamp: "+cell.getTimestamp()+" ");
					System.out.println("value: "+new String(valueArray,cell.getValueOffset(),cell.getValueLength()));
					
				} 
			}
		
	}


	// 查询数据
	
	public static void selectData(String tableName,String family, String qualifier) throws IOException{
		
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		
		s.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier));
		
		Iterator<Result> results = ss.iterator();
		
		int count=1;  //
		
        while (results.hasNext()){
			
            Result r = results.next();
			
			
			
            for (Cell cell : r.listCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.print("row: 00");   //
				System.out.print(count);    //
                System.out.println("  "+column + ": " + value);
				//count=count+1;
            }
			
			count=count+1;   //
			
        }
		
	}
	

	// 增加新列族
	
	public static void addFamily(String tableName, String newFamily) throws IOException{
		
		HColumnDescriptor newColumnFamliy = new HColumnDescriptor(newFamily);
		admin.addColumn(TableName.valueOf(tableName), newColumnFamliy);
		
	}


	// 删除表

    public static void dropTable(String tableName) throws IOException{
		
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
		
    }



    public static void main(String[] args) 
            throws Exception {
        
		
        // 1 创建 students 表并添加数据
		
		System.out.println(" ");
		System.out.println("Task1");
		System.out.println(" ");
      
        String[] column = {"ID", "Description", "Courses", "Home"};
        creatTable("students",column);
		
        addData("students","001","Description","Name","Li Lei");
		addData("students","001","Description","Height","176");
		addData("students","001","Courses","Chinese","80");
		addData("students","001","Courses","Math","90");
		addData("students","001","Courses","Physics","95");
		addData("students","001","Home","Province","Zhejiang");
		
        addData("students","002","Description","Name","Han Meimei");
		addData("students","002","Description","Height","183");
		addData("students","002","Courses","Chinese","88");
		addData("students","002","Courses","Math","77");
		addData("students","002","Courses","Physics","66");
		addData("students","002","Home","Province","Beijing");
		
        addData("students","003","Description","Name","Xiao Ming");
        addData("students","003","Description","Height","162");
        addData("students","003","Courses","Chinese","90");
        addData("students","003","Courses","Math","90");
        addData("students","003","Courses","Physics","90");
        addData("students","003","Home","Province","Shanghai");


		// 2 扫描 students 表
		
		System.out.println(" ");
		System.out.println("Task2");
		System.out.println(" ");
		
        scan("students");
		
		
		// 3 查询学生来自的省
		
		System.out.println(" ");
		System.out.println("Task3");
		System.out.println(" ");
		
        selectData("students","Home","Province");
		
		
		// 4 增加新的列 Courses:English 并添加数据
		
		System.out.println(" ");
		System.out.println("Task4");
		System.out.println(" ");
		
        addData("students","001","Courses","English","95");
        addData("students","002","Courses","English","85");
        addData("students","003","Courses","English","98");
		
		scan("students");
		
		
		// 5 增加新的列族 Contact 和新列 Contact:Email 并添加数据
		
		System.out.println(" ");
		System.out.println("Task5");
		System.out.println(" ");

        addFamily("students","Contact");
		
        addData("students","001","Contact","Email","lilei@qq.com");
        addData("students","002","Contact","Email","hanmeimei@qq.com");
        addData("students","003","Contact","Email","xiaoming@qq.com");

        scan("students");


		// 6 删除 students 表

		System.out.println(" ");
		System.out.println("Task6");
		System.out.println(" ");

        dropTable("students");
		
		
        admin.close();
        connection.close();
    }
}