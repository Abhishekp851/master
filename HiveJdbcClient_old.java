import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.Arrays;
import org.apache.hive.jdbc.HiveStatement;

public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 
  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);

      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    //replace "hive" here with the name of the user the queries should run as
    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    Statement stmt = con.createStatement();

System.out.println("Creating Hivestatment :" );
Statement setStmt = con.createStatement();
System.out.println("Setting logging to true :" );
setStmt.execute("set hive.server2.logging.operation.enabled = true");
System.out.println("getting test count :" );
String sql1 = "select count(*) from default.orders_sqoop";
HiveStatement stmt1 = (HiveStatement)con.createStatement();
//assertNotNull("Statement is null", stmt1);
System.out.println("Running:"+ sql1 );
ResultSet res1 = stmt1.executeQuery(sql1);
System.out.println("Getting logs using functions:");
List<String> logs = stmt1.getQueryLog(true,100);
System.out.println(Arrays.toString(logs.toArray()));
System.out.println("count as below:"+ res1.getRow());
stmt1.close();
//assertTrue(logs.size() == 0);
setStmt.execute("set hive.server2.logging.operation.enabled = true");
setStmt.close();
/*

+  public void testGetQueryLogOnDisabledLog() throws Exception {
+    Statement setStmt = con.createStatement();
+    setStmt.execute("set hive.server2.logging.operation.enabled = false");
+    String sql = "select count(*) from " + tableName;
+    HiveStatement stmt = (HiveStatement)con.createStatement();
+    assertNotNull("Statement is null", stmt);
+    stmt.executeQuery(sql);
+    List<String> logs = stmt.getQueryLog(false, 10);
+    stmt.close();
+    assertTrue(logs.size() == 0);
+    setStmt.execute("set hive.server2.logging.operation.enabled = true");
+    setStmt.close();
+  }

*/



    String tableName = "testHiveDriverTable";
    stmt.execute("set hive.server2.logging.operation.enabled=true");
    stmt.execute("set hive.server2.logging.operation.log.location=/user/cloudera/script/jdbcout.log");
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("create table " + tableName + " (key int, value string)");
    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
       // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
 
    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
    String filepath = "/tmp/a.txt";
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    System.out.println("Running: " + sql);
    stmt.execute(sql);
 
    // select * query
    sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
    }
 
    // regular hive query
    sql = "select count(1) from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1));
    }
  }
}
