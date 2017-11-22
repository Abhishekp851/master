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
import org.apache.log4j.PropertyConfigurator;
import java.io.*;
import java.util.*;

public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 
  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {

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

final Logger log = Logger.getLogger(HiveJdbcClient.class);
String log4JPropertyFile = "/usr/lib/hive/conf/log4j.properties";
//Properties p = new Properties();
//p.load(new FileInputStream(log4JPropertyFile));
PropertyConfigurator.configure(log4JPropertyFile);


log.info("Creating Hivestatment :");
Statement setStmt = con.createStatement();
log.info("Setting logging to true :" );
setStmt.execute("set hive.server2.logging.operation.enabled = true");
log.info("getting test count :" );
String sql1 = "select count(*) from default.orders_sqoop";
HiveStatement stmt1 = (HiveStatement)con.createStatement();
//assertNotNull("Statement is null", stmt1);
log.info("Running:"+ sql1 );

    Thread logThread = null;
    logThread = new Thread(createLogRunnable(stmnt1));
    logThread.setDaemon(true);
    logThread.start();
    boolean hasResults1 = stmnt1.execute(sql);
    logThread.interrupt();

    ResultSet res2 = stmnt1.getResultSet();
    if (res2.next()) {
      log.info(res2.getString(1));
    }

//ResultSet res1 = stmt1.executeQuery(sql1);
//log.info("Getting logs using functions:");
//List<String> logs = stmt1.getQueryLog(true,100);
//log.info(Arrays.toString(logs.toArray()));
log.info("count as below:"+ res2.getRow());
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
Statement setStmt2 = con.createStatement();
HiveStatement stmnt2 = (HiveStatement)con.createStatement();

    String tableName = "testHiveDriverTable";
    setStmt2.execute("set hive.server2.logging.operation.enabled=true");
    setStmt2.execute("set hive.server2.logging.operation.log.location=/user/cloudera/script/jdbcout.log");
    setStmt2.execute("drop table if exists " + tableName);
    setStmt2.execute("create table " + tableName + " (key int, value string)");
    // show tables
    String sql = "show tables '" + tableName + "'";
    log.info("Running: " + sql);

    Thread logThread = null;
    logThread = new Thread(createLogRunnable(stmnt2));
    logThread.setDaemon(true);
    logThread.start();
    boolean hasResults = stmnt2.execute(sql);
    logThread.interrupt();

    ResultSet res = stmnt2.getResultSet();
    if (res.next()) {
      log.info(res.getString(1));
    }
       // describe table
    sql = "describe " + tableName;
    log.info("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      log.info(res.getString(1) + "\t" + res.getString(2));
    }
 
    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
    String filepath = "/tmp/a.txt";
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    log.info("Running: " + sql);
    stmt.execute(sql);
 
    // select * query
    sql = "select * from " + tableName;
    log.info("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      log.info(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
    }
 
    // regular hive query
    sql = "select count(1) from " + tableName;
    log.info("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      log.info(res.getString(1));
    }
  }
protected static Runnable createLogRunnable (Statement statement) {
//    if (statement instanceof HiveStatement) {
      final HiveStatement hiveStatement = (HiveStatement) statement;

final Logger log2 = Logger.getLogger(HiveJdbcClient.class);
String log4JPropertyFile2 = "/usr/lib/hive/conf/log4j.properties";
PropertyConfigurator.configure(log4JPropertyFile2);
      
Runnable runnable = new Runnable() {
        @Override
        public void run() {
          while (hiveStatement.hasMoreLogs()) {
            try {
              // fetch the log periodically and output to beeline console
              for (String logi : hiveStatement.getQueryLog()) {
                log2.info(logi);
                }
              Thread.sleep(1);
            } catch (SQLException e) {
              log2.error("Found sql error");
              return;
            } catch (InterruptedException e) {
              log2.debug("Getting log thread is interrupted, since query is done!");
              return;
            }
          }
      }
  };
//}
return runnable;
}
}
