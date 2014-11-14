package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 10/8/14.
 */
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

public class PhoenixTest {

    public static void main(String[] args) throws SQLException {
        Statement stmt = null;
        ResultSet rset = null;

        try {
            Properties prop = new Properties();
            FileInputStream in = new FileInputStream("/Users/chiyao/dev/lubeinsightsplatform/conf/hadoop-metrics2-phoenix.properties");
            prop.load(in);
            in.close();
            in = new FileInputStream("/Users/chiyao/dev/lubeinsightsplatform/conf/hadoop-metrics2-hbase.properties");
            prop.load(in);
            in.close();
            Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2182",prop);
            stmt = con.createStatement();

            stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");
            stmt.executeUpdate("upsert into test values (1,'Hello')");
            stmt.executeUpdate("upsert into test values (2,'World!')");
            con.commit();

            PreparedStatement statement = con.prepareStatement("select * from test");
            rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("mycolumn"));
            }
            statement.close();
            con.close();
        } catch (Exception e) {e.printStackTrace();}
    }
}