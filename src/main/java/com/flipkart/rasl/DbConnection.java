package com.flipkart.rasl;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class DbConnection {
    /*static final String JDBC_PASS = "";

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String JDBC_DB_URL = "jdbc:mysql://localhost:3306/ra_materialmanager_fki_development";

    // JDBC Database Credentials
    static final String JDBC_USER = "root";
    private static GenericObjectPool gPool = null;
    */

    public DataSource dataSource;
    private Session session;

    private String url;
    private String password;
    private String username;
    private String seller_id;
    private String ip;
    public DbConnection(String url, String username, String password, String ip) {
        this.url = url;
        this.password = password;
        this.username = username;
        this.ip = ip;
    }
    @SuppressWarnings("unused")

    private static Session doSshTunnel(String strSshUser, String strSshPassword, String strSshHost, int nSshPort,
                                    String strRemoteHost, int nLocalPort, int nRemotePort) throws JSchException {
        final JSch jsch = new JSch();
        jsch.addIdentity("~/.ssh/id_rsa","intelligentgirl@1995");

        Session session = jsch.getSession(strSshUser, strSshHost, nSshPort);
        session.setPassword(strSshPassword);

        final Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.connect();
        session.setPortForwardingL(nLocalPort, strRemoteHost, nRemotePort);
        return session;
    }

    public DataSource setUpPool() throws Exception {


        String strSshUser = "angelina.l"; // SSH loging username
        String strSshPassword = "Intelligentgirl@1995"; // SSH login password
        String strSshHost = "fkl-shipping18.nm.flipkart.com"; // hostname or ip or
        // SSH server
        int nSshPort = 22; // remote SSH host port number
        String strRemoteHost = this.ip; // hostname or
        // ip of
        // your
        // database
        // server
        int nLocalPort = 3366; // local port number use to bind SSH tunnel
        int nRemotePort = 3306; // remote port number of your database

        final JSch jsch = new JSch();
        jsch.addIdentity("~/.ssh/id_rsa","intelligentgirl@1995");

        Session session = jsch.getSession(strSshUser, strSshHost, nSshPort);
        session.setPassword(strSshPassword);

        final Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.connect();
        session.setPortForwardingL(nLocalPort, strRemoteHost, nRemotePort);

        //Session session = doSshTunnel(strSshUser, strSshPassword, strSshHost, nSshPort, strRemoteHost, nLocalPort,
        //        nRemotePort);
        this.session = session;
        /*Class.forName(JDBC_DRIVER);


        // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
        gPool = new GenericObjectPool();
        gPool.setMaxActive(5);

        // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
        ConnectionFactory cf = new DriverManagerConnectionFactory(JDBC_DB_URL, JDBC_USER, JDBC_PASS);

        // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
        return new PoolingDataSource(gPool);
        */
        Channel channel = session.openChannel("shell");
        channel.connect();
        PoolProperties p = new PoolProperties();
        p.setUrl(this.url);
        p.setDriverClassName("com.mysql.jdbc.Driver");

        p.setUsername(this.username);
        p.setPassword(this.password);
        p.setJmxEnabled(true);
        p.setTestWhileIdle(false);
        p.setTestOnBorrow(true);
        p.setValidationQuery("SELECT 1");
        p.setTestOnReturn(false);
        p.setValidationInterval(30000);
        p.setTimeBetweenEvictionRunsMillis(60000);
        p.setMaxActive(30);
        p.setInitialSize(30);
        p.setMaxWait(300000);
        p.setRemoveAbandonedTimeout(60*60);
        p.setMinEvictableIdleTimeMillis(30000);
        p.setMinIdle(10);
        p.setLogAbandoned(true);
        p.setRemoveAbandoned(true);
        p.setJdbcInterceptors(
                "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"+
                        "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
        DataSource datasource = new DataSource() {

        };
        datasource.setPoolProperties(p);

        this.dataSource = datasource;
        return datasource;
    }



    public synchronized Connection getConnectionAsync() {
        Connection con = null;
        try {
            Future<Connection> future = null;
            try {
                future = this.dataSource.getConnectionAsync();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            while (!future.isDone()) {
                System.out.println("Connection is not yet available. Do some background work");
                try {
                    Thread.sleep(100); //simulate work
                }catch (InterruptedException x) {
                    Thread.currentThread().interrupt();
                }
            }
            try {
                con = future.get(); //should return instantly
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

    } catch (Exception e){

            System.out.println("Could not create a new connection to database");
        }

        return con;
    }


    public void disconnectSession(){
        this.session.disconnect();
    }


}
