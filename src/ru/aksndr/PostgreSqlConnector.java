package ru.aksndr;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgreSqlConnector {

    private Connection c = null;

    private String hostname;
    private String port;
    private String dbname;
    private String username;
    private String password;

    public PostgreSqlConnector(String hostname, String port, String dbname, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.dbname = dbname;
        this.username = username;
        this.password = password;
    }

    public Map connect() {
        Map result = validateConnectionParams();

        if (!(Boolean) result.get("ok")) return result;

        StringBuilder sb = new StringBuilder()
                .append("jdbc:postgresql://")
                .append(this.hostname).append(":")
                .append(this.port).append("/")
                .append(this.dbname);
        String connectionString = sb.toString();

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            result.put("ok", false);
            result.put("msg", e.getMessage());
            return result;
        }
        try {
            c = DriverManager.getConnection(connectionString, this.username, this.password);
        } catch (Exception e) {
            result.put("ok", false);
            result.put("msg", e.getMessage());
        }
        return result;
    }

    private Map validateConnectionParams() {
        Map result = new HashMap();
        result.put("ok", true);

        List<String> missingArgs = new ArrayList<>(5);
        if (this.hostname == null || "".equals(this.hostname)) missingArgs.add("hostname");
        if (this.port == null || "".equals(this.port)) missingArgs.add("port");
        if (this.dbname == null || "".equals(this.dbname)) missingArgs.add("dbname");
        if (this.username == null || "".equals(this.username)) missingArgs.add("username");
        if (this.password == null || "".equals(this.password)) missingArgs.add("password");

        if (!missingArgs.isEmpty()) {
            result.put("ok", false);
            result.put("msg", "Missing required params: " + missingArgs.toString());
        }
        return result;
    }

    public Map execute(String sqlQuery) {
        Map result = new HashMap();
        result.put("ok", true);

        if (sqlQuery == null || "".equals(sqlQuery)) {
            result.put("ok", false);
            result.put("msg", "Missing required sqlQuery param.");
        }

        Statement stmt;
        try {
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery(sqlQuery);

            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            ArrayList list = new ArrayList(50);
            while (rs.next()) {
                HashMap row = new HashMap(columns);
                for (int i = 1; i <= columns; ++i) {
                    row.put(md.getColumnName(i), rs.getObject(i));
                }
                list.add(row);
            }
            result.put("value", list);
            rs.close();
            stmt.close();
        } catch (Exception e) {
            result.put("ok", false);
            result.put("msg", e.getMessage());
        }
        return result;
    }

    public Map disconnect() {
        Map result = new HashMap();
        result.put("ok", true);
        try {
            c.close();
        } catch (Exception e) {
            result.put("ok", false);
            result.put("msg", e.getMessage());
        }
        return result;
    }
}
