package com.mycompany.reniec.server;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.mycompany.reniec.server.Person;
import com.mycompany.reniec.server.Env;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Db implements AutoCloseable {
  private final HikariDataSource ds;

  public Db() {
    String host = Env.get("DB_HOST", "localhost");
    int port = Env.getInt("DB_PORT", 5432);
    String db   = Env.get("DB_NAME", "reniec");
    String user = Env.get("DB_USER", "reniec");
    String pass = Env.get("DB_PASS", "reniec");

    String jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + db;

    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(jdbcUrl);
    cfg.setUsername(user);
    cfg.setPassword(pass);
    cfg.setMaximumPoolSize(Env.getInt("DB_POOL_SIZE", 8));
    cfg.setMinimumIdle(1);
    cfg.setAutoCommit(true);

    // Opcionales:
    cfg.addDataSourceProperty("prepareThreshold", "3");

    this.ds = new HikariDataSource(cfg);
  }

  public Person findByDni(String dni) throws Exception {
    final String sql = """
        SELECT dni, apell_pat, apell_mat, nombres, to_char(fecha_naci,'YYYY-MM-DD') AS fecha_naci,
               sexo, direccion
        FROM personas WHERE dni = ?
        """;
    try (Connection c = ds.getConnection();
         PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, dni);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return null;
        return new Person(
            rs.getString("dni").trim(),
            rs.getString("apell_pat"),
            rs.getString("apell_mat"),
            rs.getString("nombres"),
            rs.getString("fecha_naci"),
            rs.getString("sexo"),
            rs.getString("direccion")
        );
      }
    }
  }

  @Override public void close() { if (ds != null) ds.close(); }
}
