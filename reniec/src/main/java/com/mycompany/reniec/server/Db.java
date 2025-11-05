package com.mycompany.reniec.server;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Db implements AutoCloseable {
  private final HikariDataSource ds;

    public Db() throws Exception {
      String url = Env.get("DB_SQLITE_URL", "jdbc:sqlite:./data/reniec.db");
      ensureSqliteDir(url);                      // <-- AÑADE ESTA LÍNEA

      int pool = Env.getInt("DB_POOL_SIZE", 4);

      HikariConfig cfg = new HikariConfig();
      cfg.setJdbcUrl(url);
      cfg.setMaximumPoolSize(pool);
      cfg.setMinimumIdle(1);
      cfg.setAutoCommit(true);
      // cfg.setDriverClassName("org.sqlite.JDBC"); // opcional
      this.ds = new HikariDataSource(cfg);

      try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
        st.execute("PRAGMA journal_mode=WAL");
        st.execute("PRAGMA synchronous=NORMAL");
      }
      initIfNeeded();
    }
    
    private static void ensureSqliteDir(String jdbcUrl) throws Exception {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:sqlite:")) return;

        String path = jdbcUrl.substring("jdbc:sqlite:".length());
        // ignora memoria
        if (":memory:".equals(path) || path.startsWith("file:")) return;

        Path dbPath = Paths.get(path).toAbsolutePath();
        Path dir = dbPath.getParent();
        if (dir != null && !Files.exists(dir)) {
          Files.createDirectories(dir);
      }
    }

  private void initIfNeeded() throws Exception {
    if (!tableExists("personas")) {
      System.out.println("[DB] Inicializando BD (schema + seed)...");
      execSqlResource("com/mycompany/reniec/Resources/db/schema.sql");
      execSqlResource("com/mycompany/reniec/Resources/db/seed.sql");
      System.out.println("[DB] Inicialización completada.");
    }
  }

  private boolean tableExists(String table) throws Exception {
    final String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    try (Connection c = ds.getConnection();
         PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private void execSqlResource(String resourcePath) throws Exception {
    List<String> statements = loadSqlStatements(resourcePath);
    try (Connection c = ds.getConnection()) {
      for (String s : statements) {
        try (Statement st = c.createStatement()) {
          st.execute(s);
        }
      }
    }
  }

  private List<String> loadSqlStatements(String resourcePath) throws Exception {
    List<String> out = new ArrayList<>();
    InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
    if (in == null) throw new IllegalArgumentException("No se encontró recurso SQL: " + resourcePath);
    try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        // Ignora líneas de comentario simples
        if (line.trim().startsWith("--")) continue;
        sb.append(line).append('\n');
      }
      for (String stmt : sb.toString().split(";")) {
        String s = stmt.trim();
        if (!s.isEmpty()) out.add(s);
      }
    }
    return out;
  }

    public Person findByDni(String dni) throws Exception {
      final String sql = """
          SELECT dni, apell_pat, apell_mat, nombres, fecha_naci, sexo,
                 estado_civil, lugar_nacimiento, direccion
          FROM personas
          WHERE dni = ?
          """;
      try (Connection c = ds.getConnection();
           PreparedStatement ps = c.prepareStatement(sql)) {
        ps.setString(1, dni);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) return null;
          return new Person(
              safe(rs.getString("dni")),
              safe(rs.getString("apell_pat")),
              safe(rs.getString("apell_mat")),
              safe(rs.getString("nombres")),
              safe(rs.getString("fecha_naci")),
              safe(rs.getString("sexo")),
              safe(rs.getString("estado_civil")),      
              safe(rs.getString("lugar_nacimiento")),  
              safe(rs.getString("direccion"))
          );
        }
      }
    }


  private static String safe(String s) { return s == null ? "" : s; }

  @Override public void close() { if (ds != null) ds.close(); }
}
