package com.mycompany.reniec.server;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.regex.Pattern;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Db (RENIEC)
 * -----------
 * Adaptador de base de datos con:
 *  - JDBC genérico vía HikariCP (PostgreSQL / MySQL / SQLite).
 *  - Bootstrap automático de schema/seed al primer arranque.
 *  - Método de dominio: findByDni(String).
 *
 * Configuración (Env → primero ENV, luego application.properties):
 *  - DB_JDBC_URL   : p.ej. jdbc:postgresql://localhost:5432/reniec
 *  - DB_USERNAME   : usuario (pgsql/mysql)
 *  - DB_PASSWORD   : contraseña (pgsql/mysql)
 *  - DB_POOL_SIZE  : default 4
 *
 * Compatibilidad local (si no defines DB_JDBC_URL):
 *  - DB_SQLITE_URL : p.ej. jdbc:sqlite:data/reniec.db
 */
public final class Db implements AutoCloseable {

  private final HikariDataSource ds;

  public Db() throws Exception {
    // 1) Resolver URL: si no hay DB_JDBC_URL, caer a SQLite local
    String jdbcUrl = Env.get("DB_JDBC_URL", null);
    if (jdbcUrl == null || jdbcUrl.isBlank()) {
      jdbcUrl = Env.get("DB_SQLITE_URL", "jdbc:sqlite:data/reniec.db");
    }

    // 2) Configurar Hikari
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(jdbcUrl);
    cfg.setMaximumPoolSize(Env.getInt("DB_POOL_SIZE", 4));
    cfg.setAutoCommit(false);

    // Usuario/clave solo si aplica
    if (jdbcUrl.startsWith("jdbc:postgresql:")
        || jdbcUrl.startsWith("jdbc:mysql:")
        || jdbcUrl.startsWith("jdbc:mariadb:")) {
      cfg.setUsername(Env.get("DB_USERNAME", "reniec"));
      cfg.setPassword(Env.get("DB_PASSWORD", "reniec"));
    }

    // Asegurar carpeta del archivo SQLite si corresponde
    if (jdbcUrl.startsWith("jdbc:sqlite:")) {
      ensureSqliteDir(jdbcUrl);
    }
    
    // ==================== DIAGNÓSTICO DB (INICIO) ====================
    String envUrl  = System.getenv("DB_JDBC_URL");
    String envUser = System.getenv("DB_USERNAME");
    String envPass = System.getenv("DB_PASSWORD");

    String effUrl  = Env.get("DB_JDBC_URL", null);
    if (effUrl == null || effUrl.isBlank()) {
        effUrl = Env.get("DB_SQLITE_URL", "jdbc:sqlite:data/reniec.db");
    }
    String effUser = Env.get("DB_USERNAME", null);
    String effPass = Env.get("DB_PASSWORD", null);

    System.out.println("========== [DB DEBUG] ==========");
    System.out.println("[DB] cwd=" + System.getProperty("user.dir"));
    System.out.println("[DB] RAW ENV      url=" + envUrl + "  user=" + envUser + "  pass=" + envPass);
    System.out.println("[DB] Env.get()    url=" + effUrl + "  user=" + effUser + "  pass=" + effPass);

    try {
        if (effUrl != null && effUrl.startsWith("jdbc:postgresql:")) {
            String after = effUrl.substring("jdbc:postgresql://".length());
            String hostPort = after.contains("/") ? after.substring(0, after.indexOf('/')) : after;
            System.out.println("[DB] JDBC PG hostPort=" + hostPort + " (esperado 127.0.0.1:5432 o postgres:5432)");
        } else if (effUrl != null && effUrl.startsWith("jdbc:sqlite:")) {
            System.out.println("[DB] JDBC SQLite path=" + effUrl.substring("jdbc:sqlite:".length()));
        }
    } catch (Exception ignore) {}

    System.out.println("[DB] Hikari config about to init ...");
    System.out.println("=================================");
    // ==================== DIAGNÓSTICO DB (FIN) =======================


    this.ds = new HikariDataSource(cfg);

    // 3) Bootstrap: crear tablas y seed si no existe la principal
    try (Connection c = get()) {
      if (!tableExists(c, "personas")) {
        runSqlResource(c, "/com/mycompany/reniec/Resources/db/schema.sql");
        runSqlResource(c, "/com/mycompany/reniec/Resources/db/seed.sql");
        c.commit();
      } else {
        c.rollback();
      }
    }
  }

  /** Obtiene una conexión con autoCommit=false. */
  public Connection get() throws SQLException {
    Connection c = ds.getConnection();
    c.setAutoCommit(false);
    return c;
  }

  @Override public void close() {
    if (ds != null) ds.close();
  }

  // =====================================================================
  // Dominio
  // =====================================================================

  /**
   * Busca una persona por DNI. Devuelve null si no existe.
   * Soporta:
   *  - fecha_naci: DATE (pgsql/mysql) o TEXT (sqlite "YYYY-MM-DD")
   *  - columnas opcionales: estado_civil, lugar_nacimiento
   */
  public Person findByDni(String dni) throws Exception {
    if (dni == null || dni.isBlank()) return null;

    final String sql = "SELECT * FROM personas WHERE dni = ?";

    try (Connection c = get()) {
      c.setReadOnly(true);
      try (PreparedStatement ps = c.prepareStatement(sql)) {
        ps.setString(1, dni);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            c.rollback(); // limpiar transacción antes de devolver
            return null;
          }

          // Campos base (siempre presentes en tu schema.sql)
          String _dni        = rs.getString("dni");
          String apellPat    = rs.getString("apell_pat");
          String apellMat    = rs.getString("apell_mat");
          String nombres     = rs.getString("nombres");

          // fecha_naci puede ser DATE (pgsql) o TEXT (sqlite)
          String fechaNaci;
          try {
            Date d = rs.getDate("fecha_naci");
            fechaNaci = (d != null) ? d.toLocalDate().toString() : null;
          } catch (SQLException ignore) {
            fechaNaci = rs.getString("fecha_naci");
          }

          String sexo        = safeGet(rs, "sexo");
          String direccion   = safeGet(rs, "direccion");

          // Opcionales (si existen en la tabla)
          String estadoCivil     = safeGet(rs, "estado_civil");
          String lugarNacimiento = safeGet(rs, "lugar_nacimiento");

          c.rollback(); // SELECT: no dejamos transacción abierta en el pool

          return new Person(
              _dni,
              apellPat,
              apellMat,
              nombres,
              fechaNaci,
              sexo,
              direccion,
              estadoCivil,
              lugarNacimiento
          );
        }
      }
    }
  }

  // =====================================================================
  // Helpers internos
  // =====================================================================

  /** Si es SQLite archivo, crea el directorio contenedor. */
  private static void ensureSqliteDir(String sqliteUrl) {
    String path = sqliteUrl.substring("jdbc:sqlite:".length()).trim();
    if (":memory:".equals(path)) return;
    Path p = Paths.get(path).toAbsolutePath();
    Path dir = p.getParent();
    if (dir != null) {
      try { Files.createDirectories(dir); } catch (Exception ignored) {}
    }
  }

  /**
   * Chequea existencia de tabla usando metadata (portable).
   * Intenta sin schema y con "public" para Postgres, en mayúsc/minúsc.
   */
  private static boolean tableExists(Connection c, String tableName) throws SQLException {
    DatabaseMetaData md = c.getMetaData();
    String[] types = { "TABLE" };
    if (existsIn(md, null, null, tableName, types)) return true;
    if (existsIn(md, null, null, tableName.toUpperCase(), types)) return true;
    if (existsIn(md, null, null, tableName.toLowerCase(), types)) return true;
    if (existsIn(md, null, "public", tableName, types)) return true;
    if (existsIn(md, null, "public", tableName.toUpperCase(), types)) return true;
    if (existsIn(md, null, "public", tableName.toLowerCase(), types)) return true;
    return false;
  }

  private static boolean existsIn(DatabaseMetaData md, String catalog, String schema, String table, String[] types) throws SQLException {
    try (ResultSet rs = md.getTables(catalog, schema, table, types)) {
      return rs.next();
    }
  }

  /** Ejecuta un script SQL del classpath (split por ';'). */
  private static void runSqlResource(Connection c, String resourcePath) throws Exception {
    String script = loadResourceAsString(resourcePath);
    for (String stmt : splitSqlStatements(script)) {
      try (Statement s = c.createStatement()) {
        s.executeUpdate(stmt);
      }
    }
  }

  private static String loadResourceAsString(String resourcePath) throws Exception {
    try (InputStream in = Db.class.getResourceAsStream(resourcePath)) {
      if (in == null) throw new IllegalArgumentException("Recurso no encontrado: " + resourcePath);
      try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line).append('\n');
        return sb.toString();
      }
    }
  }

  /** Split ingenuo por ';' fuera de cadenas, suficiente para nuestros scripts. */
  private static List<String> splitSqlStatements(String script) {
    List<String> out = new ArrayList<>();
    if (script == null) return out;

    boolean inString = false;
    char quoteChar = 0;
    StringBuilder cur = new StringBuilder();

    for (int i = 0; i < script.length(); i++) {
      char ch = script.charAt(i);
      if (!inString && (ch == '\'' || ch == '"')) {
        inString = true; quoteChar = ch; cur.append(ch);
      } else if (inString && ch == quoteChar) {
        inString = false; quoteChar = 0; cur.append(ch);
      } else if (!inString && ch == ';') {
        String stmt = cur.toString().trim();
        if (!stmt.isEmpty()) out.add(stmt + ";");
        cur.setLength(0);
      } else {
        cur.append(ch);
      }
    }
    String tail = cur.toString().trim();
    if (!tail.isEmpty()) out.add(tail);
    return out;
  }

  /** Lee una columna String si existe; si no existe, devuelve null. */
  private static String safeGet(ResultSet rs, String col) {
    try {
      rs.findColumn(col);
      return rs.getString(col);
    } catch (SQLException ignore) {
      return null;
    }
  }
}

