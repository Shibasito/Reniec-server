package com.mycompany.reniec.server;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class Env {
  // Carpeta fija de recursos dentro del classpath
  private static final String BASE = "com/mycompany/reniec/Resources/";
  private static final Properties P = new Properties();

  static {
    // Cargar application.properties desde BASE
    try (InputStream in = cl().getResourceAsStream(BASE + "application.properties")) {
      if (in != null) P.load(in);
    } catch (IOException ignored) {}

    // Las variables de entorno tienen prioridad
    System.getenv().forEach((k, v) -> {
      if (v != null && !v.isBlank()) P.setProperty(k, v);
    });
  }

  private Env() {}

  private static ClassLoader cl() {
    ClassLoader c = Thread.currentThread().getContextClassLoader();
    return (c != null) ? c : Env.class.getClassLoader();
  }

  public static String get(String key, String def) {
    String v = System.getenv(key);
    if (v != null && !v.isBlank()) return v;
    v = P.getProperty(key);
    return (v == null || v.isBlank()) ? def : v;
  }

  public static int getInt(String key, int def) {
    try { return Integer.parseInt(get(key, String.valueOf(def))); }
    catch (Exception e) { return def; }
  }

  /** Abre un recurso SOLO desde com/mycompany/reniec/Resources/... */
  public static InputStream openResource(String relativePath) throws IOException {
    String norm = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
    InputStream in = cl().getResourceAsStream(BASE + norm);
    if (in == null) throw new FileNotFoundException("Recurso no encontrado: " + BASE + norm);
    return in;
  }

  /** Lee el recurso (UTF-8) de esa carpeta fija. */
  public static String readStringResource(String relativePath) throws IOException {
    try (InputStream in = openResource(relativePath);
         BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) sb.append(line).append('\n');
      return sb.toString();
    }
  }
}

