package com.mycompany.reniec.server;


public class App {
  public static void main(String[] args) throws Exception {
    try (Db db = new Db()) {
      ReniecService service = new ReniecService(db);
      try (RabbitReniecServer server = new RabbitReniecServer(service)) {
        System.out.println("[RENIEC] Servidor iniciado. Ctrl+C para salir.");
        // Bloquea el hilo principal
        Thread.currentThread().join();
      }
    }
  }
}

