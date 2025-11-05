package com.mycompany.reniec.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class RabbitReniecServer implements AutoCloseable {
  private final Connection conn;
  private final Channel ch;
  private final ReniecService service;
  private final ObjectMapper om = new ObjectMapper();

  public RabbitReniecServer(ReniecService service) throws Exception {
    this.service = service;

    ConnectionFactory f = new ConnectionFactory();
    f.setHost(Env.get("RABBIT_HOST","localhost"));
    f.setPort(Env.getInt("RABBIT_PORT",5672));
    f.setUsername(Env.get("RABBIT_USERNAME","guest"));
    f.setPassword(Env.get("RABBIT_PASSWORD","guest"));
    f.setVirtualHost(Env.get("RABBIT_VHOST","/"));

    // Thread pool para callbacks (mejor throughput)
    ThreadFactory tf = r -> {
      Thread t = new Thread(r);
      t.setName("reniec-rpc-" + t.getId());
      t.setDaemon(true);
      return t;
    };
    f.setSharedExecutor(Executors.newFixedThreadPool(8, tf));

    this.conn = f.newConnection("reniec-server");
    this.ch = conn.createChannel();

    final String ex = Env.get("RABBIT_VERIFY_EXCHANGE","verify_exchange");
    final String q  = Env.get("RABBIT_VERIFY_QUEUE","verify_queue");
    final String rk = Env.get("RABBIT_VERIFY_ROUTING","verify");

    ch.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true);
    ch.queueDeclare(q, true, false, false, null);
    ch.queueBind(q, ex, rk);
    ch.basicQos(8);

    DeliverCallback cb = (tag, delivery) -> {
      AMQP.BasicProperties props = delivery.getProperties();
      String corrId = props.getCorrelationId();
      String replyTo = props.getReplyTo();

      String resp = handle(delivery.getBody());
      byte[] body = resp.getBytes(StandardCharsets.UTF_8);

      // Publica respuesta RPC (default exchange → replyTo)
      AMQP.BasicProperties rProps = new AMQP.BasicProperties.Builder()
          .correlationId(corrId)
          .contentType("application/json")
          .build();
      ch.basicPublish("", replyTo, rProps, body);

      ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    };

    ch.basicConsume(q, false, "reniec-consumer", cb, tag -> {});
    System.out.printf("[RENIEC] Escuchando %s (exchange=%s rk=%s)%n", q, ex, rk);
  }

  private String handle(byte[] request) {
    try {
      JsonNode node = om.readTree(new String(request, StandardCharsets.UTF_8));
      // Acepta dni tanto en la raíz como dentro de {data:{dni}} por compatibilidad
      String dni = node.path("dni").asText(
          node.path("data").path("dni").asText("")
      );

      Person p = service.verifyPerson(dni);

      ObjectNode out = om.createObjectNode();

      if (p == null) {
        // Formato que el bank entiende: ok=true + data.valid=false
        ObjectNode data = om.createObjectNode();
        data.put("valid", false);
        data.put("dni", dni);

        out.put("ok", true);
        out.set("data", data);
        out.putNull("person");   // alias opcional
        out.putNull("error");
      } else {
        // ok=true + data.valid=true con los campos que usa el bank
        ObjectNode data = om.createObjectNode();
        data.put("valid", true);
        data.put("dni", p.dni());
        data.put("nombres", p.nombres());
        data.put("apellidoPat", p.apellPat());
        data.put("apellidoMat", p.apellMat());

        // (Opcional) Campos adicionales del Person si quieres exponerlos
        if (p.estadoCivil() != null) data.put("estado_civil", p.estadoCivil());
        if (p.lugarNacimiento() != null) data.put("lugar_nacimiento", p.lugarNacimiento());
        if (p.direccion() != null) data.put("direccion", p.direccion());

        out.put("ok", true);
        out.set("data", data);   // <- lo que el bank realmente consume
        out.set("person", data); // <- alias por compatibilidad
        out.putNull("error");
      }

      return om.writeValueAsString(out);

    } catch (Exception e) {
      // Errores reales de procesamiento → ok=false con mensaje
      try {
        ObjectNode out = om.createObjectNode();
        out.put("ok", false);
        out.putNull("person");
        ObjectNode err = om.createObjectNode();
        err.put("message", e.getMessage());
        out.set("error", err);
        return om.writeValueAsString(out);
      } catch (Exception ignored) {
        return "{\"ok\":false,\"person\":null,\"error\":{\"message\":\"SERIALIZATION_ERROR\"}}";
      }
    }
  }

  @Override
  public void close() throws Exception {
    try { if (ch != null && ch.isOpen()) ch.close(); }
    finally { if (conn != null && conn.isOpen()) conn.close(); }
  }
}

