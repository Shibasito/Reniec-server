package com.mycompany.reniec.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.*;
import com.mycompany.reniec.server.Person;
import com.mycompany.reniec.server.ReniecService;
import com.mycompany.reniec.server.Env;

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

      var resp = handle(delivery.getBody());
      byte[] body = resp.getBytes(StandardCharsets.UTF_8);

      // Publica respuesta RPC
      AMQP.BasicProperties rProps = new AMQP.BasicProperties.Builder()
          .correlationId(corrId)
          .build();
      ch.basicPublish("", replyTo, rProps, body);

      ch.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    };

    ch.basicConsume(q, false, "reniec-consumer", cb, tag -> {});
    System.out.printf("[RENIEC] Escuchando %s (exchange=%s rk=%s)%n", q, ex, rk);
  }

  private String handle(byte[] request) {
    try {
      var node = om.readTree(new String(request, StandardCharsets.UTF_8));
      String dni = node.path("dni").asText("");

      Person p = service.verifyPerson(dni);

      ObjectNode out = om.createObjectNode();
      if (p == null) {
        out.put("ok", false);
        out.putNull("person");
        out.put("error", "NOT_FOUND");
      } else {
        ObjectNode person = om.createObjectNode();
        person.put("dni", p.dni());
        person.put("apell_pat", p.apellPat());
        person.put("apell_mat", p.apellMat());
        person.put("nombres", p.nombres());
        person.put("fecha_naci", p.fechaNaci());
        person.put("sexo", p.sexo());
        person.put("estado_civil", p.estadoCivil());           
        person.put("lugar_nacimiento", p.lugarNacimiento());   
        person.put("direccion", p.direccion());

        out.put("ok", true);
        out.set("person", person);
        out.putNull("error");
      }
      return om.writeValueAsString(out);
    } catch (Exception e) {
      try {
        ObjectNode out = om.createObjectNode();
        out.put("ok", false);
        out.putNull("person");
        out.put("error", e.getMessage());
        return om.writeValueAsString(out);
      } catch (Exception ignored) {
        return "{\"ok\":false,\"person\":null,\"error\":\"SERIALIZATION_ERROR\"}";
      }
    }
  }

  @Override public void close() throws Exception {
    try { if (ch != null && ch.isOpen()) ch.close(); }
    finally { if (conn != null && conn.isOpen()) conn.close(); }
  }
}

