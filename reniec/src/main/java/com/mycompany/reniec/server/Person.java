package com.mycompany.reniec.server;

public record Person(
    String dni,
    String apellPat,
    String apellMat,
    String nombres,
    String fechaNaci, // yyyy-MM-dd
    String sexo,
    String direccion
) {}
