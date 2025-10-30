package com.mycompany.reniec.server;

import com.mycompany.reniec.server.Db;
import com.mycompany.reniec.server.Person;

public class ReniecService {
  private final Db db;
  public ReniecService(Db db) { this.db = db; }

  public Person verifyPerson(String dni) throws Exception {
    if (dni == null || dni.length() != 8 || !dni.chars().allMatch(Character::isDigit)) {
      return null;
    }
    return db.findByDni(dni);
  }
}
