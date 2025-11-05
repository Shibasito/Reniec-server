CREATE TABLE IF NOT EXISTS personas (
  dni           VARCHAR(8) PRIMARY KEY,
  apell_pat     TEXT NOT NULL,
  apell_mat     TEXT NOT NULL,
  nombres       TEXT NOT NULL,
  fecha_naci    DATE NOT NULL,
  sexo          CHAR(1) CHECK (sexo IN ('M','F')),
  direccion     TEXT
);

