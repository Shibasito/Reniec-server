CREATE TABLE IF NOT EXISTS personas(
  dni CHAR(8) PRIMARY KEY,
  apell_pat VARCHAR(50) NOT NULL,
  apell_mat VARCHAR(50) NOT NULL,
  nombres   VARCHAR(80) NOT NULL,
  fecha_naci DATE NOT NULL,
  sexo CHAR(1) CHECK (sexo IN ('M','F')),
  direccion VARCHAR(120)
);

