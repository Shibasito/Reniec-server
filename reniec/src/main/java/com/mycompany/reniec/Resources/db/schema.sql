CREATE TABLE IF NOT EXISTS personas(
  dni TEXT PRIMARY KEY,
  apell_pat TEXT NOT NULL,
  apell_mat TEXT NOT NULL,
  nombres   TEXT NOT NULL,
  fecha_naci TEXT NOT NULL,   -- 'YYYY-MM-DD'
  sexo TEXT CHECK (sexo IN ('M','F')),
  estado_civil TEXT NOT NULL,
  lugar_nacimiento TEXT NOT NULL,
  direccion TEXT
);

