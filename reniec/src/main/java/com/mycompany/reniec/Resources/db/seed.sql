INSERT INTO personas (dni, apell_pat, apell_mat, nombres, fecha_naci, sexo, direccion)
VALUES
('12345678','TORRES','MENDOZA','LUIS ALBERTO','1992-11-05','M','Sacsayhuamán 789'),
('23456789','PEREZ','GOMEZ','ANA MARIA','1990-03-12','F','Jr. Cusco 123'),
('34567890','ROJAS','SALAZAR','CARLOS','1985-07-21','M','Av. Arequipa 456'),
('45678912','GARCIA','FLORES','MARIA ELENA','1993-01-10','F','Av. Universitaria 1234'),
('78901234','RAMIREZ','QUISPE','JUAN CARLOS','1988-12-30','M','Av. La Molina 5678')
ON CONFLICT (dni) DO NOTHING;
