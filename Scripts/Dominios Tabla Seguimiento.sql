create table if not exists tbl_profesionales_verificacion_calidad
(
	identificador int2 primary key
	,numero_documento_identidad varchar(20) not null
	,nombre_completo varchar(100) not null
);

insert into tbl_profesionales_verificacion_calidad values(1, '9999999999', 'Magda Castro');
insert into tbl_profesionales_verificacion_calidad values(2, '8888888888', 'Santiago Gutierrez');
insert into tbl_profesionales_verificacion_calidad values(3, '80798034', 'Rodian Saby');

create table if not exists tbl_estado_verificacion_calidad
(
	identificador int2 primary key
	,tipo_estado varchar(30) not null
);

insert into tbl_estado_verificacion_calidad values (1, 'Sin asignar');
insert into tbl_estado_verificacion_calidad values (2, 'Asignado');
insert into tbl_estado_verificacion_calidad values (3, 'En verificación');
insert into tbl_estado_verificacion_calidad values (4, 'Verificado y aprobado');
insert into tbl_estado_verificacion_calidad values (5, 'Dudas e inquietudes');
insert into tbl_estado_verificacion_calidad values (6, 'FRM a eliminar');