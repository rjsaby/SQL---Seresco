select dblink_connect('conn1', 'host=172.19.8.3 dbname=maphurricane user=cmedicion password=Serescobm');

--****************************** CONEXIÓN ******************************

call colombiaseg_lote6.dblink_bd_maphurricane();

drop table if exists validacion_geografica_lote_6;
drop table if exists dom_validacion_geografica;
drop table if exists dom_profesional_validador;

create table validacion_geografica_lote_6 as
(
	select t_id seguimiento_t_id
		,local_id
		,cast(null as date) fecha_validacion 
		,null validado_por
		,'Sin_Validar' estado_validacion
		,geom
	from dblink_ue_terreno_campo
	where substring(local_id,1,5) in ('20250','70713','70823')
);

alter table validacion_geografica_lote_6 add primary key (seguimiento_t_id);

create table dom_validacion_geografica
(
	id serial primary key not null
	,codigo varchar(50) not null
	,descripcion varchar(100) not null
);

create table dom_profesional_validador
(
	id serial primary key not null
	,codigo varchar(50) not null
	,descripcion varchar(100) not null
);

insert into dom_validacion_geografica values (1,'Sin_Validar','Sin_Validar');
insert into dom_validacion_geografica values (2,'Ajustado','Ajustado');
insert into dom_validacion_geografica values (3,'Rechazado','Rechazado');
insert into dom_validacion_geografica values (4,'Aprobado','Aprobado');

insert into dom_profesional_validador values (1,'Luisa_Lopez','Luisa Lopez');
insert into dom_profesional_validador values (2,'Luz_Gomez','Luz Gomez');

-- Modificación Dominios
select estado_validacion
	,count(*)
from validacion_geografica_lote_6
group by 1;

update validacion_geografica_lote_6
set estado_validacion = 'Ajustado'
where estado_validacion = 'ajustado';

update validacion_geografica_lote_6
set estado_validacion = 'aprobado'
where estado_validacion = 'Aprobado';

update validacion_geografica_lote_6
set estado_validacion = 'Rechazado'
where estado_validacion = 'rechazado';