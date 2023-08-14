
-- Verificación de la existencia de campo y creación del mismo bajo los resultados del condicional if
do
$$
begin
  if not exists (select column_name 
                 from information_schema.columns 
                 where table_schema='colombiaseg_lote6' and table_name='saldos_conservacion' and column_name='vigencia_seresco') then
    alter table colombiaseg_lote6.saldos_conservacion add column vigencia_seresco varchar(9);
  else
    raise notice 'El campo -vigencia_seresco- ya existe';
  end if;
end
$$;

-- Actualización del campo: Se asocia a procesos propios de actualización
update colombiaseg_lote6.saldos_conservacion
set vigencia_seresco = 'Vigente'
where municipio in ('SAN ONOFRE', 'TOLUVIEJO');

update colombiaseg_lote6.saldos_conservacion
set vigencia_seresco = 'Historico'
where municipio in ('EL PASO');

-- Se debe cargar a la base de datos la información actualizada
-- Para este caso la capa se llama -actualizacion_saldos_conservacion-

select *
from colombiaseg_lote6.actualizacion_saldos_conservacion;

-- Insercion de actualización a tabla base
-- Tabla Destino
insert into colombiaseg_lote6.saldos_conservacion 
(
	territorial
	,id_negocio
	,numero_solicitud
	,municipio
	,zona
	,numero_radicacion
	,numero_predial
	,tipo_tramite
	,clasificacion
	,estado_tramite
	,estado_proceso
	,inicio_proceso
	,fin_proceso
	,tarea
	,estado_tarea
	,fecha_inicio_tarea
	,fecha_fin_tarea
	,dias_habiles
	,funcionario_radicador
	,usuario_propietario
	,numero_resolucion
	,fecha_resolucion
	,radicacion_masivo
	,folio_matricula
	,vigencia_seresco
)
-- Tabla Origen
select territorial
	,id_negocio
	,numero_solicitud
	,municipio
	,zona
	,numero_radicacion
	,numero_predial
	,tipo_tramite
	,clasificacion
	,estado_tramite
	,estado_proceso
	,inicio_proceso
	,fin_proceso
	,tarea
	,estado_tarea
	,fecha_inicio_tarea
	,fecha_fin_tarea
	,dias_habiles
	,funcionario_radicador
	,usuario_propietario
	,numero_resolucion
	,fecha_resolucion
	,radicacion_masivo
	,folio_matricula
	,vigencia_seresco
from colombiaseg_lote6.actualizacion_saldos_conservacion;