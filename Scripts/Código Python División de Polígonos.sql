select ui.nombre_municipio
	,ui.codigo_unidad_intervencion
	,T1.total_terrenos
	,(case when T1.total_terrenos between 101 and 200 then 2
		   when T1.total_terrenos between 201 and 300 then 3
		   when T1.total_terrenos between 301 and 400 then 4
		   when T1.total_terrenos between 401 and 500 then 5
		   when T1.total_terrenos between 501 and 600 then 6
		   when T1.total_terrenos between 601 and 700 then 7
		   when T1.total_terrenos between 701 and 800 then 8
		   when T1.total_terrenos between 801 and 900 then 9
		   when T1.total_terrenos between 1301 and 1400 then 14
		   when T1.total_terrenos between 1501 and 1600 then 16
		   when T1.total_terrenos between 1701 and 1800 then 18
		   when T1.total_terrenos between 5001 and 5100 then 51
		   when T1.total_terrenos between 8201 and 8300 then 83
	)
	,ui.geometria
from colombiaseg_lote6.unidades_intervencion_mh_lote_6 ui
inner join (select tb.codigo_unidad_intervencion,tb.municipio,tb.total_terrenos from colombiaseg_lote6.tbl_terrenos_en_ui_lote_6 tb) T1
on concat(ui.nombre_municipio,ui.codigo_unidad_intervencion) = concat(T1.municipio, T1.codigo_unidad_intervencion)
where T1.total_terrenos > 100;