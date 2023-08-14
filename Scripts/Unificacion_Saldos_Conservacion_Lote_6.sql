create table saldos_conservacion_lote_6 as
(
	select terrenosdigitalizadoslote6.predio_t_id 
		,saldoconservacionelpaso.*
	from public.saldo_conservacion_elpaso saldoconservacionelpaso
	left join public.terrenos_digitalizados_lote_6 terrenosdigitalizadoslote6
	on saldoconservacionelpaso.numero_predial = terrenosdigitalizadoslote6.predio_numero_predial
);

