UPDATE S 
SET  
--S.RESULTADO_PERFILAMIENTO = T.RESULTADO_PERFILAMIENTO,
S.FECHA_PERFILAMIENTO = T.FECHA_PERFILAMIENTO,
S.HORA_PERFILAMIENTO = T.HORA_PERFILAMIENTO



FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 
		--FI.perfil_actual_ent AS RESULTADO_PERFILAMIENTO,
		TRUNC(FI.FECHA_HORA_OPERACION, 'dd')  AS FECHA_PERFILAMIENTO, 
		CAST(from_timestamp(FI.FECHA_HORA_OPERACION,'HH:mm:ss') AS STRING)  AS HORA_PERFILAMIENTO, 
		FI.nro_id_sesion

		FROM ${var:bd_cruda}.perfilamiento_novedad_perfil_nrt FI
		WHERE FI.ID_APLICACION IN ('EPR')
		AND FI.PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_novedad_perfil_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;