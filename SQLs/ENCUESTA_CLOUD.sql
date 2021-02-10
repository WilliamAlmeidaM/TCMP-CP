UPDATE S 
SET  
S.CANT_ENCUESTAS_PERFILAMIENTO = T.CANT_ENCUESTAS_PERFILAMIENTO

FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 
		FI.cant_intento_encuesta_sal AS CANT_ENCUESTAS_PERFILAMIENTO,
		FI.nro_id_sesion

		FROM ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt FI
		WHERE FI.ID_APLICACION IN ('EPR')
		AND FI.PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_consulta_encuesta_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;