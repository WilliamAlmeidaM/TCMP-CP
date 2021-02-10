UPDATE S 
SET  
S.FLG_REALIZA_PERFILAMIENTO = T.FLG_REALIZA_PERFILAMIENTO,
S.CANT_ENCUESTAS_PERFILAMIENTO = S.CANT_ENCUESTAS_PERFILAMIENTO + T.VALOR



FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT 
	
		'Si' AS FLG_REALIZA_PERFILAMIENTO,
   CASE WHEN FI.nombre_perfil_riesgo_sal IS NOT NULL OR FI.nombre_perfil_riesgo_sal != '' 
			THEN 1 else 0
		END AS VALOR, 
		FI.nro_id_sesion

		FROM ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt FI
		WHERE FI.ID_APLICACION IN ('EPR')
		AND FI.PERIODO = ${var:periodo_inicial}

		--Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt T 
            WHERE T.id_aplicacion IN ('EPR')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;