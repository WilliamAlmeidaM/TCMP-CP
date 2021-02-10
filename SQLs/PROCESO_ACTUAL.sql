UPDATE S 
SET  
S.NRO_PROCESO_CULMINADO = T.NRO_PROCESO_CULMINADO

FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT
	 	FI.proceso_actual AS NRO_PROCESO_CULMINADO,
		FI.nro_id_sesion

		FROM ${var:bd_cruda}.fic_traza_proceso_actual_nrt FI
		WHERE FI.ID_APLICACION IN ('FIC')
        AND FI.PERIODO = ${var:periodo_inicial}
        
        --Subconsulta para garantizar la MAX fecha hora operación

        AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
        IN (
            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
            FROM ${var:bd_cruda}.fic_traza_proceso_actual_nrt T 
            WHERE T.id_aplicacion IN ('FIC')
            AND T.PERIODO = ${var:periodo_inicial}
            GROUP BY T.nro_id_sesion
            )
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;