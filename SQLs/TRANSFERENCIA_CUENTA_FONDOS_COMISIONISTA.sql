UPDATE S 
SET  
S.INDICADOR_RESULTADO_INVERSION = T.INDICADOR_RESULTADO_INVERSION

FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S 

JOIN (
		SELECT T.flg_aceptacion_sal, T.nro_id_sesion, 
		CASE WHEN  T.flg_aceptacion_sal = 'B' THEN 'Exitoso' ELSE 'Negado' END AS INDICADOR_RESULTADO_INVERSION
		FROM (
			SELECT flg_aceptacion_sal ,PERIODO, nro_id_sesion, nombre_operacion_ent
			from ${var:bd_cruda}.fic_tx_cuenta_fondos_comisionista_nrt FON 
			where id_aplicacion like 'FIC'
			and periodo = ${var:periodo_inicial}
			AND (CONCAT(FON.NRO_ID_SESION, CAST(FON.FECHA_HORA_OPERACION AS STRING)))
	        IN (
	            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
	            FROM ${var:bd_cruda}.fic_tx_cuenta_fondos_comisionista_nrt T 
	            WHERE T.id_aplicacion IN ('FIC')
	            AND T.PERIODO = ${var:periodo_inicial}
	            GROUP BY T.nro_id_sesion 
	            )
			UNION ALL
			SELECT flg_aceptacion_sal ,PERIODO, nro_id_sesion, nombre_operacion_ent
			from ${var:bd_cruda}.fic_tx_fondo_a_fondo_unificada_nrt FI
			where id_aplicacion like 'FIC'
			and periodo = ${var:periodo_inicial}
			AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
	        IN (
	            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
	            FROM ${var:bd_cruda}.fic_tx_fondo_a_fondo_unificada_nrt T 
	            WHERE T.id_aplicacion IN ('FIC')
	            AND T.PERIODO = ${var:periodo_inicial}
	            GROUP BY T.nro_id_sesion
	            )	
	        UNION ALL
			SELECT caracter_aceptacion_sal AS flg_aceptacion_sal ,PERIODO, nro_id_sesion, nombre_operacion_ent
			from ${var:bd_cruda}.administrar_tdc_transferencias_nrt FI
			where id_aplicacion like 'FIC'
			and periodo = ${var:periodo_inicial}
			AND (CONCAT(FI.NRO_ID_SESION, CAST(FI.FECHA_HORA_OPERACION AS STRING)))
	        IN (
	            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
	            FROM ${var:bd_cruda}.administrar_tdc_transferencias_nrt T 
	            WHERE T.id_aplicacion IN ('FIC')
	            AND T.PERIODO = ${var:periodo_inicial}
	            GROUP BY T.nro_id_sesion
	            )	
        	)T   
	) T 

ON S.rgst_nro_solicitud = T.nro_id_sesion;


