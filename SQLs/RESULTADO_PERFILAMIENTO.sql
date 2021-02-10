UPDATE S 
SET  
S.RESULTADO_PERFILAMIENTO = T.RESULTADO_PERFILAMIENTO


FROM ${var:bd_temp}.temp_apertura_fic_trazabilidad_nrt S

JOIN (
		
SELECT T.nro_id_sesion, T.RESULTADO_PERFILAMIENTO, T.FECHA
      

		FROM
		( 
			SELECT PER.perfil_actual_ent AS RESULTADO_PERFILAMIENTO, PER.nombre_operacion_ent SER, PER.FECHA_HORA_OPERACION FECHA,
			PER.nro_id_sesion as nro_id_sesion

			FROM ${var:bd_cruda}.perfilamiento_novedad_perfil_nrt PER  
			WHERE ID_APLICACION IN ('EPR')
			AND PERIODO = ${var:periodo_inicial}

			--Subconsulta para garantizar la MAX fecha hora operación

	        AND (CONCAT(PER.NRO_ID_SESION, CAST(PER.FECHA_HORA_OPERACION AS STRING)))
	        IN (
	            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
	            FROM ${var:bd_cruda}.perfilamiento_novedad_perfil_nrt T 
	            WHERE T.id_aplicacion IN ('EPR')
	            AND T.PERIODO = ${var:periodo_inicial}
	            GROUP BY T.nro_id_sesion
	            )
          union ALL

		        SELECT
				PER.perfil_asignado_sal AS RESULTADO_PERFILAMIENTO, PER.nombre_operacion_ent SER, PER.FECHA_HORA_OPERACION FECHA,
				PER.nro_id_sesion as nro_id_sesion

				FROM ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt PER  
				WHERE ID_APLICACION IN ('EPR')
				AND PERIODO = ${var:periodo_inicial}

				--Subconsulta para garantizar la MAX fecha hora operación

		        AND (CONCAT(PER.NRO_ID_SESION, CAST(PER.FECHA_HORA_OPERACION AS STRING)))
		        IN (
		            SELECT MAX(CONCAT(T.NRO_ID_SESION, CAST(T.FECHA_HORA_OPERACION AS STRING)))
		            FROM ${var:bd_cruda}.perfilamiento_generacion_perfil_nrt T 
		            WHERE T.id_aplicacion IN ('EPR')
		            AND T.PERIODO = ${var:periodo_inicial}
		            GROUP BY T.nro_id_sesion
		            ) 

		        ) T   
		        
		        
	) T  

ON S.rgst_nro_solicitud = T.nro_id_sesion;