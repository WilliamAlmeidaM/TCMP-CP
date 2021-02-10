#! /usr/bin/env python
#VERSION 5.0
import subprocess as sp
import json
from sys import argv
import os
from time import sleep
import re
import pandas as pd
import time
from datetime import datetime, timedelta


       
#periodo_inicial=periodo[:6]+'01'
#SI  NO SE RECIBE EL PARAMETRO DE FECHA, TOMAR EL DIA ACTUAL
if len(argv) == 1:
    periodo=time.strftime('%Y%m%d') 
    periodo_desde = datetime.strptime(periodo, "%Y%m%d") - timedelta(days=1)
    periodo_hasta = datetime.strptime(periodo, "%Y%m%d")
    #SI SE RECIBE uUN PARAMETRO DE FECHA, TOMAR EL VALOR DEL PARAMETRO RECIBIDO
elif len(argv) == 2:
    periodo = argv[1]
    periodo_desde = datetime.strptime(periodo, "%Y%m%d") - timedelta(days=1)
    periodo_hasta = datetime.strptime(periodo, "%Y%m%d")
else:
    periodo = argv[1]
    periodo_desde = datetime.strptime(argv[1], "%Y%m%d") # strptime covertir numero en fecha 
    periodo_hasta = datetime.strptime(argv[2], "%Y%m%d")



periodo_inicial=periodo
paso=0

#while(periodo_hasta >= periodo_desde):
#    print "periodo_hasta  {}.".format(periodo_hasta)
#    
#    periodo_inicial = datetime.strftime(periodo_hasta, "%Y%m%d") 
#    print "periodo_inicial  {}.".format(periodo_inicial)
#
#    periodo_hasta = periodo_hasta  - timedelta(days=1)

#exit(1)

os.environ['PYTHON_EGG_DIR'] = './tmp/.python-eggs'
os.environ['PYTHON_EGG_CACHE'] = './tmp/.python-eggs'
print os.environ['PYTHON_EGG_DIR']
print os.environ['PYTHON_EGG_CACHE']
print argv


def EjecutarPaso(SCRIPT):
    global paso
    paso = paso + 1
    HORA_INICIO=datetime.now().now()
    print "{EL_PASO}.{EL_SCRIPT} ".format(EL_PASO=paso, EL_SCRIPT=SCRIPT)
    impala_shell_execute_file(SCRIPT)
    HORA_FIN=datetime.now().now()
    impala_shell_execute(config_log['GRABAR_LOG'].format(PROCESO = SCRIPT, BD_DESTINO=config_log['bd_final'],PERIODO=periodo_inicial,SERVICIO=SERVICIO,FECHA_PROCESO=FECHA_PROCESO, HORA_INICIO=HORA_INICIO, HORA_FIN=HORA_FIN, DURACION=HORA_FIN-HORA_INICIO ))
    return 0



def load_config(config_file="config_v3.json"):
    with open(config_file) as f:
        return json.load(f)
#1
def load_config_log(config_file="config_log.json"):
    with open(config_file) as f:
        return json.load(f)

def lstring(list, sep=None):
    if sep is None:
        return "".join(list)
    return sep.join(list)

def kerberos_initializer(user):
    kinit_executable = "kinit"
    keytab = """{user}.keytab""".format(user=user)
    cmd = [
        kinit_executable,
        "-kt",
        keytab,
        user
    ]
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=False)
    proc.wait()
    if proc.returncode != 0:
        print("--Unrecoverable error, kerberos initialization failed")
        print(lstring(proc.stdout.readlines()))
        exit(1)
    else:
        print("--Kerberos initialization successful")

def impala_shell_execute_file(sqlFile):
    cmd = [
        'impala-shell',
        "-k",
        "--ssl",
        "-i",
        config['impala_host'],
        "-f",
        "{}".format(sqlFile),
        "--var=periodo_inicial={}".format(periodo_inicial),
        "--var=bd_cruda={}".format(config['bd_cruda']),
        "--var=bd_temp={}".format(config['bd_temp']),
        "--var=bd_final={}".format(config['bd_final'])
        ]
    #print " ".join(cmd)
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=False)
    out, err = proc.communicate()

    if proc.returncode != 0:
        #print("--Query failed")
        #print(err)
        print(out)
        exit(1)
    else:
        if out is not None:
            #print("--Query execution success")
            #print(out)
            out_table = parse_table(out)
            #print out_table
            return out_table 

            
def impala_shell_execute(query):
    cmd = [
        'impala-shell',
        "-k",
        "--ssl",
        "-i",
        config['impala_host'],
        "-q",
        "{}".format(query)
    ]
    ####### NO IMPRIMIR EL SQL      print " ".join(cmd)
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=False)
    out, err = proc.communicate()

    if proc.returncode != 0:
        print("--Query failed")
        print(err)
        print(out)
        exit(1)
    else:
        if out is not None:
            #print("--Query execution success")
           ####### NO MOSTRAR EL QUERY QUE DEVUELVE IMPALA  print(out)
            out_table = parse_table(out)
            #print out_table #PARA QUE IMPIRMA EN PANTALLA EL RESULTADO DE UN QUERY
            return out_table


def parse_table(out):
    horizontal_lines_re = r"(\+\-+)+\+"
    out_lines = out.split("\n")
    while not re.search(horizontal_lines_re, out_lines[0]):
        del out_lines[0]
        if len(out_lines) == 0:
            return None
    columns = map(lambda x: x.strip(), out_lines[1].split('|')[1:-1])
    print columns
    data = out_lines[3:-3]
    data = map(lambda x: map(lambda y: y.strip(),  x.split('|')[1:-1]), data)
    return pd.DataFrame(data, columns=columns)

print("############# Inicio")

config = load_config()
config_log = load_config_log()
kerberos_initializer(config['kerberos_user'])


#periodo_menos_10_dias = datetime.strptime(periodo, "%Y%m%d") - timedelta(days=10)
#ARMAR FECHAS PARA BUSCAR CRECIMIENTOS
#LOS CUALES SE CALCULA DEL MES ANTERIOR A LA FECHA DE PROCESO
#Ej : SI LA FECHA DE PROCESO ES 20191114
#LAS FECHAS PARA CRECIMIENTOS SON DEL 20191001 AL 20191031
#periodo_tmp = datetime.strptime(periodo_inicial, "%Y%m%d") - timedelta(days=1)
#periodo_final_crecimientos = periodo_tmp.strftime("%Y%m%d")
#periodo_inicial_crecimientos=periodo_final_crecimientos[:6]+'01'


print("============================================")
print "PERIODO  {}.".format(periodo_inicial)
print "BD TEMP  {}.".format(config['bd_temp'])
print "BD CRUDA  {}.".format(config['bd_cruda'])
print "BD FINAL  {}.".format(config['bd_final'])
print("============================================")

#3
SERVICIO = 'FONDOS_INVERSION'
while(periodo_hasta >= periodo_desde):
    print("============================================")
    print "PERIODO  {}.".format(periodo_hasta)
    print("============================================")
    periodo_inicial = datetime.strftime(periodo_hasta, "%Y%m%d")  # strftime covertir fecha en numero


    paso +=1

    HORA_INICIAL = datetime.now().now()
    FECHA_PROCESO = time.strftime('%Y%m%d') 

    #5
    impala_shell_execute(config_log['GRABAR_LOG'].format(PROCESO='INICIO_EJECUCION', BD_DESTINO=config_log['bd_final'],PERIODO=periodo_inicial,SERVICIO=SERVICIO,FECHA_PROCESO=FECHA_PROCESO, HORA_INICIO=datetime.now().now(), HORA_FIN=datetime.now().now(), DURACION=0 ))

    
    print "{}. LIMPIARTABLATEMPORAL temp_apertura_fic_trazabilidad_nrt".format(paso)
    impala_shell_execute(config['LIMPIARTABLATEMPORAL'].format(bd_temp=config['bd_temp']))

    EjecutarPaso("SOLICITUDES.sql")

    CANT_REGISTROS =    int(impala_shell_execute(config['CONTARTABLATEMPORAL'].format(bd_temp=config['bd_temp'])).iloc[0]['cantidad'])
    if CANT_REGISTROS == 0:
        print "NO HAY REGISTROS : {}".format(CANT_REGISTROS)
        HORA_FINAL = datetime.now().now()
        DURACION  = HORA_FINAL - HORA_INICIAL
        impala_shell_execute(config_log['GRABAR_LOG'].format(PROCESO='FIN_EJECUCION', BD_DESTINO=config_log['bd_final'],PERIODO=periodo_inicial,SERVICIO=SERVICIO,FECHA_PROCESO=FECHA_PROCESO, HORA_INICIO=HORA_INICIAL, HORA_FIN=HORA_FINAL, DURACION=DURACION ))
        periodo_hasta = periodo_hasta  - timedelta(days=1)
        continue

    EjecutarPaso("JURIDICO.sql")
    EjecutarPaso("CONSULTACLIENTEPNV2.sql")
    EjecutarPaso("PROCESO_ACTUAL.sql")
    EjecutarPaso("MOTIVO_RECHAZO_INGRESO.sql")
    EjecutarPaso("FONDO_SELECCIONADO.sql")
    EjecutarPaso("INDICADOR_PERFIL.sql")
    EjecutarPaso("NOVEDAD_PERFIL_DIRECTA_CLOUD.sql")
    EjecutarPaso("ENCUESTA_CLOUD.sql")
    EjecutarPaso("GENERACIONPEFIL_CLOUD.sql")
    EjecutarPaso("PRODUCTO_ORIGEN_FONDOS.sql")
    EjecutarPaso("PERIODICIDAD_APORTES.sql")
    EjecutarPaso("SRV_SCN_NOTIFICACIONES_MAIL.sql")
    EjecutarPaso("TRANSFERENCIA_CUENTA_FONDOS_COMISIONISTA.sql")
    EjecutarPaso("APERTURA_PRODUCTOS_BANCA_PATRIMONIAL.sql")
    EjecutarPaso("RESULTADO_PERFILAMIENTO.sql")
    EjecutarPaso("SERVICIOS.sql")

    #impala_shell_execute(config['LIMPIARTABLA_DETALLE'].format(bd_temp=config['bd_temp']))

    # EjecutarPaso("DETALLE_BICICLETAS.sql")
    # EjecutarPaso("CANTIDAD_VALOR_ASEGURADO.sql")
    EjecutarPaso("POBLAR_TABLA_FINAL.sql")
    # EjecutarPaso("POBLAR_TABLA_DETALLE.sql")

    
    
    CANT_REGISTROS =    int(impala_shell_execute(config['CONTARTABLATEMPORAL'].format(bd_temp=config['bd_temp'])).iloc[0]['cantidad'])
    print "CANTIDAD DE REGISTROS : {}".format(CANT_REGISTROS)


    print "{}. INVALIDAR METADATA".format(paso)
    impala_shell_execute(config_log['INVALIDAR_METADATA'].format(bd_final=config_log['bd_final'],TABLA='apertura_fic_trazabilidad_nrt' ))


    HORA_FINAL = datetime.now().now()
    DURACION  = HORA_FINAL - HORA_INICIAL
    print "DURACION {}.".format(DURACION)
    #9
    impala_shell_execute(config_log['GRABAR_LOG'].format(PROCESO='FIN_EJECUCION', BD_DESTINO=config_log['bd_final'],PERIODO=periodo_inicial,SERVICIO=SERVICIO,FECHA_PROCESO=FECHA_PROCESO, HORA_INICIO=HORA_INICIAL, HORA_FIN=HORA_FINAL, DURACION=DURACION ))

    periodo_hasta = periodo_hasta  - timedelta(days=1)


#### EJECUCION : ./trazabilidadSoat.py 20191007
#### PARA QUE SE EJECUTE CON LA FECHA DEL DIA ACTUAL, NO MANDAR PARAMETRO
