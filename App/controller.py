"""
 * Copyright 2020, Departamento de sistemas y Computación,
 * Universidad de Los Andes
 *
 *
 * Desarrolado para el curso ISIS1225 - Estructuras de Datos y Algoritmos
 *
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along withthis program.  If not, see <http://www.gnu.org/licenses/>.
 """

import config as cf
import model
import time
import csv
import tracemalloc

"""
El controlador se encarga de mediar entre la vista y el modelo.
"""


def new_controller(tipo):
    """
    Crea una instancia del modelo
    """
    #TODO: Llamar la función del modelo que crea las estructuras de datos
    control = {
        'model': None
    }
    control['model'] = model.new_data_structs(tipo)
    return control


# Funciones para la carga de datos

def load_data(control,size_archivo):
    """
    Carglos datos del reto
    """
    if size_archivo == 1:
        arc = "10-por"
    elif size_archivo ==2:
        arc = "20-por"
    elif size_archivo ==3:
        arc = "small"
    elif size_archivo == 4: 
        arc= "80-por"
    else: 
        arc = "large"
        
    
    skills = load_skills(control['model'], arc)
    jobs = load_jobs(control["model"], arc)
    locations = load_locations(control['model'], arc)
    employments = load_employment_type(control['model'], arc)
    return (skills, jobs, locations, employments)

def load_skills(catalog,arc):
    booksfile = cf.data_dir + str(arc+"-skills.csv")
    input_file = csv.DictReader(open(booksfile, encoding="utf-8"),delimiter=";")
    for skill in input_file:
        model.add_skills(catalog,skill)
   
    return model.data_size(catalog["skills"])
    
def load_jobs(catalog,arc):
    booksfile = cf.data_dir + str(arc+"-jobs.csv")
    input_file = csv.DictReader(open(booksfile, encoding="utf-8"),delimiter=";")
    for job in input_file:
        model.add_jobs(catalog,job)  
    
    #model.sort(catalog)
    return model.data_size(catalog['jobs'])

def load_locations(catalog,arc):
        
    booksfile = cf.data_dir + str(arc+"-multilocations.csv")
        
    input_file = csv.DictReader(open(booksfile, encoding="utf-8"),delimiter=";")
    for multilocation in input_file:
        model.add_locations(catalog, multilocation)

    return model.data_size(catalog['multi-locations'])

def load_employment_type(catalog,arc):
   
    booksfile = cf.data_dir + str(arc+"-employments_types.csv")
    input_file = csv.DictReader(open(booksfile, encoding="utf-8"),delimiter=";")
    for employment in input_file:
        model.add_employment_types(catalog, employment)
    return model.data_size(catalog['employment-types'])


# Funciones de ordenamiento

def sort(control):
    """
    Ordena los datos del modelo
    """
    #TODO: Llamar la función del modelo para ordenar los datos
    pass


# Funciones de consulta sobre el catálogo

def get_data(control, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Llamar la función del modelo para obtener un dato
    pass


def req_1(control,n,pais,exp):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    start_time = get_time()
    lista = model.req_1(control['model'],n,pais,exp)
    end_time = get_time()
    deltaTime = delta_time(start_time, end_time)
    print(deltaTime,"[ms]")
    size = model.data_size(lista)
    
    return size, lista


def req_2(control, n , empresa, city):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    start_time = get_time()
    lista = model.req_2(control['model'],n , empresa, city)
    end_time = get_time()
    deltaTime = delta_time(start_time, end_time)
    print(deltaTime,"[ms]")
    size = model.data_size(lista)

    return size, lista 


def req_3(control,empresa,fecha_in,fecha_fin):
    """
    Retorna el resultado del requerimiento 3
    Número total de ofertas.
• Número total de ofertas con experticia junior.
• Número total de ofertas con experticia mid.
• Número total de ofertas con experticia senior
    """
    # TODO: Modificar el requerimiento 3
    start_time = get_time()
   # lista, keys = model.req_3(control['model'],empresa,fecha_in,fecha_fin)
    lista, keys = model.req_3(control['model'],'Bitfinex','2005-10-10','2023-10-10')

    end_time = get_time()
    deltaTime = delta_time(start_time, end_time)
    print(deltaTime,"[ms]")
    size = model.data_size(lista)
    junior = model.mp.get(keys,'junior') 
    mid = model.mp.get(keys,'mid')
    senior= model.mp.get(keys,'senior')
  
        
    return size, junior, mid, senior, lista
    


def req_4(control, country, f_inicio, f_fin):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    start_time = get_time()
    ofertas = model.req_4(control['model'], country, f_inicio, f_fin)
    end_time = get_time()
    deltaTime =delta_time(start_time,end_time)
    print(deltaTime, "[ms]")
    return ofertas
   

def req_5(catalog, city, fecha_in, fecha_fin):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    prueba= model.req_5(catalog["model"], city, fecha_in, fecha_fin)
    
    return prueba

def req_6(control,n,exp,fecha):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    start_time = get_time()
    #ofertas = model.req_6(control['model'],n,exp,fecha)
    ofertas = model.req_6(control['model'],20,'mid','2022')

    end_time = get_time()
    deltaTime = delta_time(start_time, end_time)
    print(deltaTime,"[ms]")

    return ofertas




def req_7(control, n, f_inicio, f_fin):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    start_time = get_time()
    ofertas = model.req_7(control['model'], n, f_inicio, f_fin)
    end_time = get_time()
    deltaTime =delta_time(start_time,end_time)
    print(deltaTime, "[ms]")
    return ofertas

def req_8(control):
    """
    Retorna el resultado del requerimiento 8
    """
    # TODO: Modificar el requerimiento 8
    pass

# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed

def get_memory():
    """
    toma una muestra de la memoria alocada en instante de tiempo
    """
    return tracemalloc.take_snapshot()


def delta_memory(stop_memory, start_memory):
    """
    calcula la diferencia en memoria alocada del programa entre dos
    instantes de tiempo y devuelve el resultado en bytes (ej.: 2100.0 B)
    """
    memory_diff = stop_memory.compare_to(start_memory, "filename")
    delta_memory = 0.0

    # suma de las diferencias en uso de memoria
    for stat in memory_diff:
        delta_memory = delta_memory + stat.size_diff
    # de Byte -> kByte
    delta_memory = delta_memory/1024.0
    return delta_memory
