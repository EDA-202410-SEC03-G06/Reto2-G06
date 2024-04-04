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
 *
 * Contribuciones:
 *
 * Dario Correal - Version inicial
 """


import config as cf
from DISClib.ADT import list as lt
from DISClib.ADT import stack as st
from DISClib.ADT import queue as qu
from DISClib.ADT import map as mp
from DISClib.DataStructures import mapentry as me
from DISClib.Algorithms.Sorting import shellsort as sa
from DISClib.Algorithms.Sorting import insertionsort as ins
from DISClib.Algorithms.Sorting import selectionsort as se
from DISClib.Algorithms.Sorting import mergesort as merg
from DISClib.Algorithms.Sorting import quicksort as quk
assert cf

"""
Se define la estructura de un catálogo de videos. El catálogo tendrá
dos listas, una para los videos, otra para las categorias de los mismos.
"""

# Construccion de modelos


def new_data_structs(tipo):
    """
    Inicializa las estructuras de datos del modelo. Las crea de
    manera vacía para posteriormente almacenar la información.
    title;street;city;country_code;address_text;marker_icon;workplace_type;
    company_name;company_url;company_size;experience_level;published_at;remote_interview;
    open_to_hire_ukrainians;id;display_offer

    """ 
    if tipo == None:
        tipo = 'CHAINING'
        
    catalog = {'skills':None,
               'multi-locations': None,
               'jobs': None,
               'employment-types':None
              }
    
    
    catalog['skills'] = mp.newMap('CHAINING')
    catalog['multi-locations'] = mp.newMap('CHAINING')
    catalog['jobs'] = mp.newMap('CHAINING')
    catalog['employment-types'] = mp.newMap('CHAINING')
    #TODO: Inicializar las estructuras de datos
    return catalog


# Funciones para agregar informacion al modelo

def add_skills(catalog, skills):
    """
    Función para agregar nuevos elementos a la lista
    """
    dup = me.newMapEntry(skills['id'],skills)
    lt.addLast(catalog['skills'],dup)

# Funciones para agregar informacion al modelo

    
    
def add_jobs(catalog, job):
    
    date = job['published_at']
    job['published_at'] = datetime.strptime(date,'%Y-%m-%dT%H:%M:%S.%fZ')
    lt.addLast(catalog['jobs'], job)
    
def add_locations(catalog, location):
    me.newMapEntry()
    
def add_employment_types(catalog,emptype): 
    lt.addLast(catalog['employment-types'], emptype)
# Funciones para creacion de datos
def add_data(data_structs, data):
    """
    Función para agregar nuevos elementos a la lista
    """
    #TODO: Crear la función para agregar elementos a una lista
    pass


# Funciones para creacion de datos

def new_data(id, info):
    """
    Crea una nueva estructura para modelar los datos
    """
    #TODO: Crear la función para estructurar los datos
    pass


# Funciones de consulta

def get_data(data_structs, id):
    """
    Retorna un dato a partir de su ID
    """
    #TODO: Crear la función para obtener un dato de una lista
    pass


def data_size(data_structs):
    """
    Retorna el tamaño de la lista de datos
    """
    #TODO: Crear la función para obtener el tamaño de una lista
    pass


def req_1(catalog, n, pais, expert):
    """
    Función que soluciona el requerimiento 1
    """
    # TODO: Realizar el requerimiento 1
    ofertas = catalog['jobs']
    filtro = lt.newList('ARRAY_LIST')
    total_ofertas=0
    for oferta in lt.iterator(ofertas):
        if oferta['country_code'] ==pais and oferta['experience_level']==expert:
            lt.addLast(filtro, oferta)
            total_ofertas+=1
            if total_ofertas>=n:
                break
            
    filtro_2 = lt.newList('ARRAY_LIST')
    for o in lt.iterator(filtro):
        datos = {'title':o['title'],'company_name':o['company_name'],'experience_level':o['experience_level'],
                 'country_code':o['country_code'],'city':o['city'],'company_size':o['company_size'],
                 'workplace_type':o['workplace_type'], 'open_to_hire_ukrainians':o['open_to_hire_ukrainians']}
        lt.addLast(filtro_2,datos)
    return filtro_2 


def req_2(catalog, n, empresa, ciudad):
    """
    Función que soluciona el requerimiento 2
    """
    # TODO: Realizar el requerimiento 2
    ofertas = catalog['jobs']
    filtro = lt.newList('ARRAY_LIST')
    total_ofertas=0
    for oferta in lt.iterator(ofertas):
        if oferta['city'] ==ciudad and oferta['company_name']==empresa:
            lt.addLast(filtro, oferta)
            total_ofertas+=1
            if total_ofertas>=n:
                break
            
    filtro_2 = lt.newList('ARRAY_LIST')
    for o in lt.iterator(filtro):
        datos = {'published_at':o['published_at'],'country_code':o['country_code'],'city':o['city'],
                 'company_name':o['company_name'],'title':o['title'], 'experience_level':o['experience_level'],
                 'remote_interview':o['remote_interview'],'workplace_type':o['workplace_type']}
        lt.addLast(filtro_2,datos)    
    
    return filtro_2


def req_3(catalog, empresa, fecha_in, fecha_fin):
    """
    Función que soluciona el requerimiento 3
    """
    # TODO: Realizar el requerimiento 3
    ofertas = catalog['jobs']
    final  = lt.newList('ARRAY_LIST')
   

    for oferta in lt.iterator(ofertas):
        if empresa == oferta['company_name']:
            date = oferta['published_at']
            fecha = datetime.strftime(date,'%Y-%m-%d')
            if fecha<=fecha_fin and fecha>=fecha_in:
                lt.addLast(final,oferta)
            elif fecha<fecha_in:
                break
            
    filtro_2 = lt.newList('ARRAY_LIST')
    for o in lt.iterator(final):
        datos = {'published_at':o['published_at'],'title':o['title'],'experience_level':o['experience_level'],
                 'city':o['city'],'country_code':o['country_code'],'company_size':o['company_size'], 
                 'workplace_type':o['workplace_type'],'open_to_hire_ukrainians':o['open_to_hire_ukrainians']}
        lt.addLast(filtro_2,datos)    
    
    ins.sort(filtro_2, sort_criteria_req3)
    print(filtro_2)
            
    return filtro_2


def req_4(catalog, pais, f_inicio, f_fin):
    """
    Función que soluciona el requerimiento 4
    """
    # TODO: Realizar el requerimiento 4
    ofertas = catalog['jobs']
    ofertas_rango = lt.newList('ARRAY_LIST')
    empresas = lt.newList('ARRAY_LIST')
    f_inicio = datetime.strptime(f_inicio,'%Y-%m-%d')
    f_fin = datetime.strptime(f_fin,'%Y-%m-%d')
    ciudades = {}
    for oferta in lt.iterator(ofertas):
        if pais == oferta['country_code']:
            empresa = oferta["company_name"]
            
            fecha_oferta = oferta['published_at']
            fecha_string = datetime.strftime(fecha_oferta,'%Y-%m-%d')
            fecha = datetime.strptime(fecha_string,'%Y-%m-%d')
            if (f_inicio <= fecha) and (fecha <= f_fin):
                remote = oferta['workplace_type']
                if 'remote' in remote:
                    oferta['remote'] = remote
                else:
                    oferta['remote'] = False
                lt.addLast(ofertas_rango, oferta)
                empresa = oferta["company_name"]

                if lt.isPresent(empresas, empresa) == 0:
                    lt.addLast(empresas, empresa)
                
                if oferta['city'] not in ciudades:
                    ciudades[oferta['city']] = 1
                else:
                    ciudades[oferta['city']] +=1
    ciudades_ordenadas = lt.newList('ARRAY_LIST')
    for city in ciudades.keys():
        lt.addLast(ciudades_ordenadas, {'ciudad': city,'count': ciudades[city]})
    merg.sort(ciudades_ordenadas, sort_criteria_req6y7)
    mayor = lt.firstElement(ciudades_ordenadas)
    ciudad_mayor = mayor["ciudad"]
    cuenta_ciudad_mayor = mayor['count']
    menor = lt.lastElement(ciudades_ordenadas)
    ciudad_menor = menor['ciudad']
    cuenta_ciudad_menor = menor['count']
    
    ofertas_rango_criterios = lt.newList('ARRAY_LIST')
    for oferta in lt.iterator(ofertas_rango):
        datos = {'published_at': oferta['published_at'],'title':oferta['title'], 'experience_level': oferta['experience_level'],'company_name': oferta['company_name'],
                 'city': oferta['city'], 'workplace_type': oferta['workplace_type'],'remote': oferta['remote'],'open_to_hire_ukrainians': oferta['open_to_hire_ukrainians']}
        lt.addLast(ofertas_rango_criterios, datos)  
               
    return lt.size(ofertas_rango), lt.size(empresas), lt.size(ciudades_ordenadas), (ciudad_mayor, cuenta_ciudad_mayor),(ciudad_menor,cuenta_ciudad_menor), ofertas_rango_criterios
    


def req_5(catalog, city, fecha_in, fecha_fin):
    """
    Función que soluciona el requerimiento 5
    """
    # TODO: Realizar el requerimiento 5
    ofertas = catalog['jobs']
    ofertas_filtradas  = lt.newList('ARRAY_LIST')
    empresas= lt.newList("ARRAY_LIST")
    mayor_numero_empresas = {}
    numero_empresas_ordenadas = lt.newList("ARRAY_LIST")
 
    for oferta in lt.iterator(ofertas):
        fecha= datetime.strftime(oferta["published_at"], "%Y-%m-%d")
        if city == oferta['city'] and fecha<=fecha_fin and fecha>=fecha_in:
            empresa = oferta["company_name"]
            lt.addLast(ofertas_filtradas,oferta)
            if empresa not in mayor_numero_empresas.keys():
                mayor_numero_empresas[oferta["company_name"]] = 1
            elif empresa in mayor_numero_empresas.keys(): 
                mayor_numero_empresas[oferta["company_name"]] +=1
    cantidad_ofertas= lt.size(ofertas_filtradas)        
                    
                    
    for empresa in mayor_numero_empresas.keys():
        lt.addLast(numero_empresas_ordenadas, {"empresa":empresa, "count":mayor_numero_empresas[empresa]})
    merg.sort(numero_empresas_ordenadas, sort_criteria_req6y7)
    cant_empresas= lt.size(numero_empresas_ordenadas)
    mayor= lt.firstElement(numero_empresas_ordenadas)
    menor= lt.lastElement(numero_empresas_ordenadas)
    
    ultima_respuesta = lt.newList('ARRAY_LIST')
    for llave in lt.iterator(ultima_respuesta):
        datos = {'published_at':llave['published_at'],'title': llave['title'], "company_name": llave["company_name"], 
                   "workplace_type": llave["workplace_type"],'company_size': llave['company_size']} 
                 
        lt.addLast(ultima_respuesta,datos)    
    
    
        
                    
    return (cantidad_ofertas, cant_empresas, mayor, menor, ultima_respuesta)


def req_6(data_structs, n, pais, experience, fecha_in, fecha_fin):
    """
    Función que soluciona el requerimiento 6
    """
    # TODO: Realizar el requerimiento 
    catalog = data_structs['jobs']
    emptypes = data_structs['employment-types']
    ciudades = lt.newList('ARRAY_LIST')
    ofertas = lt.newList('ARRAY_LIST')
    empresas = lt.newList('ARRAY_LIST')
    id_list = lt.newList('ARRAY_LIST')
    city = {}
    cant_empresas = 0
    sal_promedio = 0
    div_salario = 0
#filtrar con pais
    if pais != None: 
        for oferta in lt.iterator(catalog):
         
            if pais == oferta['country_code'] and experience == oferta['experience_level']:
                date = oferta['published_at']
                fecha = datetime.strftime(date,'%Y-%m-%d')
                if fecha<=fecha_fin and fecha>=fecha_in:
                 
                    if oferta['city'] not in city:
                        city[oferta['city']] = 1
                        lt.addLast(ofertas,oferta)     
                        
                    elif oferta['city']  in city:
                        lt.addLast(ofertas,oferta)
                        city[oferta['city']] += 1

#filtrar sin pais        
    else:
        for oferta in lt.iterator(catalog):
            date = oferta['published_at']
            fecha = datetime.strftime(date,'%Y-%m-%d')
            if  experience == oferta['experience_level'] and fecha<=fecha_fin and fecha>=fecha_in:
                    
                    if oferta['city'] not in city:
                        city[oferta['city']] = 1
                        lt.addLast(ofertas,oferta)     
                        
                    elif oferta['city']  in city:
                        lt.addLast(ofertas,oferta)
                        city[oferta['city']] += 1
    
      
       
# sort a ciudades
    for ciudad in city.keys():
        lt.addLast(ciudades,{'city':ciudad,'count':city[ciudad]})     
           
    merg.sort(ciudades,sort_criteria_req6y7)
    lista_de_n_cities = lt.newList('ARRAY_LIST')
    for ciudad in lt.iterator(ciudades):
        if lt.size(lista_de_n_cities)<n:
            lt.addLast(lista_de_n_cities,ciudad['city'])
        else:
            break
    cant_ciudades = lt.size(lista_de_n_cities)
    mayor = lt.firstElement(ciudades)
    sub = lt.subList(ciudades,0,lt.size(lista_de_n_cities)+1)
    menor = lt.lastElement(sub)
    
#lista filtrada con las ciudades
    filtro = lt.newList('ARRAY_LIST')
    for oferta in lt.iterator(ofertas):
        present = lt.isPresent(lista_de_n_cities,oferta['city'])
        if present>0:
            lt.addLast(filtro,oferta)
    total_ofertas = lt.size(filtro)

#contar empresas y sacar id  
    for oferta in lt.iterator(filtro):
        present_empresa = lt.isPresent(empresas,oferta['company_name'])
        if present_empresa==0:
            lt.addLast(empresas,oferta['company_name']) 
            cant_empresas +=1
        lt.addLast(id_list,oferta['id'])
          
    
    #promedio salario
    id_set = set(id_list['elements'])
    if pais!=None:
        for oferta in lt.iterator(emptypes):
            #present_id = lt.isPresent(id_set,oferta['id'])
            if oferta['id'] in id_set and oferta['salary_from']!='':
                sal_promedio+= int(oferta['salary_from'])
                div_salario +=1

    promedio = sal_promedio//div_salario

    
    
    #devolver catalogo ciudades
    catalogo_ciudades = lt.newList('ARRAY_LIST')
    #for ciudad in lt.iterator(sub):
    #    lt.addLast(catalogo_ciudades,{'city':ciudad['city'],'ofertas':ciudad['count']})
        
    return (total_ofertas, cant_ciudades, cant_empresas, mayor, menor, promedio)                                 
    




def req_7(catalog, n, f_inicial, f_final):
    """
    Función que soluciona el requerimiento 7
    """
    # TODO: Realizar el requerimiento 7
    ofertas_jobs = catalog['jobs']
    ofertas_skills = catalog['skills']
    

    f_inicio = datetime.strptime(f_inicial,'%Y-%m-%d')
    f_fin = datetime.strptime(f_final,'%Y-%m-%d')

    ofertas_rango = lt.newList('ARRAY_LIST')
    ofertas_paises = {}
    for oferta in lt.iterator(ofertas_jobs):
        fecha_oferta = oferta['published_at']
        fecha_string = datetime.strftime(fecha_oferta,'%Y-%m-%d')
        fecha = datetime.strptime(fecha_string,'%Y-%m-%d')
        if (( fecha >= f_inicio) and (fecha <= f_fin)):
            lt.addLast(ofertas_rango, oferta)
            pais_oferta = oferta['country_code']
            if pais_oferta not in ofertas_paises:
               ofertas_paises[pais_oferta] = 1
            else:
                ofertas_paises[pais_oferta] += 1
    paises_organizados = lt.newList('ARRAY_LIST')  
    for pais in ofertas_paises.keys():
        lt.addLast(paises_organizados, {'pais': pais,'count': ofertas_paises[pais]})
    merg.sort(paises_organizados, sort_criteria_req6y7)
    top_n = lt.newList('ARRAY_LIST')
    i= 1
    while lt.size(top_n) < n:
        element = lt.getElement(paises_organizados, i)
        lt.addLast(top_n, element['pais'])
        i+=1
    
    pais_mayor = lt.firstElement(top_n)
    cuenta_pais_mayor = ofertas_paises[pais_mayor]
    
    ciudades = {}
    
    ofertas_n_paises = lt.newList()
    for oferta in lt.iterator(ofertas_rango):
        if lt.isPresent(top_n, oferta['country_code']) != 0:
            lt.addLast(ofertas_n_paises, oferta)
            
            if oferta['city'] not in ciudades.keys():
                ciudades[oferta['city']] = 1
            else:
                ciudades[oferta['city']] +=1
    total_ofertas = lt.size(ofertas_n_paises)
# Criterios para retornar en ciudades
    ciudades_ordenadas = lt.newList('ARRAY_LIST')
    for city in ciudades.keys():
        lt.addLast(ciudades_ordenadas, {'ciudad': city,'count': ciudades[city]})
    merg.sort(ciudades_ordenadas, sort_criteria_req6y7)
    numero_ciudades = lt.size(ciudades_ordenadas)
    ciudad_mayor_datos= lt.firstElement(ciudades_ordenadas)
    ciudad_mayor = ciudad_mayor_datos['ciudad']
    cuenta_ciudad_mayor = ciudad_mayor_datos['count']
#Encontrar los skills y organizarlos
    skills_junior = {}
    skills_mid = {}
    skills_senior = {}
    
    suma_nivel_junior = 0
    suma_nivel_mid = 0
    suma_nivel_senior = 0
    
    total_ofertas_junior = 0
    total_ofertas_mid = 0
    total_ofertas_senior = 0
    
    empresas_junior = {}
    empresas_mid = {}
    empresas_senior = {}
    
    skills_id = lt.newList('ARRAY_LIST')
    for skills in lt.iterator(ofertas_skills):
        lt.addLast(skills_id, skills['id'])
        
    
    for oferta in lt.iterator(ofertas_n_paises):
        id_oferta = oferta['id']
        position = lt.isPresent(skills_id, id_oferta)
        elemento = lt.getElement(ofertas_skills, position)

        experiencia = oferta['experience_level']
        empresa = oferta['company_name']
        
        if experiencia == 'senior':
            suma_nivel_senior += int(elemento['level'])
            total_ofertas_senior+=1
            habilidad = elemento['name']
            if habilidad not in skills_senior.keys():
                skills_senior[habilidad]= 1
            else:
                skills_senior[habilidad]+=1
            
            if empresa not in empresas_senior.keys():
                empresas_senior[empresa] = 1
            else:
                empresas_senior[empresa] +=1
                  
        elif experiencia == 'mid':
            suma_nivel_mid += int(elemento['level'])
            total_ofertas_mid +=1
            habilidad = elemento['name']
            if habilidad not in skills_mid.keys():
                skills_mid[habilidad]= 1
            else:
                skills_mid[habilidad]+=1 
                
            if empresa not in empresas_mid.keys():
                empresas_mid[empresa] = 1
            else:
                empresas_mid[empresa] +=1
                
        elif experiencia == 'junior':
            suma_nivel_junior += int(elemento['level'])
            total_ofertas_junior += 1
            habilidad = elemento['name']
            if habilidad not in skills_junior.keys():
                skills_junior[habilidad]= 1
            else:
                skills_junior[habilidad]+=1
            
            if empresa not in empresas_junior.keys():
                empresas_junior[empresa] = 1
            else:
                empresas_junior[empresa] +=1
    
    promedio_junior = suma_nivel_junior//total_ofertas_junior
    promedio_mid = suma_nivel_mid//total_ofertas_mid
    promedio_senior = suma_nivel_senior//total_ofertas_senior
    
    #Requerimientos por experiencia
    skills_senior_ordenadas = lt.newList('ARRAY_LIST')
    for habilidad in skills_senior.keys():
        lt.addLast(skills_senior_ordenadas, {'skill': habilidad,'count': skills_senior[habilidad]})
    merg.sort(skills_senior_ordenadas, sort_criteria_req6y7)
    
    habilidades_diferentes_senior = lt.size(skills_senior_ordenadas)
    habilidad_mas_senior = lt.firstElement(skills_senior_ordenadas)
    habilidad_menos_senior = lt.lastElement(skills_senior_ordenadas)
    
    skills_mid_ordenadas = lt.newList('ARRAY_LIST')
    for habilidad in skills_mid.keys():
        lt.addLast(skills_mid_ordenadas, {'skill': habilidad,'count': skills_mid[habilidad]})
    merg.sort(skills_mid_ordenadas, sort_criteria_req6y7)
    
    habilidades_diferentes_mid = lt.size(skills_mid_ordenadas)
    habilidad_mas_mid = lt.firstElement(skills_mid_ordenadas)
    habilidad_menos_mid = lt.lastElement(skills_mid_ordenadas)
    
    skills_junior_ordenadas = lt.newList('ARRAY_LIST')
    for habilidad in skills_junior.keys():
        lt.addLast(skills_junior_ordenadas, {'skill': habilidad,'count': skills_junior[habilidad]})
    merg.sort(skills_junior_ordenadas, sort_criteria_req6y7)
    
    habilidades_diferentes_junior = lt.size(skills_mid_ordenadas)
    habilidad_mas_junior = lt.firstElement(skills_mid_ordenadas)
    habilidad_menos_junior = lt.lastElement(skills_mid_ordenadas)
    
    # empresas por experiencia
    empresas_senior_ordenadas = lt.newList('ARRAY_LIST')
    for empresa in empresas_senior.keys():
        lt.addLast(empresas_senior_ordenadas, {'empresa': empresa,'count': empresas_senior[empresa]})
    merg.sort(empresas_senior_ordenadas, sort_criteria_req6y7)
    
    empresas_diferentes_senior = lt.size(empresas_senior_ordenadas)
    empresa_mas_senior = lt.firstElement(empresas_senior_ordenadas)
    empresa_menos_senior = lt.lastElement(empresas_senior_ordenadas)
    
    empresas_mid_ordenadas = lt.newList('ARRAY_LIST')
    for empresa in empresas_mid.keys():
        lt.addLast(empresas_mid_ordenadas, {'empresa': empresa,'count': empresas_mid[empresa]})
    merg.sort(empresas_mid_ordenadas, sort_criteria_req6y7)
    
    empresas_diferentes_mid = lt.size(empresas_mid_ordenadas)
    empresa_mas_mid = lt.firstElement(empresas_mid_ordenadas)
    empresa_menos_mid = lt.lastElement(empresas_mid_ordenadas)

    empresas_junior_ordenadas = lt.newList('ARRAY_LIST')
    for empresa in empresas_junior.keys():
        lt.addLast(empresas_junior_ordenadas, {'empresa': empresa,'count': empresas_junior[empresa]})
    merg.sort(empresas_junior_ordenadas, sort_criteria_req6y7)
    
    empresas_diferentes_junior = lt.size(empresas_junior_ordenadas)
    empresa_mas_junior = lt.firstElement(empresas_junior_ordenadas)
    empresa_menos_junior = lt.lastElement(empresas_junior_ordenadas)
    
    senior = (habilidades_diferentes_senior, habilidad_mas_senior, habilidad_menos_senior, promedio_senior, empresas_diferentes_senior, empresa_mas_senior, empresa_menos_senior)
    mid = (habilidades_diferentes_mid, habilidad_mas_mid, habilidad_menos_mid, promedio_mid, empresas_diferentes_mid, empresa_mas_mid, empresa_menos_mid)
    junior = (habilidades_diferentes_junior, habilidad_mas_junior, habilidad_menos_junior, promedio_junior, empresas_diferentes_junior, empresa_mas_junior, empresa_menos_junior)
    return total_ofertas, numero_ciudades, (pais_mayor, cuenta_pais_mayor), (ciudad_mayor, cuenta_ciudad_mayor), senior, mid, junior



def req_8(data_structs):
    """
    Función que soluciona el requerimiento 8
    """
    # TODO: Realizar el requerimiento 8
    pass


# Funciones utilizadas para comparar elementos dentro de una lista

def compare(data_1, data_2):
    """
    Función encargada de comparar dos datos
    """
    #TODO: Crear función comparadora de la lista
    pass

# Funciones de ordenamiento


def sort_criteria(data_1, data_2):
    """sortCriteria criterio de ordenamiento para las funciones de ordenamiento

    Args:
        data1 (_type_): _description_
        data2 (_type_): _description_

    Returns:
        _type_: _description_
    """
    #TODO: Crear función comparadora para ordenar
    return data_1["published_at"] > data_2["published_at"]


def sort(data_structs):
    """
    Función encargada de ordenar la lista con los datos
    """
    #TODO: Crear función de ordenamiento
    return merg.sort(data_structs["jobs"], sort_criteria)

def sort_criteria_req3(data_1,data_2):
    if data_1['published_at']==data_2['published_at']:
        return data_1['country_code'] < data_2['country_code']
    else:
        return data_1["published_at"] > data_2["published_at"]


def sort_criteria_req6y7(data_1,data_2):
    return data_1['count']>data_2['count']


def sort_criteria_req3(data_1,data_2):
    if data_1['published_at']==data_2['published_at']:
        return data_1['country_code'] < data_2['country_code']
    else:
        return data_1["published_at"] > data_2["published_at"]


def sort_criteria_req6y7(data_1,data_2):
    return data_1['count']>data_2['count']
