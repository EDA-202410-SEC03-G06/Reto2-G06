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


from datetime import datetime as datetime
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
    
    
    catalog['skills'] = mp.newMap(10000,
                                   maptype='CHAINING',
                                   loadfactor=4,
                                   cmpfunction=compareMapBookIds)

    catalog['multi-locations'] = mp.newMap(10000,
                                   maptype='CHAINING',
                                   loadfactor=4,
                                   cmpfunction=compareMapBookIds
                                   )

    catalog['jobs'] = mp.newMap(10000,
                                   maptype='CHAINING',
                                   loadfactor=4,
                                   cmpfunction=compareMapBookIds
                                   )

    catalog['employment-types'] = mp.newMap(10000,
                                   maptype='CHAINING',
                                   loadfactor=4,
                                   cmpfunction=compareMapBookIds
                                   )

    #TODO: Inicializar las estructuras de datos
    return catalog


# Funciones para agregar informacion al modelo

def add_skills(catalog, skills):
    """
    Función para agregar nuevos elementos a la lista
    """
    mp.put(catalog['skills'],skills['id'],skills)

# Funciones para agregar informacion al modelo

    
    
def add_jobs(catalog, job):
    
    date = job['published_at']
    job['published_at'] = datetime.strptime(date,'%Y-%m-%dT%H:%M:%S.%fZ')
    mp.put(catalog['jobs'],job['id'],job)
    
def add_locations(catalog, location):
    mp.put(catalog['multi-locations'],location['id'],location)
    
def add_employment_types(catalog,emptype): 
    mp.put(catalog['employment-types'],emptype['id'],emptype)
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
    keyset = mp.keySet(data_structs)
    return lt.size(keyset)

def req_1(catalog, n, pais, expert):
    """
    Función que soluciona el requerimiento 1
    """
    # TODO: Realizar el requerimiento 1
    ofertas = mp.valueSet(catalog['jobs'])
    filtro = lt.newList('ARRAY_LIST')
    
    total_ofertas_pais=0
    total_ofertas_condicion=0
    for oferta in lt.iterator(ofertas):
        if oferta["country_code"] == pais:
            total_ofertas_pais+=1
        if oferta['country_code'] ==pais and oferta['experience_level']==expert:
            lt.addLast(filtro, oferta)
            total_ofertas_condicion+=1
            if total_ofertas_condicion>=n:
                break
    
        
    filtro_2 = mp.newMap()
    for o in lt.iterator(filtro):
        datos = {'published_at': o['published_at'],'title':o['title'],'company_name':o['company_name'],'experience_level':o['experience_level'],
                 'country_code':o['country_code'],'city':o['city'],'company_size':o['company_size'],
                 'workplace_type':o['workplace_type'], 'open_to_hire_ukrainians':o['open_to_hire_ukrainians']}
        if lt.size(filtro_2) > 10:
            final= (datos[0:5], datos[-5:-1])
        else:
            final= datos
        mp.put(filtro_2,o["id"],final)
    
    return (total_ofertas_pais, total_ofertas_condicion, filtro_2 )

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
    ofertas = mp.valueSet(catalog['jobs'])
    final  = lt.newList('ARRAY_LIST')
    fecha_in = datetime.strptime(fecha_in,'%Y-%m-%d')
    fecha_fin = datetime.strptime(fecha_fin,'%Y-%m-%d')

    for oferta in lt.iterator(ofertas):
        if empresa == oferta['company_name']:
            fecha_oferta = oferta['published_at']
            fecha_string = datetime.strftime(fecha_oferta,'%Y-%m-%d')
            fecha = datetime.strptime(fecha_string,'%Y-%m-%d')
            if fecha<=fecha_fin and fecha>=fecha_in:
                lt.addLast(final,oferta)
    
    filtro_2 = mp.newMap()
    keys = mp.newMap()
    junior = 0
    mid = 0
    senior = 0
    for o in lt.iterator(final):
        datos = {'published_at':o['published_at'],'title':o['title'],'experience_level':o['experience_level'],
                 'city':o['city'],'country_code':o['country_code'],'company_size':o['company_size'], 
                 'workplace_type':o['workplace_type'],'open_to_hire_ukrainians':o['open_to_hire_ukrainians']}
        mp.put(filtro_2,o['id'], datos)
        if o['experience_level']=='junior':
            junior +=1
        elif o['experience_level']=='mid':
            mid +=1
        elif o['experience_level']=='senior':
            senior +=1
            
    mp.put(keys,'junior',junior)
    mp.put(keys,'mid',mid)
    mp.put(keys,'senior',senior)

    return filtro_2, keys


def req_4(catalog, pais, f_inicio, f_fin):
    """
    Función que soluciona el requerimiento 4
    """
    # TODO: Realizar el requerimiento 4
    ofertas = catalog['jobs']
    ofertas_rango = lt.newList('ARRAY_LIST')
    empresas = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    f_inicio = datetime.strptime(f_inicio,'%Y-%m-%d')
    f_fin = datetime.strptime(f_fin,'%Y-%m-%d')
    ciudades = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    ofertas_lista = mp.valueSet(ofertas)
    
    for oferta in lt.iterator(ofertas_lista):
        if pais == oferta['country_code']:
            empresa = oferta["company_name"]
            
            fecha_oferta = oferta['published_at']
            fecha_string = datetime.strftime(fecha_oferta,'%Y-%m-%d')
            fecha = datetime.strptime(fecha_string,'%Y-%m-%d')
            if (f_inicio <= fecha) and (fecha <= f_fin):
                ciudad = oferta['city']
                remote = oferta['workplace_type']
                if 'remote' in remote:
                    oferta['remote'] = remote
                else:
                    oferta['remote'] = False
                lt.addLast(ofertas_rango, oferta)
                empresa = oferta["company_name"]

                if mp.contains(empresas, empresa) == False:
                    mp.put(empresas, empresa, True)
                
                if mp.contains(ciudades, ciudad) == False:
                    mp.put(ciudades, ciudad, 1)
                else:
                    tupla_city = mp.get(ciudades, oferta['city'])
                    cantidad_ciudad = me.getValue(tupla_city)
                    cantidad_ciudad +=1
                    me.setValue(tupla_city, cantidad_ciudad)
                    
                    
    ciudades_ordenadas = lt.newList('ARRAY_LIST')
    ciudades_lista = mp.keySet(ciudades)
    for city in lt.iterator(ciudades_lista):
        tupla = mp.get(ciudades, city)
        valor = me.getValue(tupla)
        city_name = me.getKey(tupla)
        lt.addLast(ciudades_ordenadas, {'ciudad': city_name,'count': valor})
    merg.sort(ciudades_ordenadas, sort_criteria_req6y7)
    mayor = lt.firstElement(ciudades_ordenadas)
    ciudad_mayor = mayor["ciudad"]
    cuenta_ciudad_mayor = mayor['count']
    menor = lt.lastElement(ciudades_ordenadas)
    ciudad_menor = menor['ciudad']
    cuenta_ciudad_menor = menor['count']
    empresas_size = mp.keySet(empresas)
    ofertas_rango_criterios = lt.newList('ARRAY_LIST')
    for oferta in lt.iterator(ofertas_rango):
        datos = {'published_at': oferta['published_at'],'title':oferta['title'], 'experience_level': oferta['experience_level'],'company_name': oferta['company_name'],
                 'city': oferta['city'], 'workplace_type': oferta['workplace_type'],'remote': oferta['remote'],'open_to_hire_ukrainians': oferta['open_to_hire_ukrainians']}
        lt.addLast(ofertas_rango_criterios, datos)  
               
    return lt.size(ofertas_rango), lt.size(empresas_size), lt.size(ciudades_ordenadas), (ciudad_mayor, cuenta_ciudad_mayor),(ciudad_menor,cuenta_ciudad_menor), ofertas_rango_criterios
    


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


def req_6(data_structs, n, experience, fecha):
    """
    Función que soluciona el requerimiento 6
    """
    # TODO: Realizar el requerimiento 
    catalog = mp.valueSet(data_structs['jobs'])
    #emptypes = mp.valueSet(data_structs['employment-types'])
    ciudades = lt.newList('ARRAY_LIST')
    ofertas = lt.newList('ARRAY_LIST')
    empresas = lt.newList('ARRAY_LIST')
    id_list = lt.newList('ARRAY_LIST')
    city = mp.newMap()
    cant_empresas = 0
    sal_promedio = 0
    div_salario = 0
#filtrar con pais
    if experience!='indiferente':
        for oferta in lt.iterator(catalog):
            
            if experience == oferta['experience_level']:
                date = oferta['published_at']
                fecha_oferta = datetime.strftime(date,'%Y')
                if fecha_oferta==fecha:
                    
                        ispresent = mp.contains(city,oferta['city'])
                        if ispresent==False:
                            mp.put(city,oferta['city'],1)
                            lt.addLast(ofertas,oferta)     
                            
                        if ispresent:
                            pareja = mp.get(city,oferta['city'])
                            valor = me.getValue(pareja)
                            valor +=1
                            me.setValue(pareja, valor)
                            
    elif experience=='indiferente':
        for oferta in lt.iterator(catalog):
                date = oferta['published_at']
                fecha_oferta = datetime.strftime(date,'%Y')
                if fecha_oferta==fecha:
                    
                        ispresent = mp.contains(city,oferta['city'])
                        if ispresent==False:
                            mp.put(city,oferta['city'],1)
                            lt.addLast(ofertas,oferta)     
                            
                        if ispresent:
                            pareja = mp.get(city,oferta['city'])
                            valor = me.getValue(pareja)
                            valor +=1
                            me.setValue(pareja, valor)
        
       
# sort a ciudades
    city_keys = mp.keySet(city)
    for ciudad in lt.iterator(city_keys):
        pareja = mp.get(city,ciudad)
        count = me.getValue(pareja)
        lt.addLast(ciudades,{'city':ciudad,'count':count})     
           
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
    filtro = mp.newMap()
    total_ofertas = 0
    for oferta in lt.iterator(ofertas):
        present = lt.isPresent(lista_de_n_cities,oferta['city'])
        if present>0:
            if (mp.contains(filtro,oferta['city']))==True:
                pareja = mp.get(filtro,oferta['city'])
                mapa = me.getValue(pareja)
                mp.put(mapa,oferta['id'],oferta)
            if (mp.contains(filtro,oferta['city']))==False:
                new_map = mp.newMap()
                mp.put(new_map,oferta['id'],oferta)
                mp.put(filtro,oferta['city'],new_map)
                
            total_ofertas+=1
            present_empresa = lt.isPresent(empresas,oferta['company_name'])
            if present_empresa==0:
                lt.addLast(empresas,oferta['company_name']) 
                cant_empresas +=1
            
    

#contar empresas y sacar id  
 
#lista de ciudades
    lista_c = mp.newMap()
    for ciudad in lt.iterator(lista_de_n_cities):  
        pareja_dict = mp.get(filtro,ciudad)  
        valores = me.getValue(pareja_dict)
        values = mp.valueSet(valores)
        div_salario = 1
        num_empresas = 0
        total = 0
        mejor = 0
        mejor_id =''
        peor = 999999
        peor_id =''
        pais = ''
        empresas_ciudad = lt.newList('ARRAY_LIST')
        for oferta in lt.iterator(values):
            #contar empresas en total

            #contar empresas por ciudad
            ciudad_present = lt.isPresent(empresas_ciudad,oferta['company_name'])
            if ciudad_present==0:
                lt.addLast(empresas_ciudad,oferta['company_name']) 
                num_empresas +=1
            #contar promedio
            pareja = mp.get(data_structs['employment-types'],oferta['id'])
            emp= me.getValue(pareja)
            if emp['salary_from']!='':
                sal_promedio+= ( ( int(emp['salary_from'])+ int(emp['salary_to'])) )/2
                div_salario +=1
                if int(emp['salary_from'])<peor:
                    peor = int(emp['salary_from'])
                    peor_id = emp['id']
                if int(emp['salary_from'])>mejor:
                    mejor = int(emp['salary_to'])
                    mejor_id = emp['id']

            total +=1
            pais = oferta['country_code']
            
        info = mp.newMap()    
        mp.put(info,'city',ciudad)
        mp.put(info,'country',pais)
        mp.put(info,'promedio',(sal_promedio/div_salario))
        mp.put(info,'total',total)
        mp.put(info,'company_num',num_empresas)
        pp = mp.get(data_structs['jobs'],peor_id)
        mp.put(info,'salary_from',pp)
        pm = mp.get(data_structs['jobs'],mejor_id)
        mp.put(info,'salary_from',pm)
        mp.put(lista_c,ciudad,info)
    return (total_ofertas, cant_ciudades, cant_empresas, mayor, menor, lista_c)                                 
    




def req_7(catalog, n, año, mes):
    """
    Función que soluciona el requerimiento 7
    """
    # TODO: Realizar el requerimiento 7
    ofertas_jobs = catalog['jobs']
    ofertas_skills = catalog['skills']
    
    string_fecha = str(año+'-'+mes)
    

    ofertas_rango = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    ofertas_paises = mp.newMap(100,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    ofertas_jobs_id = mp.keySet(ofertas_jobs)
    
    for oferta_id in lt.iterator(ofertas_jobs_id):
        pareja_oferta = mp.get(ofertas_jobs, oferta_id)
        oferta = me.getValue(pareja_oferta)
        
        fecha_oferta = oferta['published_at']
        fecha_string = datetime.strftime(fecha_oferta,'%Y-%m')
        
        if fecha_string == string_fecha:
            mp.put(ofertas_rango, oferta_id, oferta)
            pais_oferta = oferta['country_code']
            if mp.contains(ofertas_paises, pais_oferta) == False:
                mp.put(ofertas_paises, pais_oferta, 1)
            else:
                tupla_pais = mp.get(ofertas_paises, pais_oferta)
                cantidad_pais = me.getValue(tupla_pais)+1
                me.setValue(tupla_pais, cantidad_pais)
    
    paises_ordenados = lt.newList('ARRAY_LIST')
    paises_lista = mp.keySet(ofertas_paises)
    for country in lt.iterator(paises_lista):
        tupla_pais = mp.get(ofertas_paises, country)
        valor_pais = me.getValue(tupla_pais)
        country_name = me.getKey(tupla_pais)
        lt.addLast(paises_ordenados, {'pais': country_name,'count': valor_pais})
    merg.sort(paises_ordenados, sort_criteria_req6y7)
    
    info_pais_mayor = lt.firstElement(paises_ordenados)
    pais_mayor =  info_pais_mayor['pais']
    cuenta_pais_mayor = info_pais_mayor['count']
    
    ciudades = mp.newMap(500,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    
    ofertas_n_paises = mp.newMap(5000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    top_n = mp.newMap(n,
            maptype='CHAINING',
            loadfactor=4,
            cmpfunction=compareMapBookIds
            )
    cuenta_top_n = 0
    for country in lt.iterator(paises_ordenados):
        if cuenta_top_n < n:
            pais = country['pais']
            cuenta = country['count']
            mp.put(top_n, pais, cuenta)
            
        else:
            break
        cuenta_top_n+=1
    total_ofertas = 0
    ofertas_rango_id = mp.keySet(ofertas_rango)
    for oferta_id in lt.iterator(ofertas_rango_id):
        pareja_oferta = mp.get(ofertas_jobs, oferta_id)
        oferta = me.getValue(pareja_oferta)
        
        pais_oferta = oferta['country_code']
        if mp.contains(top_n, pais_oferta) == True:
            mp.put(ofertas_n_paises, oferta_id, oferta)
            total_ofertas +=1
    
    ofertas_n_paises_keys = mp.keySet(ofertas_n_paises)
    for id in lt.iterator(ofertas_n_paises_keys):
        pareja_oferta_pais = mp.get(ofertas_n_paises, id)
        oferta = me.getValue(pareja_oferta_pais)
        ciudad = oferta['city']
        
        if mp.contains(ciudades, ciudad) == False:
            mp.put(ciudades, ciudad, 1)
        else:
            tupla_city = mp.get(ciudades, oferta['city'])
            cantidad_ciudad = me.getValue(tupla_city)
            cantidad_ciudad +=1
            me.setValue(tupla_city, cantidad_ciudad)
                    
                    
    ciudades_ordenadas = lt.newList('ARRAY_LIST')
    ciudades_lista = mp.keySet(ciudades)
    for city in lt.iterator(ciudades_lista):
        tupla = mp.get(ciudades, city)
        valor = me.getValue(tupla)
        city_name = me.getKey(tupla)
        lt.addLast(ciudades_ordenadas, {'ciudad': city_name,'count': valor})
    merg.sort(ciudades_ordenadas, sort_criteria_req6y7)
    
    numero_ciudades = lt.size(ciudades_lista)
    mayor = lt.firstElement(ciudades_ordenadas)
    ciudad_mayor = mayor["ciudad"]
    cuenta_ciudad_mayor = mayor['count']
#Encontrar los skills y organizarlos
    skills_junior = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    skills_mid = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    skills_senior = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    
    suma_nivel_junior = 0
    suma_nivel_mid = 0
    suma_nivel_senior = 0
    
    total_ofertas_junior = 0
    total_ofertas_mid = 0
    total_ofertas_senior = 0
    
    empresas_junior = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    empresas_mid = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    empresas_senior = mp.newMap(1000,
                         maptype='CHAINING',
                         loadfactor=4,
                         cmpfunction=compareMapBookIds
                         )
    ofertas_n_paises_id = mp.keySet(ofertas_n_paises)
    for oferta_id in lt.iterator(ofertas_n_paises_id):
        pareja_skill = mp.get(ofertas_skills, oferta_id)
        skill = me.getValue(pareja_skill)
        oferta_pareja = mp.get(ofertas_jobs, oferta_id)
        oferta = me.getValue(oferta_pareja)
        
        experiencia = oferta['experience_level']
        
        
        if experiencia == 'senior':
            empresa = oferta['company_name']
            suma_nivel_senior += int(skill['level'])
            total_ofertas_senior+=1
            habilidad = skill['name']
            #habilidades senior
            if mp.contains(skills_senior, habilidad)==False:
                mp.put(skills_senior, habilidad, 1)
            else:
                pareja_senior = mp.get(skills_senior, habilidad)
                valor_senior = me.getValue(pareja_senior)
                valor_senior+=1
                me.setValue(pareja_senior, valor_senior)
            #Empresas senior    
            if mp.contains(empresas_senior, empresa) == False:
                mp.put(empresas_senior, empresa, 1)
            else:
                pareja_empresa_senior = mp.get(empresas_senior, empresa)
                valor_empresa_senior = me.getValue(pareja_empresa_senior)
                valor_empresa_senior+=1
                me.setValue(pareja_empresa_senior, valor_empresa_senior)
                  
        #mid
        if experiencia == 'mid':
            empresa = oferta['company_name']
            suma_nivel_mid += int(skill['level'])
            total_ofertas_mid+=1
            habilidad = skill['name']
            #habilidades mid
            if mp.contains(skills_mid, habilidad)==False:
                mp.put(skills_mid, habilidad, 1)
            else:
                pareja_mid = mp.get(skills_mid, habilidad)
                valor_mid = me.getValue(pareja_mid)+1
                me.setValue(pareja_mid, valor_mid)
            #Empresas mid   
            if mp.contains(empresas_mid, empresa) == False:
                mp.put(empresas_mid, empresa, 1)
            else:
                pareja_empresa_mid = mp.get(empresas_mid, empresa)
                valor_empresa_mid = me.getValue(pareja_empresa_mid)+1
                me.setValue(pareja_empresa_mid, valor_empresa_mid)
        
        #junior
        if experiencia == 'junior':
            empresa = oferta['company_name']
            suma_nivel_junior += int(skill['level'])
            total_ofertas_junior+=1
            habilidad = skill['name']
            #habilidades junior
            if mp.contains(skills_junior, habilidad)==False:
                mp.put(skills_junior, habilidad, 1)
            else:
                pareja_junior = mp.get(skills_junior, habilidad)
                valor_junior = me.getValue(pareja_junior)+1
                me.setValue(pareja_junior, valor_junior)
            #Empresas junior    
            if mp.contains(empresas_junior, empresa) == False:
                mp.put(empresas_junior, empresa, 1)
            else:
                pareja_empresa_junior = mp.get(empresas_junior, empresa)
                valor_empresa_junior = me.getValue(pareja_empresa_junior)+1
                me.setValue(pareja_empresa_junior, valor_empresa_junior)
    
    promedio_junior = suma_nivel_junior//total_ofertas_junior
    promedio_mid = suma_nivel_mid//total_ofertas_mid
    promedio_senior = suma_nivel_senior//total_ofertas_senior
    
    #Requerimientos por experiencia
    skills_senior_ordenadas = lt.newList('ARRAY_LIST')
    skills_senior_keys = mp.keySet(skills_senior)
    
    for habilidad in lt.iterator(skills_senior_keys):
        pareja_senior_skill = mp.get(skills_senior, habilidad)
        valor_senior_skill = me.getValue(pareja_senior_skill)
        lt.addLast(skills_senior_ordenadas, {'skill': habilidad,'count': valor_senior_skill})
    merg.sort(skills_senior_ordenadas, sort_criteria_req6y7)
    
    habilidades_diferentes_senior = lt.size(skills_senior_ordenadas)
    habilidad_mas_senior = lt.firstElement(skills_senior_ordenadas)
    habilidad_menos_senior = lt.lastElement(skills_senior_ordenadas)
    
    skills_mid_ordenadas = lt.newList('ARRAY_LIST')
    skills_mid_keys = mp.keySet(skills_mid)
    for habilidad in lt.iterator(skills_mid_keys):
        pareja_mid_skill = mp.get(skills_mid, habilidad)
        valor_mid_skill = me.getValue(pareja_mid_skill)
        lt.addLast(skills_mid_ordenadas, {'skill': habilidad,'count': valor_mid_skill})
    merg.sort(skills_mid_ordenadas, sort_criteria_req6y7)
    
    habilidades_diferentes_mid = lt.size(skills_mid_ordenadas)
    habilidad_mas_mid = lt.firstElement(skills_mid_ordenadas)
    habilidad_menos_mid = lt.lastElement(skills_mid_ordenadas)
    
    skills_junior_ordenadas = lt.newList('ARRAY_LIST')
    skills_junior_keys = mp.keySet(skills_junior)
    for habilidad in lt.iterator(skills_junior_keys):
        pareja_junior_skill = mp.get(skills_junior, habilidad)
        valor_junior_skill = me.getValue(pareja_junior_skill)
        lt.addLast(skills_junior_ordenadas, {'skill': habilidad,'count': valor_junior_skill})
    merg.sort(skills_junior_ordenadas, sort_criteria_req6y7)
    
    habilidades_diferentes_junior = lt.size(skills_mid_ordenadas)
    habilidad_mas_junior = lt.firstElement(skills_mid_ordenadas)
    habilidad_menos_junior = lt.lastElement(skills_mid_ordenadas)
    
    # empresas por experiencia
    empresas_senior_ordenadas = lt.newList('ARRAY_LIST')
    empresas_senior_keys = mp.keySet(empresas_senior)
    for empresa in lt.iterator(empresas_senior_keys):
        pareja = mp.get(empresas_senior, empresa)
        valor_senior_empresa = me.getValue(pareja)
        lt.addLast(empresas_senior_ordenadas, {'empresa': empresa,'count': valor_senior_empresa})
    merg.sort(empresas_senior_ordenadas, sort_criteria_req6y7)
    
    empresas_diferentes_senior = lt.size(empresas_senior_ordenadas)
    empresa_mas_senior = lt.firstElement(empresas_senior_ordenadas)
    empresa_menos_senior = lt.lastElement(empresas_senior_ordenadas)
    
    empresas_mid_ordenadas = lt.newList('ARRAY_LIST')
    empresas_mid_keys = mp.keySet(empresas_mid)
    for empresa in lt.iterator(empresas_mid_keys):
        pareja = mp.get(empresas_mid, empresa)
        valor_mid_empresas = me.getValue(pareja)
        lt.addLast(empresas_mid_ordenadas, {'empresa': empresa,'count': valor_mid_empresas})
    merg.sort(empresas_mid_ordenadas, sort_criteria_req6y7)
    
    empresas_diferentes_mid = lt.size(empresas_mid_ordenadas)
    empresa_mas_mid = lt.firstElement(empresas_mid_ordenadas)
    empresa_menos_mid = lt.lastElement(empresas_mid_ordenadas)

    empresas_junior_ordenadas = lt.newList('ARRAY_LIST')
    empresas_junior_keys = mp.keySet(empresas_junior)
    for empresa in lt.iterator(empresas_junior_keys):
        pareja = mp.get(empresas_junior, empresa)
        valor_junior_empresa = me.getValue(pareja)
        lt.addLast(empresas_junior_ordenadas, {'empresa': empresa,'count': valor_junior_empresa})
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

def compareMapBookIds(id, entry):
    """
    Compara dos ids de libros, id es un identificador
    y entry una pareja llave-valor
    """
    identry = me.getKey(entry)
    if id == identry:
        return 0
    elif id > identry:
        return 1
    else:
        return -1