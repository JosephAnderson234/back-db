from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
CORS(app)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuraciones de bases de datos
DB_CONFIGS = {
    'railway': {
        'host': os.getenv('RAILWAY_HOST'),
        'port': os.getenv('RAILWAY_PORT'),
        'database': os.getenv('RAILWAY_DATABASE'),
        'user': os.getenv('RAILWAY_USER'),
        'password': os.getenv('RAILWAY_PASSWORD'),
        'schema': 'public'
    },
    'colegio2': {
        'host': os.getenv('COLEGIO2_HOST'),
        'port': os.getenv('COLEGIO2_PORT'),
        'database': os.getenv('COLEGIO2_DATABASE'),
        'user': os.getenv('COLEGIO2_USER'),
        'password': os.getenv('COLEGIO2_PASSWORD'),
        'schema': 'cole2'
    },
    'colegio3': {
        'host': os.getenv('COLEGIO3_HOST'),
        'port': os.getenv('COLEGIO3_PORT'),
        'database': os.getenv('COLEGIO3_DATABASE'),
        'user': os.getenv('COLEGIO3_USER'),
        'password': os.getenv('COLEGIO3_PASSWORD'),
        'schema': 'cole3'
    },
    'colegio4': {
        'host': os.getenv('COLEGIO4_HOST'),
        'port': os.getenv('COLEGIO4_PORT'),
        'database': os.getenv('COLEGIO4_DATABASE'),
        'user': os.getenv('COLEGIO4_USER'),
        'password': os.getenv('COLEGIO4_PASSWORD'),
        'schema': 'cole4'
    }
}

# Consultas preestablecidas
PREDEFINED_QUERIES = {
    'top_students': """
        SELECT
            da.dni_alumno,
            da.nombre,
            ROUND(AVG(n.valor), 2) AS promedio_general,
            COUNT(DISTINCT n.tipo_evaluacion || '-' || n.codigo_asignatura) AS cantidad_evaluaciones
        FROM
            {schema}.nota n
        JOIN
            {schema}.datosalumno da ON n.dni_alumno = da.dni_alumno
        GROUP BY
            da.dni_alumno, da.nombre
        HAVING
            COUNT(DISTINCT n.tipo_evaluacion || '-' || n.codigo_asignatura) >= 5
        ORDER BY
            promedio_general DESC
        LIMIT 10;
    """,
    
    'latest_grades': """
        SELECT DISTINCT ON (n.dni_alumno, n.codigo_asignatura)
            n.dni_alumno,
            da.nombre AS nombre_alumno,
            n.codigo_asignatura,
            a.nombre AS nombre_asignatura,
            n.tipo_evaluacion,
            n.valor,
            e.fecha_inicio AS fecha_evaluacion
        FROM {schema}.nota n
        JOIN {schema}.datosalumno da ON da.dni_alumno = n.dni_alumno
        JOIN {schema}.evaluacion e ON e.tipo = n.tipo_evaluacion AND e.codigo_asignatura = n.codigo_asignatura
        JOIN {schema}.asignatura a ON a.codigo = n.codigo_asignatura
        ORDER BY n.dni_alumno, n.codigo_asignatura, e.fecha_inicio DESC;
    """,
    
    'classroom_performance': """
        SELECT 
            a.nombre AS asignatura,
            m.grado,
            m.seccion,
            au.tipo AS tipo_aula,
            t.nombre AS profesor,
            ROUND(AVG(n.valor), 2) AS promedio_notas,
            COUNT(DISTINCT da.dni_alumno) AS total_alumnos
        FROM {schema}.Matricula m
        INNER JOIN {schema}.DatosAlumno da ON m.dni_alumno = da.dni_alumno
        INNER JOIN {schema}.Nota n ON da.dni_alumno = n.dni_alumno
        INNER JOIN {schema}.Asignatura a ON n.codigo_asignatura = a.codigo
        INNER JOIN {schema}.AsignaturaDictada ad 
            ON a.codigo = ad.codigo_asignatura 
            AND da.dni_alumno = ad.dni_alumno
        INNER JOIN {schema}.Profesor p ON ad.id_profesor = p.id
        INNER JOIN {schema}.Trabajadores t ON p.id = t.id
        INNER JOIN {schema}.Aula au ON m.id_aula = au.id
        WHERE 
            m.estado_matricula = 'activo'
            AND a.fecha_inicio BETWEEN '2025-03-01' AND '2025-12-15'
            AND n.valor >= 11  
        GROUP BY 
            a.nombre, 
            m.grado, 
            m.seccion, 
            au.tipo, 
            t.nombre
        HAVING AVG(n.valor) > 13 
        ORDER BY 
            m.grado DESC, 
            promedio_notas DESC;
    """,
    
    'student_complete_info': """
        SELECT
            da.dni_alumno,
            da.nombre AS nombre_alumno,
            apo.nombre AS nombre_apoderado,
            m.grado,
            m.seccion,
            a.nombre AS asignatura,
            AVG(n.valor) AS promedio_asignatura,
            t.nombre AS profesor,
            (
                SELECT COUNT(*)
                FROM {schema}.Limpia l
                WHERE l.id_aula = m.id_aula
                  AND l.fecha BETWEEN (SELECT MIN(fecha_matricula) FROM {schema}.Matricula WHERE dni_alumno = da.dni_alumno) AND CURRENT_DATE
            ) AS limpiezas_aula,
            (
                SELECT SUM(c.monto)
                FROM {schema}.Cobra c
                WHERE c.dni_apoderado = apo.dni_apoderado
                  AND c.dni_alumno = da.dni_alumno
                  AND c.fecha_pago BETWEEN '2025-01-01' AND '2025-12-31'
            ) AS total_pagado_2025,
            CASE
                WHEN AVG(n.valor) >= 14 THEN 'Excelente'
                WHEN AVG(n.valor) >= 11 THEN 'Aprobado'
                ELSE 'Desaprobado'
                END AS estado_academico
        FROM {schema}.Matricula m
                 JOIN {schema}.DatosAlumno da ON m.dni_alumno = da.dni_alumno
                 JOIN {schema}.AlumnoApoderado aa ON da.dni_alumno = aa.dni_alumno
                 JOIN {schema}.Apoderado apo ON aa.dni_apoderado = apo.dni_apoderado
                 JOIN {schema}.Nota n ON da.dni_alumno = n.dni_alumno
                 JOIN {schema}.Asignatura a ON n.codigo_asignatura = a.codigo
                 JOIN {schema}.AsignaturaDictada ad ON a.codigo = ad.codigo_asignatura AND da.dni_alumno = ad.dni_alumno
                 JOIN {schema}.Profesor p ON ad.id_profesor = p.id
                 JOIN {schema}.Trabajadores t ON p.id = t.id
        WHERE m.estado_matricula = 'activo'
          AND a.fecha_inicio >= '2025-03-01'
          AND EXISTS (
            SELECT 1
            FROM {schema}.Evaluacion e
            WHERE e.codigo_asignatura = a.codigo
              AND e.fecha_inicio <= CURRENT_DATE
        )
        GROUP BY
            da.dni_alumno,
            da.nombre,
            apo.nombre,
            m.grado,
            m.seccion,
            a.nombre,
            t.nombre,
            m.id_aula,
            apo.dni_apoderado
        HAVING AVG(n.valor) >= 10
        ORDER BY
            m.grado,
            m.seccion,
            promedio_asignatura DESC;
    """
}

def get_db_connection(database_name):
    """Obtener conexión a la base de datos especificada"""
    if database_name not in DB_CONFIGS:
        raise ValueError(f"Base de datos '{database_name}' no encontrada")
    
    config = DB_CONFIGS[database_name]
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error conectando a {database_name}: {e}")
        raise

def execute_query(database_name, query, params=None, page=1, per_page=50):
    """Ejecutar consulta con paginación"""
    conn = None
    try:
        conn = get_db_connection(database_name)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Calcular offset
        offset = (page - 1) * per_page
        
        # Agregar paginación a la consulta
        paginated_query = f"{query} LIMIT {per_page} OFFSET {offset}"
        
        # Ejecutar consulta
        cursor.execute(paginated_query, params)
        results = cursor.fetchall()
        
        # Obtener total de registros (sin paginación)
        count_query = f"SELECT COUNT(*) FROM ({query}) AS count_table"
        cursor.execute(count_query, params)
        total_records = cursor.fetchone()['count']
        
        return {
            'data': [dict(row) for row in results],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_records': total_records,
                'total_pages': (total_records + per_page - 1) // per_page
            }
        }
    
    except psycopg2.Error as e:
        logger.error(f"Error ejecutando consulta: {e}")
        raise
    finally:
        if conn:
            conn.close()

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint de verificación de salud"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'databases': list(DB_CONFIGS.keys())
    })

@app.route('/databases', methods=['GET'])
def list_databases():
    """Listar bases de datos disponibles"""
    return jsonify({
        'databases': [
            {'name': 'railway', 'description': 'Railway - 1K datos'},
            {'name': 'colegio2', 'description': 'Colegio2 - 10K datos (schema: cole2)'},
            {'name': 'colegio3', 'description': 'Colegio3 - 100K datos (schema: cole3)'},
            {'name': 'colegio4', 'description': 'Colegio4 - 1M datos (schema: cole4)'}
        ]
    }), 200

@app.route('/query', methods=['POST'])
def execute_custom_query():
    """Ejecutar consulta personalizada"""
    try:
        data = request.get_json()
        
        # Validar parámetros requeridos
        if not data or 'database' not in data or 'query' not in data:
            return jsonify({'error': 'Faltan parámetros requeridos: database, query'}), 400
        
        database_name = data['database']
        query = data['query']
        page = data.get('page', 1)
        per_page = min(data.get('per_page', 50), 100)  # Máximo 100 registros por página
        
        # Validar que sea una consulta SELECT y evitar SQL injection
        query_upper = query.strip().upper()
        if not query_upper.startswith('SELECT'):
            return jsonify({'error': 'Solo se permiten consultas SELECT'}), 400
            
        # Bloquear comandos peligrosos
        dangerous_keywords = ['DELETE', 'DROP', 'TRUNCATE', 'INSERT', 'UPDATE', 'CREATE', 'ALTER', '--']
        if any(keyword in query_upper for keyword in dangerous_keywords):
            return jsonify({'error': 'Comando SQL no permitido'}), 400
        
        # Ejecutar consulta
        result = execute_query(database_name, query, page=page, per_page=per_page)
        
        return jsonify({
            'success': True,
            'database': database_name,
            'result': result,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error en consulta personalizada: {e}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.route('/predefined-queries', methods=['GET'])
def list_predefined_queries():
    """Listar consultas preestablecidas"""
    return jsonify({
        'queries': [
            {'key': 'top_students', 'description': 'Top 10 estudiantes con mejor promedio'},
            {'key': 'latest_grades', 'description': 'Últimas notas por alumno y asignatura'},
            {'key': 'classroom_performance', 'description': 'Rendimiento por aula y profesor'},
            {'key': 'student_complete_info', 'description': 'Información completa de estudiantes'}
        ]
    })

@app.route('/predefined-query/<query_key>', methods=['POST'])
def execute_predefined_query(query_key):
    """Ejecutar consulta preestablecida"""
    try:
        data = request.get_json()
        
        # Validar parámetros requeridos
        if not data or 'database' not in data:
            return jsonify({'error': 'Falta parámetro requerido: database'}), 400
        
        database_name = data['database']
        page = data.get('page', 1)
        per_page = min(data.get('per_page', 50), 100)
        
        # Validar consulta preestablecida
        if query_key not in PREDEFINED_QUERIES:
            return jsonify({'error': f'Consulta preestablecida "{query_key}" no encontrada'}), 404
        
        # Obtener schema según la base de datos
        schema = DB_CONFIGS[database_name]['schema']
        
        # Formatear consulta con el schema apropiado
        query = PREDEFINED_QUERIES[query_key].format(schema=schema)
        
        # Ejecutar consulta
        result = execute_query(database_name, query, page=page, per_page=per_page)
        
        return jsonify({
            'success': True,
            'database': database_name,
            'query_key': query_key,
            'result': result,
            'timestamp': datetime.now().isoformat()
        })
        
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error en consulta preestablecida: {e}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.route('/tables/<database_name>', methods=['GET'])
def list_tables(database_name):
    """Listar tablas de una base de datos"""
    try:
        if database_name not in DB_CONFIGS:
            return jsonify({'error': f'Base de datos "{database_name}" no encontrada'}), 404
        
        schema = DB_CONFIGS[database_name]['schema']
        
        query = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;
        """
        
        conn = get_db_connection(database_name)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, (schema,))
        tables = cursor.fetchall()
        conn.close()
        
        return jsonify({
            'database': database_name,
            'schema': schema,
            'tables': [dict(row) for row in tables]
        })
        
    except Exception as e:
        logger.error(f"Error listando tablas: {e}")
        return jsonify({'error': 'Error interno del servidor'}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint no encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Error interno del servidor'}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    app.run(debug=debug, host='0.0.0.0', port=port)