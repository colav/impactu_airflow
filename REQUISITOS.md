# Requisitos del Sistema - ImpactU Airflow ETL

Este documento detalla los requisitos técnicos y funcionales para el sistema de extracción, transformación y carga (ETL) de ImpactU.

## 1. Requisitos Fundamentales (Definidos)

### 1.1 Centralización en MongoDB
* Todas las fuentes de datos, independientemente de su origen o formato (API, SQL, NoSQL, archivos planos), deben ser normalizadas o almacenadas en su forma cruda dentro de una instancia centralizada de **MongoDB**.

### 1.2 Estructura de Paquete para Grandes Volúmenes
* El sistema debe estar diseñado como un paquete modular que permita la gestión eficiente de grandes volúmenes de datos, evitando cuellos de botella en memoria y optimizando el I/O.

### 1.3 Soporte de Checkpoints (Puntos de Control)
* Es crítico que los procesos de extracción soporten checkpoints. Si una tarea falla o se detiene, debe ser capaz de reanudarse desde el último punto guardado con éxito, evitando procesar datos ya descargados.

### 1.4 Ejecución en Paralelo
* El sistema debe aprovechar las capacidades de Airflow para ejecutar múltiples tareas de extracción y carga de forma simultánea, optimizando el tiempo total de ejecución.

### 1.5 Tolerancia a Fallos (Fault Tolerance)
* El sistema debe ser resiliente. Las fallas en una fuente de datos no deben detener el pipeline completo, y deben existir mecanismos de reintento (retries) automáticos.

### 1.6 Integridad y No Duplicación
* Se deben implementar mecanismos para evitar la corrupción de datos y, sobre todo, la duplicación de información en MongoDB (Idempotencia).

---

## 2. Recomendaciones Adicionales (Ingeniería de Datos Avanzada)

Para asegurar que el sistema sea de clase empresarial y mantenible a largo plazo, vamos a desarrollar los siguientes puntos:

### 2.1 Idempotencia Estricta
* Cada DAG debe ser diseñado de tal manera que si se ejecuta múltiples veces para el mismo periodo o conjunto de datos, el estado final de la base de datos sea idéntico, sin crear registros duplicados.

### 2.2 Validación de Calidad de Datos (Data Quality)
* Implementar una capa de validación post-extracción y pre-carga. Verificar esquemas mínimos, campos obligatorios y tipos de datos para evitar que "datos basura" contaminen el lago de datos.

### 2.3 Gestión de Secretos y Configuración
* No hardcodear credenciales. Utilizar **Airflow Connections** y **Variables**, o integrarse con un gestor de secretos (HashiCorp Vault, AWS Secrets Manager) para manejar llaves de API y contraseñas de DB.

### 2.4 Monitoreo y Alertas Proactivas
* Configurar notificaciones (Slack, Email o Discord) para fallos críticos. Además, implementar logs estructurados que permitan rastrear el linaje del dato (Data Lineage).

### 2.5 Estrategia de Backfilling
* El sistema debe permitir la carga histórica de datos de forma sencilla mediante el uso de `start_date` y `catchup` en Airflow, asegurando que el particionamiento de los datos sea coherente.

### 2.6 Capa de Abstracción de Fuentes (Source Abstraction)
* Crear una clase base o interfaz para los extractores. Esto facilitará la adición de nuevas fuentes (ej. una nueva API) siguiendo el mismo patrón de checkpoints y logs sin reinventar la rueda.

### 2.7 Contenerización y Orquestación
* Asegurar que todo el entorno sea reproducible mediante **Docker**, facilitando el despliegue en diferentes entornos (Dev, Test, Prod) de manera idéntica.
