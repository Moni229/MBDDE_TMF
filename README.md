# Lakehouse para análisis macroeconómico EU y USA

Este proyecto implementa un pipeline de ingeniería de datos para la ingesta, procesamiento y transformación de datos financieros y macroeconómicos mediante cargas batch y procesamiento en streaming.

---

## Descripción general

El proyecto implementa un pipeline de datos end-to-end basado en la Arquitectura Medallion.

Los datos se obtienen de múltiples fuentes, se procesan a través de diferentes capas y se transforman en conjuntos de datos preparados para su consumo analítico.

### Arquitectura

```text
                    ┌─────────────────┐
                    │ Fuentes de datos│
                    │                 │
                    │ • Finnhub       │
                    │ • Yahoo Finance │
                    │ • Eurostat      │
                    │ • FRED          │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Landing     │
                    │   Datos crudos  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Bronze      │
                    │ Tablas Delta    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Silver      │
                    │ Datos limpios   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Gold       │
                    │ Datos analíticos│
                    └─────────────────┘
```

---

## Fuentes de datos

| Fuente        | Datos                                         | Tipo de procesamiento |
| ------------- | --------------------------------------------- | --------------------- |
| Finnhub       | Operaciones de mercado en tiempo real         | Streaming             |
| Yahoo Finance | Datos históricos de mercado                   | Batch                 |
| Eurostat      | Indicadores macroeconómicos europeos          | Batch                 |
| FRED          | Indicadores macroeconómicos de Estados Unidos | Batch                 |

---

## Estructura del proyecto

```text
project/
│
├── src/
│   └── macroeconomy/
│       ├── config/             # Configuración de pipelines y fuentes de datos
│       ├── etls/               # ETLs de cada capa que lo requiera (silver y gold)
│       ├── pipelines/          # Pipelines de ingesta y procesamiento
│       ├── readers/            # Readers de ficheros y de delta tables
│       ├── sources/            # Llamadas a APIs externas y fuentes de datos
        ├── utils/              # Utilidades compartidas
        ├── writers/            # Writers de ficheros y de delta tables
│       └── setup.py            # Setup inicial del proyecto
│
├── notebooks/                  # Notebooks de Databricks
│
├── tests/                      # Tests
│
├── pyproject.toml                # Configuración del proyecto
│
└── README.md
```
---

## Fuentes de datos

El proyecto integra datos procedentes de cuatro fuentes diferentes. Estas fuentes proporcionan tanto información financiera de mercado como indicadores macroeconómicos y utilizan diferentes mecanismos de acceso y procesamiento.

| Fuente        | Tipo de datos                                 | Procesamiento |
| ------------- | --------------------------------------------- | ------------- |
| Finnhub       | Datos financieros en tiempo real              | Streaming     |
| Yahoo Finance | Datos históricos de mercado                   | Batch         |
| Eurostat      | Indicadores macroeconómicos europeos          | Batch         |
| FRED          | Indicadores macroeconómicos de Estados Unidos | Batch         |

### Finnhub

[Finnhub](https://finnhub.io/) proporciona datos financieros en tiempo real mediante su API y servicios de streaming.

En este proyecto se utiliza principalmente como fuente de datos de operaciones de mercado (*trades*) en tiempo real. Los datos se reciben de forma continua y se procesan mediante un pipeline de streaming.

**Características principales:**

* Datos financieros en tiempo real.
* Procesamiento mediante streaming.
* Recepción continua de eventos de mercado.
* Integración con la infraestructura de mensajería y procesamiento del proyecto.

### Yahoo Finance

[Yahoo Finance](https://finance.yahoo.com/) proporciona información histórica sobre instrumentos financieros y mercados.

En el proyecto se utiliza para obtener datos históricos de diferentes activos e índices financieros. Estos datos se procesan mediante cargas batch y permiten realizar análisis a nivel diario.

Por defecto, obtiene el último dato disponible de la API, pero se pueden especificar periodos de tiempo para obtener datos históricos.

**Características principales:**

* Datos históricos de mercado.
* Información sobre acciones e índices.
* Procesamiento batch.
* Soporte para la carga y reprocesamiento de periodos históricos.

### Eurostat

[Eurostat](https://ec.europa.eu/eurostat/) es la oficina estadística de la Unión Europea y proporciona datos estadísticos y macroeconómicos de los países europeos.

En este proyecto se utiliza para obtener indicadores macroeconómicos relacionados con la economía de la zona euro, como indicadores de inflación, producción industrial o sentimiento empresarial.

Los datos se obtienen a través de la API de Eurostat y se procesan mediante pipelines batch.

Requiere que se indiquen una o varias fechas para hacer ingestas incrementales. Si no se indican, se realizará una ingesta histórica con todos los datos disponibles.

**Características principales:**

* Indicadores macroeconómicos europeos.
* Datos oficiales de la Unión Europea.
* Acceso mediante API.
* Procesamiento batch.
* Datos con diferentes frecuencias temporales.

### FRED

[FRED](https://fred.stlouisfed.org/) (*Federal Reserve Economic Data*) es una plataforma mantenida por el Federal Reserve Bank of St. Louis que proporciona acceso a una amplia colección de series económicas y financieras.

En este proyecto se utiliza para obtener indicadores macroeconómicos y financieros de Estados Unidos, incluyendo indicadores como la inflación, los tipos de interés, la rentabilidad de bonos del Tesoro o índices de volatilidad.

Los datos se obtienen mediante la API de FRED y se procesan mediante cargas batch.

 Por defecto, obtiene el último dato disponible de la API, pero se pueden especificar periodos de tiempo para obtener datos históricos.

**Características principales:**

* Indicadores macroeconómicos y financieros de Estados Unidos.
* Amplia disponibilidad de series temporales históricas.
* Acceso mediante API.
* Procesamiento batch.
* Indicadores con frecuencias diarias y mensuales.


## Arquitectura de datos

El pipeline sigue una Arquitectura Medallion compuesta por cuatro capas.

### Landing

La capa Landing almacena los datos en bruto obtenidos desde las fuentes externas.

En esta capa no se aplican transformaciones de negocio sobre los datos.

### Bronze

La capa Bronze almacena los datos ingeridos en formato Delta y constituye la primera capa estructurada de persistencia.

### Silver

La capa Silver contiene los datos limpios, estandarizados y estructurados.

Las transformaciones pueden incluir:

* Normalización de tipos de datos.
* Estandarización de nombres de columnas.
* Eliminación de duplicados.
* Limpieza de datos.
* Normalización de esquemas.

### Gold

La capa Gold contiene los conjuntos de datos preparados para el consumo analítico.

Los datasets actuales incluyen:

| Dataset                | Descripción                 |
| ---------------------- | --------------------------- |
| `fact_market_intraday` | Datos intradía de mercado   |
| `fact_market_daily`    | Datos diarios de mercado    |
| `fact_market_macro`    | Indicadores macroeconómicos |

---

## Tipos de procesamiento

### Procesamiento Batch

El procesamiento batch se utiliza para datos históricos y conjuntos de datos que se actualizan periódicamente.

Las fuentes procesadas actualmente mediante batch incluyen:

* Yahoo Finance
* Eurostat
* FRED

Los pipelines batch permiten la ingesta histórica y el reprocesamiento selectivo de los datos.

### Procesamiento Streaming

El procesamiento streaming se utiliza para la ingesta de datos en tiempo real.

La fuente procesada actualmente mediante streaming es:

* Finnhub

Los datos en streaming se procesan a través del pipeline y se almacenan en tablas Delta.

---

## Requisitos previos

Antes de ejecutar el proyecto, es necesario disponer de:

* Python >= 3.8
* Entorno de Databricks 
* Acceso a las APIs necesarias
* Credenciales y secrets requeridos

---
## Configuración y gestión de Secrets

Para ejecutar correctamente el proyecto es necesario configurar las credenciales y claves de acceso utilizadas por las diferentes fuentes de datos y servicios externos.

Las credenciales no se incluyen en el repositorio. El proyecto obtiene esta información mediante **Secrets**, que deben estar configurados previamente en el entorno de ejecución.

### Secrets requeridos

| Secret            | Contenido                                         | Utilizado por         |
|-------------------|---------------------------------------------------|-----------------------|
| `FRED-API-KEY-2`  | `API key para poder conectar a la API de FRED`    | `FRED`                |
| `FINNHUB-API-KEY` | `API key para poder conectar a la API de Finnhub` | `FINNHUB`             |
| `[SECRET_NAME]`   | `[Descripción del contenido]`                     | `[Fuente o servicio]` |

---
## Instalación

Clonar el repositorio:

```bash
git clone <url-del-repositorio>
cd <nombre-del-repositorio>
```

Instalar las dependencias necesarias:

```bash
pip install -r requirements.txt
```

Configurar las variables de entorno y credenciales necesarias antes de ejecutar los pipelines.

---

## Configuración

El comportamiento de los pipelines y las fuentes de datos se configura mediante archivos de configuración.

Ejemplo de estructura:

```text
configs/
├── pipeline_config.yaml
├── sources.yaml
└── ...
```

La configuración puede incluir:

* Configuración de las fuentes de datos.
* Definición de pipelines.
* Tipo de procesamiento.
* Tablas de entrada y salida.
* Filtros de particiones.
* Opciones de streaming.

Las credenciales y otra información sensible no deben incluirse en el repositorio.

---

## Ejecución del pipeline

Las diferentes etapas del pipeline pueden ejecutarse de forma independiente, como puede verse en `notebooks/`.

---

## Tablas y datasets

### Datos de mercado

| Fuente        | Dataset                             |
| ------------- | ----------------------------------- |
| Yahoo Finance | Datasets históricos de mercado      |
| Finnhub       | Datos de operaciones en tiempo real |

### Datos macroeconómicos

| Fuente   | Indicadores                                                       |
| -------- | ----------------------------------------------------------------- |
| FRED     | CPI, Federal Funds Rate, rentabilidad del Treasury a 10 años, VIX |
| Eurostat | HICP, producción industrial, sentimiento empresarial              |
