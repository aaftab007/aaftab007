<div align="center">

<img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=700&size=26&pause=1000&color=00D4FF&center=true&vCenter=true&width=760&lines>$+whoami;Aaftab+Mohammad;SDE+%7C+ML%2FAI+Engineer+%7C+Systems" alt="intro" />

</div>

---

```text
I design backend APIs, production ML serving, and data pipelines.
Focus: correctness first, low latency, strong validation, easy ops.
```

### Focus areas
- Backend services with clean contracts, idempotency, and guardrails
- ML training → reproducible pipelines → serving endpoints
- Batch/stream ETL (Spark/Parquet), feature engineering, data hygiene
- Reliability: tracing, rate limits, SLOs, failure budgets

### Tech I reach for

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?logo=openjdk&logoColor=white)
![C++](https://img.shields.io/badge/C%2B%2B-00599C?logo=c%2B%2B&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?logo=pytorch&logoColor=white)
![scikit‑learn](https://img.shields.io/badge/scikit--learn-F7931E?logo=scikitlearn&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-337AB7)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google_Cloud-4285F4?logo=googlecloud&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![STM32](https://img.shields.io/badge/STM32-03234B)

### Selected repos
- 🏦 Bank fraud detection (Big Data): `Big_Data_Final-Project`
- 🛡️ ML in cybersecurity labs: `machine-learning-cybersecurity-labs`
- 🎨 Classical artwork generation (GAN): `classical-artwork-gan`
- 🤖 Smart pedometer (STM32, Mbed): `smart-pedometer`
- 🧭 Java Swing maze game: `tri-wizard-quest`

---

### Blueprints

<details>
<summary>FastAPI service: strict validation + timing middleware</summary>

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import time

app = FastAPI(title="svc")

class In(BaseModel):
    amount: float = Field(gt=0)
    kind: str

@app.middleware("http")
async def rt(request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    resp.headers["x-rt-ms"] = f"{(time.perf_counter()-t0)*1e3:.2f}"
    return resp

@app.post("/predict")
async def predict(payload: In):
    try:
        score = 0.42  # plug model logic here
        return {"score": score, "kind": payload.kind}
    except Exception as e:
        raise HTTPException(400, str(e))
```
</details>

<details>
<summary>Spark ETL: schema-first JSON → Parquet (feature-ready)</summary>

```python
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("etl").getOrCreate()

schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("ts", T.TimestampType()),
])

(spark.read.schema(schema).json("gs://bucket/input/*.json")
      .filter(F.col("amount") > 0)
      .withColumn("hour", F.hour("ts"))
      .repartition(64)
).write.mode("overwrite").parquet("gs://bucket/clean/events.parquet")
```
</details>

---

### How I work
- Start simple → ship → iterate
- Measure before optimizing; profile the hot path
- Tests + CI for confidence; clear docs for future changes

<div align="center">

<img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=600&size=18&pause=1200&color=00D4FF&center=true&vCenter=true&width=720&lines=Open+to+SDE%2C+ML%2FAI%2C+and+Platform+roles" alt="outro" />

</div>
