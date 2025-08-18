<div align="center">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=700&size=26&pause=1000&color=00D4FF&center=true&vCenter=true&width=740&lines=Aaftab+Mohammad;Backend+%2B+ML+Engineer;Designing+reliable+APIs%2C+ML+serving%2C+and+data+pipelines" alt="intro" />
</div>

---

### What I build
- Backend services with clean contracts, auth, rate‑limits, and observability
- ML systems from training scripts → reproducible pipelines → serving endpoints
- Data pipelines (Spark/Parquet) for features and analytics, batch + streaming
- Practical, measurable improvements: profiling, testing, tracing, CI

### Engineering principles
- Keep interfaces small and explicit; document assumptions
- Measure first; optimize the hot path only
- Fail fast, surface errors with context, and protect with guardrails
- Write code that future‑me can read without a meeting

### Toolbelt

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

---

### Mini blueprints

<details>
<summary>API service skeleton (request validation + metrics)</summary>

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import time

app = FastAPI(title="Service")

class PredictIn(BaseModel):
    amount: float = Field(gt=0)
    category: str

@app.middleware("http")
async def timing(request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Response-Time-ms"] = str(round((time.perf_counter()-start)*1000, 2))
    return response

@app.post("/predict")
async def predict(payload: PredictIn):
    try:
        # inject model/feature logic here
        score = 0.42  # placeholder
        return {"score": score, "category": payload.category}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
```
</details>

<details>
<summary>Model serving (load once, validate inputs)</summary>

```python
import pickle
from pydantic import BaseModel, Field, field_validator
from fastapi import FastAPI
import numpy as np

with open("model.pkl", "rb") as f:
    MODEL = pickle.load(f)
FEATURES = ["amount", "age", "hour"]

class Row(BaseModel):
    amount: float = Field(gt=0)
    age: int = Field(ge=0, le=120)
    hour: int = Field(ge=0, le=23)

    @field_validator("amount")
    @classmethod
    def cap_amount(cls, v):
        return float(min(v, 1e7))

app = FastAPI()

@app.post("/predict")
async def predict(x: Row):
    arr = np.array([[x.amount, x.age, x.hour]])
    prob = float(MODEL.predict_proba(arr)[0,1])
    return {"prob": prob, "decision": int(prob > 0.5)}
```
</details>

<details>
<summary>Spark batch job (schema‑first ETL → Parquet)</summary>

```python
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.appName("etl").getOrCreate())

schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("ts", T.TimestampType()),
])

(df := spark.read.schema(schema).json("gs://bucket/input/*.json")
      .withColumn("hour", F.hour("ts"))
      .filter(F.col("amount") > 0)
      .repartition(64)
 ).write.mode("overwrite").parquet("gs://bucket/clean/events.parquet")
```
</details>

---

### Currently
- Strengthening backend patterns (authn/z, idempotency, caching)
- Cleaning up ML training → serving handoffs
- Experimenting with small latency budgets and tracing

---

<div align="center">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=600&size=18&pause=1200&color=00D4FF&center=true&vCenter=true&width=720&lines=Open+to+SDE%2C+ML%2FAI%2C+and+Platform+roles" alt="outro" />
</div>
