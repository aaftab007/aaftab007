<div align="center">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=700&size=24&pause=900&color=00D4FF&center=true&vCenter=true&width=780&lines=Hi,+I+am+Aaftab+Mohammad;Software+Engineer+%E2%80%A2+ML%2FAI+%E2%80%A2+Systems;Ship+reliable+APIs%2C+serve+models%2C+and+move+data+fast" alt="header" />
</div>

---

### About
I build backend services, production ML serving, and data pipelines end‑to‑end. I like precise interfaces, measurable performance, and code that future‑me can read.

### Focus
- Backend: clean contracts, idempotency, authn/z, guardrails
- ML: training → reproducible pipelines → serving with validation
- Data: Spark/Parquet features, batch + streaming ETL
- Reliability: tracing, SLOs, rate limits, failure budgets

### Stack
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?logo=openjdk&logoColor=white)
![C++](https://img.shields.io/badge/C%2B%2B-00599C?logo=c%2B%2B&logoColor=white)
![Go](https://img.shields.io/badge/Go-00ADD8?logo=go&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?logo=pytorch&logoColor=white)
![scikit‑learn](https://img.shields.io/badge/scikit--learn-F7931E?logo=scikitlearn&logoColor=white)
![XGBoost](https://img.shields.io/badge/XGBoost-337AB7)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?logo=apachekafka&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white)
![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?logo=googlecloud&logoColor=white)
![AWS_S3](https://img.shields.io/badge/AWS_S3-569A31?logo=amazonaws&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?logo=mongodb&logoColor=white)
![GitHub_Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?logo=githubactions&logoColor=white)

### Experience (highlights)
- Software Engineer Intern — Mazgen Technologies (Jan–May 2023)
  - Kafka‑backed price‑inventory pipeline; reduced p95 latency from ~800ms → ~300ms for 5k concurrent users
  - Redis caching + MySQL indexing improvements; OAuth2 hardening and incident reduction
- Software Engineer Intern — Cloudwapp Technologies (Feb–May 2021)
  - Legacy task API middleware; 70% crash reduction, 85% failed requests auto‑recovered
  - Slack alerts + runbooks; 99.9% uptime targets; query/path optimizations
- Web Developer Intern — Global Galaxy Intl. Consultancy (Aug–Nov 2020)
  - jQuery → React migration; page‑load improved; Lighthouse/CI integration with Jest + Lighthouse

### Education
- New York University — MS, Computer Science (2023–2025)
- Nirma University — BTech, Computer Science and Engineering (2019–2023)

### Selected work
- Bank fraud detection (Big Data) — FastAPI + XGBoost, GCP, Spark — repo: `Big_Data_Final-Project`
- ML in cybersecurity labs — malware detection, AI security — repo: `machine-learning-cybersecurity-labs`
- Classical artwork generation — GANs in PyTorch — repo: `classical-artwork-gan`
- Smart pedometer — STM32F429ZI + Mbed OS — repo: `smart-pedometer`
- Tri‑Wizard Quest — Java Swing maze game — repo: `tri-wizard-quest`
- AI‑Powered Resume Analysis — Flask/React + BERT, PostgreSQL — resume project
- Project Management Tool — React/Node/Socket.IO/Twilio — resume project

---

<details>
<summary>Blueprint: tiny FastAPI service with timing + input caps</summary>

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
    t0 = time.perf_counter(); resp = await call_next(request)
    resp.headers["x-rt-ms"] = f"{(time.perf_counter()-t0)*1e3:.2f}"; return resp

@app.post("/predict")
async def predict(x: In):
    try:
        score = 0.42  # plug model
        return {"score": score, "kind": x.kind}
    except Exception as e:
        raise HTTPException(400, str(e))
```
</details>

<details>
<summary>Blueprint: Spark ETL → feature Parquet</summary>

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
).write.mode("overwrite").parquet("gs://bucket/clean/events.parquet")
```
</details>

---

<div align="center">
  <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&weight=600&size=18&pause=1200&color=00D4FF&center=true&vCenter=true&width=720&lines=Open+to+SDE%2C+ML%2FAI%2C+and+Platform+roles" alt="footer" />
</div>
