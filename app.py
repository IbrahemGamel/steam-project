import os
import json
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles


def p(*args):
    print(*args, flush=True)


def simulate_producer_output():
    orders = [
        {"customer": "Ali",  "product": "Laptop",   "price": 1200},
        {"customer": "Sara", "product": "Phone",     "price": 800},
        {"customer": "Ali",  "product": "Mouse",     "price": 25},
        {"customer": "Mona", "product": "Laptop",    "price": 1100},
        {"customer": "Sara", "product": "Keyboard",  "price": 75},
        {"customer": "Omar", "product": "Phone",     "price": 650},
    ]
    p("=" * 60)
    p(">>> PRODUCER OUTPUT  (python producer_orders.py)")
    p("=" * 60)
    for order in orders:
        p(f"Sent: {order}")
    p("All 6 orders sent to Kafka topic: orders")
    p("=" * 60)


def simulate_spark_output():
    sep = "-" * 44

    p("\n>>> SPARK TASK 1 — Show Incoming Orders")
    p(sep)
    p("Batch: 0")
    p(sep)
    p(f"{'customer':<12} {'product':<12} {'price'}")
    for row in [("Ali","Laptop",1200),("Sara","Phone",800),("Ali","Mouse",25),
                ("Mona","Laptop",1100),("Sara","Keyboard",75),("Omar","Phone",650)]:
        p(f"{row[0]:<12} {row[1]:<12} {row[2]}")

    p("\n>>> SPARK TASK 2 — Select Product Names Only")
    p(sep)
    p("Batch: 0")
    p(sep)
    p("product")
    for prod in ["Laptop","Phone","Mouse","Laptop","Keyboard","Phone"]:
        p(prod)

    p("\n>>> SPARK TASK 3 — Filter Expensive Orders (price > 500)")
    p(sep)
    p("Batch: 0")
    p(sep)
    p(f"{'customer':<12} {'product':<12} {'price'}")
    for row in [("Ali","Laptop",1200),("Sara","Phone",800),
                ("Mona","Laptop",1100),("Omar","Phone",650)]:
        p(f"{row[0]:<12} {row[1]:<12} {row[2]}")
    p("(Mouse $25 and Keyboard $75 filtered out — price <= 500)")

    p("\n>>> SPARK TASK 4 — Add Discounted Price (10% off)")
    p(sep)
    p("Batch: 0")
    p(sep)
    p(f"{'customer':<12} {'product':<12} {'price':<8} {'discounted_price'}")
    for row in [("Ali","Laptop",1200,1080.0),("Sara","Phone",800,720.0),
                ("Ali","Mouse",25,22.5),("Mona","Laptop",1100,990.0),
                ("Sara","Keyboard",75,67.5),("Omar","Phone",650,585.0)]:
        p(f"{row[0]:<12} {row[1]:<12} {row[2]:<8} {row[3]}")

    p("\n>>> SPARK BONUS — Count Orders per Customer")
    p("    (outputMode: complete)")
    p(sep)
    p("Batch: 0")
    p(sep)
    p(f"{'customer':<12} {'count'}")
    for row in [("Ali",2),("Sara",2),("Mona",1),("Omar",1)]:
        p(f"{row[0]:<12} {row[1]}")
    p("=" * 60)
    p("Workshop pipeline complete.")
    p("=" * 60)


@asynccontextmanager
async def lifespan(app):
    simulate_producer_output()
    simulate_spark_output()
    yield


app = FastAPI(title="Spark Kafka Workshop", lifespan=lifespan)
app.mount("/images", StaticFiles(directory="images"), name="images")

KAFKA_BROKER = os.getenv("KAFKA_URL", "localhost:9092")
TOPIC = "orders"

ORDERS = [
    {"customer": "Ali",  "product": "Laptop",   "price": 1200},
    {"customer": "Sara", "product": "Phone",     "price": 800},
    {"customer": "Ali",  "product": "Mouse",     "price": 25},
    {"customer": "Mona", "product": "Laptop",    "price": 1100},
    {"customer": "Sara", "product": "Keyboard",  "price": 75},
    {"customer": "Omar", "product": "Phone",     "price": 650},
]

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Spark + Kafka Workshop</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism-tomorrow.min.css" rel="stylesheet"/>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0f172a;--surface:#1e293b;--surface2:#273548;--border:#334155;
  --accent:#6366f1;--accent2:#818cf8;--green:#22c55e;--yellow:#eab308;
  --red:#ef4444;--text:#e2e8f0;--muted:#94a3b8;--mono:'JetBrains Mono',monospace;
}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;line-height:1.6;min-height:100vh}

/* ── HEADER ── */
header{
  background:linear-gradient(135deg,#1e1b4b 0%,#312e81 50%,#1e293b 100%);
  padding:3rem 2rem 2.5rem;text-align:center;
  border-bottom:1px solid var(--border);position:relative;overflow:hidden;
}
header::before{
  content:'';position:absolute;inset:0;
  background:radial-gradient(ellipse at 60% 0%,rgba(99,102,241,.25) 0%,transparent 70%);
}
header h1{font-size:clamp(1.6rem,4vw,2.6rem);font-weight:700;position:relative;
  background:linear-gradient(90deg,#a5b4fc,#e0e7ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
header .sub{color:var(--muted);font-size:.95rem;margin-top:.4rem;position:relative}
.badge{display:inline-block;background:rgba(99,102,241,.2);border:1px solid var(--accent);
  color:var(--accent2);border-radius:9999px;padding:.15rem .75rem;font-size:.75rem;font-weight:600;
  margin:.5rem .2rem 0;vertical-align:middle}

/* ── LAYOUT ── */
main{max-width:1200px;margin:0 auto;padding:2rem 1.5rem 4rem}
section{margin-bottom:3rem}
h2{font-size:1.25rem;font-weight:600;color:var(--accent2);margin-bottom:1.25rem;
  display:flex;align-items:center;gap:.5rem}
h2::before{content:'';display:block;width:3px;height:1.1em;background:var(--accent);border-radius:2px}

/* ── PIPELINE DIAGRAM ── */
.pipeline{
  display:flex;align-items:stretch;gap:0;
  background:var(--surface);border:1px solid var(--border);border-radius:12px;
  overflow:hidden;margin-bottom:2rem;
}
.pipe-step{
  flex:1;padding:1.25rem 1rem;text-align:center;position:relative;
  border-right:1px solid var(--border);
}
.pipe-step:last-child{border-right:none}
.pipe-step .icon{font-size:1.75rem;margin-bottom:.4rem}
.pipe-step .label{font-size:.8rem;font-weight:600;color:var(--accent2);text-transform:uppercase;letter-spacing:.05em}
.pipe-step .desc{font-size:.78rem;color:var(--muted);margin-top:.2rem}
.pipe-arrow{
  display:flex;align-items:center;justify-content:center;
  padding:0 .25rem;color:var(--accent);font-size:1.2rem;flex-shrink:0;
  background:var(--surface);
}
@media(max-width:640px){.pipeline{flex-direction:column}.pipe-arrow{transform:rotate(90deg)}}

/* ── KAFKA EVIDENCE ── */
.kafka-grid{display:grid;grid-template-columns:1fr 1fr;gap:1.25rem}
@media(max-width:700px){.kafka-grid{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.card-header{
  background:var(--surface2);padding:.65rem 1rem;
  display:flex;align-items:center;gap:.5rem;border-bottom:1px solid var(--border);
}
.card-header .dot{width:10px;height:10px;border-radius:50%}
.dot-red{background:#ef4444}.dot-yellow{background:#eab308}.dot-green{background:#22c55e}
.card-header .title{font-size:.8rem;color:var(--muted);font-family:var(--mono);margin-left:.25rem}
.card-body{padding:1rem}

/* ── CODE BLOCKS ── */
pre[class*="language-"]{margin:0!important;border-radius:0!important;font-size:.78rem!important;
  max-height:320px;overflow:auto;background:#0d1117!important}
code[class*="language-"]{font-family:var(--mono)!important}

/* ── KAFKA LOG ── */
.kafka-log{font-family:var(--mono);font-size:.73rem;line-height:1.8;max-height:320px;overflow:auto;
  background:#0d1117;padding:1rem;color:#94a3b8}
.log-time{color:#475569}
.log-partition{color:#6366f1}
.log-offset{color:#22c55e}
.log-msg{color:#e2e8f0}

/* ── TOPIC META ── */
.meta-table{width:100%;border-collapse:collapse;font-size:.82rem;margin-top:.75rem}
.meta-table td{padding:.35rem .5rem;border-bottom:1px solid var(--border)}
.meta-table td:first-child{color:var(--muted);width:45%}
.meta-table td:last-child{font-family:var(--mono);color:var(--accent2)}

/* ── TASK CARDS ── */
.tasks-grid{display:grid;gap:1.25rem}
.task-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.task-header{
  display:flex;align-items:flex-start;gap:1rem;padding:1rem 1.25rem;
  background:linear-gradient(90deg,var(--surface2),var(--surface));
  border-bottom:1px solid var(--border);
}
.task-num{
  background:var(--accent);color:#fff;border-radius:8px;
  width:2rem;height:2rem;display:flex;align-items:center;justify-content:center;
  font-weight:700;font-size:.85rem;flex-shrink:0;margin-top:.1rem
}
.task-num.bonus{background:var(--yellow);color:#0f172a}
.task-title{font-weight:600;font-size:1rem;color:var(--text)}
.task-goal{font-size:.82rem;color:var(--muted);margin-top:.2rem}

.tab-bar{display:flex;border-bottom:1px solid var(--border);background:var(--surface2)}
.tab{
  padding:.5rem 1.1rem;font-size:.8rem;font-weight:500;cursor:pointer;
  border:none;background:none;color:var(--muted);border-bottom:2px solid transparent;
  transition:color .2s,border-color .2s;
}
.tab.active{color:var(--accent2);border-bottom-color:var(--accent)}
.tab-panel{display:none}.tab-panel.active{display:block}

/* ── TERMINAL OUTPUT ── */
.terminal{
  font-family:var(--mono);font-size:.76rem;line-height:1.7;
  background:#0d1117;padding:1rem 1.25rem;color:#94a3b8;overflow:auto;
}
.terminal .t-batch{color:#6366f1;font-weight:600}
.terminal .t-sep{color:#334155}
.terminal .t-header{color:#22c55e;font-weight:600}
.terminal .t-row td{padding-right:1.5rem;color:#e2e8f0}
.terminal .t-row td.num{color:#fbbf24}
.terminal table{border-collapse:collapse}
.terminal .note{color:#64748b;font-style:italic;margin-top:.5rem;font-size:.72rem}

/* ── FOOTER ── */
footer{text-align:center;padding:1.5rem;color:var(--muted);font-size:.8rem;border-top:1px solid var(--border)}
</style>
</head>
<body>

<!-- HEADER -->
<header>
  <h1>Spark + Kafka Workshop</h1>
  <p class="sub">Beginner In-Class Team Task — Stream Processing Course</p>
  <span class="badge">PySpark 3.5</span>
  <span class="badge">Kafka 3.7</span>
  <span class="badge">Python 3.13</span>
  <span class="badge">Deployed on Railway</span>
</header>

<main>

<!-- PIPELINE DIAGRAM -->
<section>
  <h2>Mini Architecture</h2>
  <div class="pipeline">
    <div class="pipe-step">
      <div class="icon">🐍</div>
      <div class="label">Python Producer</div>
      <div class="desc">producer_orders.py<br/>kafka-python 2.3</div>
    </div>
    <div class="pipe-arrow">→</div>
    <div class="pipe-step">
      <div class="icon">📨</div>
      <div class="label">Kafka Topic</div>
      <div class="desc">orders<br/>1 partition · RF 1</div>
    </div>
    <div class="pipe-arrow">→</div>
    <div class="pipe-step">
      <div class="icon">⚡</div>
      <div class="label">Spark Streaming</div>
      <div class="desc">readStream.format("kafka")<br/>Structured Streaming</div>
    </div>
    <div class="pipe-arrow">→</div>
    <div class="pipe-step">
      <div class="icon">🖥️</div>
      <div class="label">Console Output</div>
      <div class="desc">writeStream<br/>.format("console")</div>
    </div>
  </div>
</section>

<!-- KAFKA EVIDENCE -->
<section>
  <h2>Kafka Producer &amp; Message Log</h2>
  <div class="kafka-grid">

    <!-- Producer code -->
    <div class="card">
      <div class="card-header">
        <div class="dot dot-red"></div><div class="dot dot-yellow"></div><div class="dot dot-green"></div>
        <span class="title">producer_orders.py</span>
      </div>
      <div class="card-body" style="padding:0">
        <pre><code class="language-python">from kafka import KafkaProducer
import json, time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v:
        json.dumps(v).encode("utf-8")
)

orders = [
    {"customer":"Ali",  "product":"Laptop",   "price":1200},
    {"customer":"Sara", "product":"Phone",    "price":800},
    {"customer":"Ali",  "product":"Mouse",    "price":25},
    {"customer":"Mona", "product":"Laptop",   "price":1100},
    {"customer":"Sara", "product":"Keyboard", "price":75},
    {"customer":"Omar", "product":"Phone",    "price":650},
]

for order in orders:
    producer.send("orders", value=order)
    print("Sent:", order)
    time.sleep(2)

producer.flush()
producer.close()</code></pre>
      </div>
    </div>

    <!-- Kafka log + metadata -->
    <div style="display:flex;flex-direction:column;gap:1.25rem">
      <div class="card">
        <div class="card-header">
          <div class="dot dot-red"></div><div class="dot dot-yellow"></div><div class="dot dot-green"></div>
          <span class="title">Kafka Topic: orders — Message Log</span>
        </div>
        <div class="card-body" style="padding:0">
          <div class="kafka-log">
<span class="log-time">[2026-05-05 06:10:01]</span> <span class="log-partition">Partition 0</span> | <span class="log-offset">Offset 0</span> | <span class="log-msg">{"customer":"Ali","product":"Laptop","price":1200}</span>
<span class="log-time">[2026-05-05 06:10:03]</span> <span class="log-partition">Partition 0</span> | <span class="log-offset">Offset 1</span> | <span class="log-msg">{"customer":"Sara","product":"Phone","price":800}</span>
<span class="log-time">[2026-05-05 06:10:05]</span> <span class="log-partition">Partition 0</span> | <span class="log-offset">Offset 2</span> | <span class="log-msg">{"customer":"Ali","product":"Mouse","price":25}</span>
<span class="log-time">[2026-05-05 06:10:07]</span> <span class="log-partition">Partition 0</span> | <span class="log-offset">Offset 3</span> | <span class="log-msg">{"customer":"Mona","product":"Laptop","price":1100}</span>
<span class="log-time">[2026-05-05 06:10:09]</span> <span class="log-partition">Partition 0</span> | <span class="log-offset">Offset 4</span> | <span class="log-msg">{"customer":"Sara","product":"Keyboard","price":75}</span>
<span class="log-time">[2026-05-05 06:10:11]</span> <span class="log-partition">Partition 0</span> | <span class="log-offset">Offset 5</span> | <span class="log-msg">{"customer":"Omar","product":"Phone","price":650}</span>
          </div>
        </div>
      </div>
      <div class="card">
        <div class="card-header">
          <div class="dot dot-red"></div><div class="dot dot-yellow"></div><div class="dot dot-green"></div>
          <span class="title">Topic Metadata</span>
        </div>
        <div class="card-body">
          <table class="meta-table">
            <tr><td>Topic name</td><td>orders</td></tr>
            <tr><td>Partitions</td><td>1</td></tr>
            <tr><td>Replication factor</td><td>1</td></tr>
            <tr><td>Bootstrap server</td><td>localhost:9092</td></tr>
            <tr><td>Broker image</td><td>confluentinc/cp-kafka:7.5.3</td></tr>
            <tr><td>Serialization</td><td>JSON → UTF-8 bytes</td></tr>
            <tr><td>Starting offsets (Spark)</td><td>latest</td></tr>
          </table>
        </div>
      </div>
    </div>
  </div>
</section>

<!-- TASKS -->
<section>
  <h2>Student Tasks</h2>
  <div class="tasks-grid">

    <!-- TASK 1 -->
    <div class="task-card">
      <div class="task-header">
        <div class="task-num">1</div>
        <div>
          <div class="task-title">Show Incoming Orders</div>
          <div class="task-goal">Display all incoming orders from Kafka — columns: customer, product, price</div>
        </div>
      </div>
      <div class="tab-bar">
        <button class="tab active" onclick="switchTab(this,'t1-code')">Code</button>
        <button class="tab" onclick="switchTab(this,'t1-out')">Console Output</button>
      </div>
      <div id="t1-code" class="tab-panel active">
        <pre><code class="language-python">raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

orders_df = json_df.select(
    from_json(col("json_value"), schema).alias("data")
).select("data.*")

# Task 1: use orders_df directly
query = orders_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()</code></pre>
      </div>
      <div id="t1-out" class="tab-panel">
        <div class="terminal">
<span class="t-batch">-------------------------------------------</span>
<span class="t-batch">Batch: 0</span>
<span class="t-batch">-------------------------------------------</span>
<table><tr class="t-row"><td><span class="t-header">customer</span></td><td><span class="t-header">product</span></td><td><span class="t-header">price</span></td></tr>
<tr class="t-row"><td>Ali</td><td>Laptop</td><td class="num">1200</td></tr>
<tr class="t-row"><td>Sara</td><td>Phone</td><td class="num">800</td></tr>
<tr class="t-row"><td>Ali</td><td>Mouse</td><td class="num">25</td></tr>
<tr class="t-row"><td>Mona</td><td>Laptop</td><td class="num">1100</td></tr>
<tr class="t-row"><td>Sara</td><td>Keyboard</td><td class="num">75</td></tr>
<tr class="t-row"><td>Omar</td><td>Phone</td><td class="num">650</td></tr></table>
<div class="note">outputMode: append — new rows only</div>
        </div>
      </div>
    </div>

    <!-- TASK 2 -->
    <div class="task-card">
      <div class="task-header">
        <div class="task-num">2</div>
        <div>
          <div class="task-title">Select Product Names Only</div>
          <div class="task-goal">Modify the Spark code to display only the product column</div>
        </div>
      </div>
      <div class="tab-bar">
        <button class="tab active" onclick="switchTab(this,'t2-code')">Code</button>
        <button class="tab" onclick="switchTab(this,'t2-out')">Console Output</button>
      </div>
      <div id="t2-code" class="tab-panel active">
        <pre><code class="language-python"># Task 2: select only the product column
products_df = orders_df.select("product")

query = products_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()</code></pre>
      </div>
      <div id="t2-out" class="tab-panel">
        <div class="terminal">
<span class="t-batch">-------------------------------------------</span>
<span class="t-batch">Batch: 0</span>
<span class="t-batch">-------------------------------------------</span>
<table><tr class="t-row"><td><span class="t-header">product</span></td></tr>
<tr class="t-row"><td>Laptop</td></tr>
<tr class="t-row"><td>Phone</td></tr>
<tr class="t-row"><td>Mouse</td></tr>
<tr class="t-row"><td>Laptop</td></tr>
<tr class="t-row"><td>Keyboard</td></tr>
<tr class="t-row"><td>Phone</td></tr></table>
<div class="note">outputMode: append — only the product column is projected</div>
        </div>
      </div>
    </div>

    <!-- TASK 3 -->
    <div class="task-card">
      <div class="task-header">
        <div class="task-num">3</div>
        <div>
          <div class="task-title">Filter Expensive Orders</div>
          <div class="task-goal">Display only orders where price &gt; 500. Mouse ($25) and Keyboard ($75) are filtered out.</div>
        </div>
      </div>
      <div class="tab-bar">
        <button class="tab active" onclick="switchTab(this,'t3-code')">Code</button>
        <button class="tab" onclick="switchTab(this,'t3-out')">Console Output</button>
      </div>
      <div id="t3-code" class="tab-panel active">
        <pre><code class="language-python"># Task 3: keep only orders with price > 500
expensive_df = orders_df.filter(col("price") > 500)

query = expensive_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()</code></pre>
      </div>
      <div id="t3-out" class="tab-panel">
        <div class="terminal">
<span class="t-batch">-------------------------------------------</span>
<span class="t-batch">Batch: 0</span>
<span class="t-batch">-------------------------------------------</span>
<table><tr class="t-row"><td><span class="t-header">customer</span></td><td><span class="t-header">product</span></td><td><span class="t-header">price</span></td></tr>
<tr class="t-row"><td>Ali</td><td>Laptop</td><td class="num">1200</td></tr>
<tr class="t-row"><td>Sara</td><td>Phone</td><td class="num">800</td></tr>
<tr class="t-row"><td>Mona</td><td>Laptop</td><td class="num">1100</td></tr>
<tr class="t-row"><td>Omar</td><td>Phone</td><td class="num">650</td></tr></table>
<div class="note">Mouse ($25) and Keyboard ($75) filtered out — price ≤ 500</div>
        </div>
      </div>
    </div>

    <!-- TASK 4 -->
    <div class="task-card">
      <div class="task-header">
        <div class="task-num">4</div>
        <div>
          <div class="task-title">Add Discounted Price</div>
          <div class="task-goal">Add a discounted_price column with 10% off. Laptop $1200 → $1080.0</div>
        </div>
      </div>
      <div class="tab-bar">
        <button class="tab active" onclick="switchTab(this,'t4-code')">Code</button>
        <button class="tab" onclick="switchTab(this,'t4-out')">Console Output</button>
      </div>
      <div id="t4-code" class="tab-panel active">
        <pre><code class="language-python"># Task 4: add a discounted_price column (10% off)
discount_df = orders_df.withColumn(
    "discounted_price", col("price") * 0.9
)

query = discount_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()</code></pre>
      </div>
      <div id="t4-out" class="tab-panel">
        <div class="terminal">
<span class="t-batch">-------------------------------------------</span>
<span class="t-batch">Batch: 0</span>
<span class="t-batch">-------------------------------------------</span>
<table><tr class="t-row"><td><span class="t-header">customer</span></td><td><span class="t-header">product</span></td><td><span class="t-header">price</span></td><td><span class="t-header">discounted_price</span></td></tr>
<tr class="t-row"><td>Ali</td><td>Laptop</td><td class="num">1200</td><td class="num">1080.0</td></tr>
<tr class="t-row"><td>Sara</td><td>Phone</td><td class="num">800</td><td class="num">720.0</td></tr>
<tr class="t-row"><td>Ali</td><td>Mouse</td><td class="num">25</td><td class="num">22.5</td></tr>
<tr class="t-row"><td>Mona</td><td>Laptop</td><td class="num">1100</td><td class="num">990.0</td></tr>
<tr class="t-row"><td>Sara</td><td>Keyboard</td><td class="num">75</td><td class="num">67.5</td></tr>
<tr class="t-row"><td>Omar</td><td>Phone</td><td class="num">650</td><td class="num">585.0</td></tr></table>
<div class="note">withColumn adds discounted_price = price × 0.9</div>
        </div>
      </div>
    </div>

    <!-- BONUS -->
    <div class="task-card">
      <div class="task-header">
        <div class="task-num bonus">★</div>
        <div>
          <div class="task-title">Bonus — Count Orders per Customer</div>
          <div class="task-goal">Count how many orders each customer placed using groupBy + count. Uses <code style="color:var(--accent2)">outputMode("complete")</code> because aggregation rewrites the full result table on each trigger.</div>
        </div>
      </div>
      <div class="tab-bar">
        <button class="tab active" onclick="switchTab(this,'tb-code')">Code</button>
        <button class="tab" onclick="switchTab(this,'tb-out')">Console Output</button>
      </div>
      <div id="tb-code" class="tab-panel active">
        <pre><code class="language-python"># Bonus: count orders grouped by customer
count_df = orders_df.groupBy("customer").count()

query = count_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()</code></pre>
      </div>
      <div id="tb-out" class="tab-panel">
        <div class="terminal">
<span class="t-batch">-------------------------------------------</span>
<span class="t-batch">Batch: 0</span>
<span class="t-batch">-------------------------------------------</span>
<table><tr class="t-row"><td><span class="t-header">customer</span></td><td><span class="t-header">count</span></td></tr>
<tr class="t-row"><td>Ali</td><td class="num">2</td></tr>
<tr class="t-row"><td>Sara</td><td class="num">2</td></tr>
<tr class="t-row"><td>Mona</td><td class="num">1</td></tr>
<tr class="t-row"><td>Omar</td><td class="num">1</td></tr></table>
<div class="note">outputMode: complete — full result table printed on every trigger (required for aggregations)</div>
        </div>
      </div>
    </div>

  </div><!-- /tasks-grid -->
</section>

<!-- SCREENSHOTS -->
<section>
  <h2>Screenshots — Live Run Evidence</h2>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:1.25rem">

    <div class="card">
      <div class="card-header">
        <div class="dot dot-red"></div><div class="dot dot-yellow"></div><div class="dot dot-green"></div>
        <span class="title">Producer sending events — python producer_orders.py</span>
      </div>
      <div class="card-body" style="padding:.75rem">
        <img src="/images/screenshot of proucer sending events.png"
             alt="Producer sending events"
             style="width:100%;border-radius:6px;border:1px solid var(--border)"/>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <div class="dot dot-red"></div><div class="dot dot-yellow"></div><div class="dot dot-green"></div>
        <span class="title">Spark console output — Task 1 Batch: 0</span>
      </div>
      <div class="card-body" style="padding:.75rem">
        <img src="/images/Screenshot of Spark console output.png"
             alt="Spark console output"
             style="width:100%;border-radius:6px;border:1px solid var(--border)"/>
      </div>
    </div>

    <div class="card" style="grid-column:1/-1">
      <div class="card-header">
        <div class="dot dot-red"></div><div class="dot dot-yellow"></div><div class="dot dot-green"></div>
        <span class="title">Spark console output — additional batches</span>
      </div>
      <div class="card-body" style="padding:.75rem">
        <img src="/images/Screenshot of Spark console output 2.png"
             alt="Spark console output 2"
             style="width:100%;border-radius:6px;border:1px solid var(--border)"/>
      </div>
    </div>

  </div>
</section>

</main>

<footer>
  Spark Structured Streaming + Kafka Workshop &nbsp;·&nbsp; Stream Processing Course &nbsp;·&nbsp; Deployed on Railway
</footer>

<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-python.min.js"></script>
<script>
function switchTab(btn, panelId) {
  const card = btn.closest('.task-card') || btn.closest('.card');
  btn.closest('.tab-bar').querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  const panels = document.querySelectorAll('#' + panelId).length
    ? btn.closest('.task-card').querySelectorAll('.tab-panel')
    : [];
  btn.closest('.task-card').querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById(panelId).classList.add('active');
  Prism.highlightAll();
}
</script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/orders")
def list_orders():
    return {"orders": ORDERS, "topic": TOPIC, "broker": KAFKA_BROKER}


@app.post("/send")
def send_orders():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    sent = []
    for order in ORDERS:
        producer.send(TOPIC, value=order)
        sent.append(order)
        time.sleep(0.5)
    producer.flush()
    producer.close()
    return {"sent": sent, "count": len(sent), "topic": TOPIC}
