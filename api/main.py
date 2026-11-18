"""FastAPI backend for the dashboard (phase 6.1). Run: uvicorn api.main:app --reload"""

from fastapi import FastAPI

app = FastAPI(title="DukaFlow API")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# TODO (phase 6.1):
#   GET  /shops/{shop_id}/today         live tiles from PostgreSQL
#   GET  /shops/{shop_id}/stock-cover   mart_stock_cover
#   GET  /shops/{shop_id}/reconciliation mart_mpesa_reconciliation
#   GET  /shops/{shop_id}/trends        mart_daily_sales
#   POST /ask                           agent.ask
#   GET  /alerts/stream                 Server-Sent Events from Kafka pos.alerts
