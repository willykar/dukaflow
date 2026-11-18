# web

The React + TypeScript dashboard is created in phase 6.2:

```bash
npm create vite@latest web -- --template react-ts
cd web && npm install && npm install recharts
```

Pages: Today (live tiles), Stock, M-Pesa reconciliation, Trends, Ask your shop. It talks to the FastAPI
backend (`API_CORS_ORIGINS` in `.env`) and subscribes to `/alerts/stream` for live updates.
