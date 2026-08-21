# Budjettihaukka Frontend

Ensimmäinen GitHub Pages -yhteensopiva React/Vite/ECharts-frontend Budjettihaukan nykyisen FastAPI-backendin päälle.

## Kehitys

```bash
cd frontend
npm install
npm run dev
```

Oletuksena frontend kutsuu APIa osoitteessa `http://127.0.0.1:8000`.

## Build

```bash
cd frontend
npm run build
```

## Ympäristömuuttujat

- `VITE_API_BASE_URL`
- `VITE_BASE_PATH`
