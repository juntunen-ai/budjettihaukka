# Budjettihaukka Frontend

Firebase Hosting- ja GitHub Pages -yhteensopiva React/Vite/ECharts-frontend Budjettihaukan FastAPI-backendin päälle.

## Kehitys

```bash
cd frontend
npm install
npm run dev
```

Kehityspalvelin välittää `/v1/**`- ja `/health`-pyynnöt oletuksena osoitteeseen `http://127.0.0.1:8000`.

## Build

```bash
cd frontend
npm run build
```

## Ympäristömuuttujat

- `VITE_API_BASE_URL`
- `VITE_BASE_PATH`
- `VITE_DEV_API_TARGET` controls the local Vite proxy target. Production
  Firebase builds leave `VITE_API_BASE_URL` empty and use same-origin
  `/v1/**` and `/health` Hosting rewrites.
