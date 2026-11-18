import React from 'react';
import ReactDOM from 'react-dom/client';

import App from './App';
import './styles.css';

// Redirect sign-in needs the app and Firebase auth helper on the same origin
// in browsers that partition third-party storage. Set explicitly for each release.
const canonicalOrigin = import.meta.env.VITE_CANONICAL_ORIGIN;
if (canonicalOrigin && window.location.origin !== canonicalOrigin) {
  window.location.replace(new URL(window.location.pathname + window.location.search + window.location.hash, canonicalOrigin).href);
} else {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
      {import.meta.env.VITE_STAGING_NOTICE ? (
        <aside role="note" style={{ padding: '12px 20px', background: '#fff3cd', color: '#553d00', textAlign: 'center' }}>
          {import.meta.env.VITE_STAGING_NOTICE}
        </aside>
      ) : null}
      <App />
    </React.StrictMode>,
  );
}
