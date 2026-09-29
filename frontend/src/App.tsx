import { Suspense, lazy, useEffect, useState } from 'react';

import { AdminView } from './components/AdminView';
import { QueryComposer } from './components/QueryComposer';
import { TrustBadge } from './components/TrustBadge';
import { ResultTable } from './components/ResultTable';
import { UsedMomentsTable } from './components/UsedMomentsTable';
import { analyzeQuestion } from './lib/api';
import {
  completeRedirectSignIn,
  observeAuthState,
  signInWithGoogle,
  signOutUser,
  type User,
} from './lib/firebase';
import { formatBytes, titleCaseWords } from './lib/format';
import type { AnalyzeResponse } from './types';

const DEFAULT_QUESTION = 'Miten asumistuen menot ovat kehittyneet 2020-2024?';
const ChartPanel = lazy(async () => {
  const module = await import('./components/ChartPanel');
  return { default: module.ChartPanel };
});

function getSessionId(): string {
  const key = 'budjettihaukka_session_id';
  const existing = window.sessionStorage.getItem(key);
  if (existing) return existing;
  const next = window.crypto.randomUUID();
  window.sessionStorage.setItem(key, next);
  return next;
}

function MetaPill({ label, value }: { label: string; value: string | null | undefined }) {
  if (!value) return null;
  return (
    <span className="meta-pill">
      <strong>{label}:</strong> {value}
    </span>
  );
}

function Footer() {
  return (
    <footer className="footer-shell">
      <div className="footer-campaign">#RohkeuttaPriorisoida</div>
      <a
        className="footer-logo-link"
        href="https://liberaalipuolue.fi/rohkeuttapriorisoida/"
        target="_blank"
        rel="noopener noreferrer"
        aria-label="Avaa Rohkeutta priorisoida -sivu"
      >
        <div className="footer-logo">liberaalipuolue.fi</div>
      </a>
      <div className="footer-socials" aria-label="Liberaalipuolueen sosiaalinen media">
        <a className="footer-social" href="https://www.facebook.com/liberaalipuolue/" target="_blank" rel="noopener noreferrer">
          Facebook
        </a>
        <a className="footer-social" href="https://instagram.com/liberaalipuolue/" target="_blank" rel="noopener noreferrer">
          Instagram
        </a>
        <a className="footer-social" href="https://twitter.com/liberaalipuolue/" target="_blank" rel="noopener noreferrer">
          X
        </a>
        <a className="footer-social" href="https://liberaalipuolue.fi/chat/" target="_blank" rel="noopener noreferrer">
          Discord
        </a>
      </div>
    </footer>
  );
}

function TopNav({ adminMode, user, onSignOut }: { adminMode: boolean; user: User; onSignOut: () => void }) {
  const href = adminMode ? '?' : '?admin=1';
  const label = adminMode ? 'Etusivu' : 'Admin';
  return (
    <div className="topnav">
      <div className="topnav-links">
        <a className="topnav-link" href="./liberaali-historiallinen-vastelaskelma.html">
          Vaihtoehtolaskelma
        </a>
        <a className="topnav-link" href="./liberaali-vaihtoehtobudjetti-2026.html">
          Vaiheistettu suunnitelma
        </a>
        <a className="topnav-link" href={href}>
          {label}
        </a>
      </div>
      <div className="account-menu">
        {user.photoURL ? <img className="account-avatar" src={user.photoURL} alt="" referrerPolicy="no-referrer" /> : null}
        <span className="account-name">{user.displayName || user.email || 'Google-käyttäjä'}</span>
        <button className="topnav-link account-signout" type="button" onClick={onSignOut}>
          Kirjaudu ulos
        </button>
      </div>
    </div>
  );
}

function LoginScreen({ busy, error, onSignIn }: { busy: boolean; error: string | null; onSignIn: () => void }) {
  return (
    <main className="login-shell">
      <section className="login-card">
        <div className="login-kicker">Budjettihaukka</div>
        <h1>Kirjaudu analytiikkaan</h1>
        <p>
          Käytä Google-tiliäsi Budjettihaukan analytiikan ja kysymyskirjaston avaamiseen.
        </p>
        <button className="google-login-button" type="button" onClick={onSignIn} disabled={busy}>
          <span className="google-mark" aria-hidden="true">G</span>
          {busy ? 'Kirjaudutaan…' : 'Kirjaudu Google-tilillä'}
        </button>
        {error ? <div className="login-error" role="alert">{error}</div> : null}
        <p className="login-note">
          Palvelu tallentaa esitetyt kysymykset palvelun laadun ja vastausten luotettavuuden kehittämistä varten.
        </p>
      </section>
    </main>
  );
}

function StatusBlock({ response }: { response: AnalyzeResponse }) {
  const observability = response.resolved_analysis?.observability_class as string | undefined;
  const fiscalSide = response.resolved_analysis?.fiscal_side as string | undefined;
  const concept = response.resolved_analysis?.concept_label as string | undefined;

  return (
    <section className="panel response-panel">
      <div className="panel-header tight">
        <div>
          <div className="section-kicker">Vastaus</div>
          <h2>{response.question}</h2>
        </div>
        <div className="panel-meta">{response.query_source || 'analytiikka-api'}</div>
      </div>

      <TrustBadge status={response.verification_status} observability={observability ? titleCaseWords(observability) : undefined} />

      <div className="meta-row">
        <MetaPill label="Konsepti" value={concept} />
        <MetaPill label="Budjettipuoli" value={fiscalSide ? titleCaseWords(fiscalSide) : undefined} />
        <MetaPill label="Contract" value={response.query_contract} />
        <MetaPill label="Dry run" value={formatBytes(response.dry_run_bytes)} />
      </div>

      <div className="explanation-card">
        <p>{response.explanation || 'Ei selitystekstiä.'}</p>
      </div>

      {response.warnings?.length ? (
        <div className="warning-stack">
          {response.warnings.map((warning, index) => (
            <div key={`${index}-${warning}`} className="warning-card">
              {warning}
            </div>
          ))}
        </div>
      ) : null}

      {response.status === 'clarification_required' ? (
        <div className="clarification-block">
          <h3>Tarkennus tarvitaan</h3>
          <p>Analyysi pysähtyi mieluummin tarkennukseen kuin arvasi liian rohkeasti.</p>
        </div>
      ) : null}

      {response.status === 'unsupported' ? (
        <div className="clarification-block unsupported-block">
          <h3>Tätä ei kannata pakottaa väkisin chartiksi</h3>
          <p>
            Backend arvioi, ettei kysymykseen voi vastata nykyisestä datasta riittävän tarkasti ilman harhaanjohtavan
            tuloksen riskiä.
          </p>
        </div>
      ) : null}
    </section>
  );
}

export default function App() {
  const adminMode = typeof window !== 'undefined' && new URLSearchParams(window.location.search).get('admin') === '1';
  const [authReady, setAuthReady] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [authBusy, setAuthBusy] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [question, setQuestion] = useState(DEFAULT_QUESTION);
  const [response, setResponse] = useState<AnalyzeResponse | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const unsubscribe = observeAuthState((nextUser) => {
      setUser(nextUser);
      setAuthReady(true);
    });
    completeRedirectSignIn().catch((err) => {
      setAuthError(err instanceof Error ? err.message : 'Google-kirjautuminen epäonnistui.');
      setAuthReady(true);
    });
    return unsubscribe;
  }, []);

  async function signIn() {
    setAuthBusy(true);
    setAuthError(null);
    try {
      await signInWithGoogle();
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : 'Google-kirjautuminen epäonnistui.');
    } finally {
      setAuthBusy(false);
    }
  }

  async function signOut() {
    setAuthError(null);
    try {
      await signOutUser();
      setResponse(null);
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : 'Uloskirjautuminen epäonnistui.');
    }
  }

  async function submit(nextQuestion: string) {
    setQuestion(nextQuestion);
    setBusy(true);
    setError(null);
    try {
      const next = await analyzeQuestion({
        question: nextQuestion,
        language: 'fi',
        ui_context: { surface: 'firebase-frontend', session_id: getSessionId() },
      });
      setResponse(next);
    } catch (err) {
      setResponse(null);
      setError(err instanceof Error ? err.message : 'Tuntematon virhe');
    } finally {
      setBusy(false);
    }
  }

  if (!authReady) {
    return <LoginScreen busy error={null} onSignIn={() => undefined} />;
  }

  if (!user) {
    return <LoginScreen busy={authBusy} error={authError} onSignIn={signIn} />;
  }

  return (
    <div className="app-shell">
      <TopNav adminMode={adminMode} user={user} onSignOut={signOut} />

      <main className="content-grid">
        {adminMode ? (
          <AdminView />
        ) : (
          <>
            <QueryComposer initialValue={question} busy={busy} onSubmit={submit} />

            {error ? (
              <section className="panel error-panel">
                <div className="section-kicker">Virhe</div>
                <h2>Analyysiä ei voitu hakea</h2>
                <p>{error}</p>
              </section>
            ) : null}

            {response ? (
              <>
                <StatusBlock response={response} />
                {response.status === 'success' && response.analytics_frame?.rows?.length ? (
                  <Suspense
                    fallback={
                      <section className="panel empty-chart-panel">
                        <h2>Visualisointi</h2>
                        <p>Chart-kirjastoa ladataan…</p>
                      </section>
                    }
                  >
                    <ChartPanel response={response} />
                  </Suspense>
                ) : null}
                <ResultTable response={response} />
                <UsedMomentsTable response={response} />
              </>
            ) : null}
          </>
        )}
      </main>

      <Footer />
    </div>
  );
}
