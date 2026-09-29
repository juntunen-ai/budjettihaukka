import type { VerificationStatus } from '../types';

const TRUST_META: Record<string, { label: string; tone: string; note: string }> = {
  trusted: {
    label: 'Luotettava',
    tone: 'trusted',
    note: 'Tulkinta ja rajaus näyttävät vahvoilta tämän kysymyksen kannalta.',
  },
  trusted_with_warning: {
    label: 'Luotettava rajauksin',
    tone: 'warning',
    note: 'Vastaus on käyttökelpoinen, mutta mukana on tulkintarajauksia tai varoituksia.',
  },
  needs_clarification: {
    label: 'Tarkennus tarvitaan',
    tone: 'clarify',
    note: 'Järjestelmä pysähtyi mieluummin tarkennukseen kuin arvasi liian rohkeasti.',
  },
  unsupported: {
    label: 'Ei riittävän luotettava',
    tone: 'unsupported',
    note: 'Nykyinen data tai rajaus ei riitä luotettavaan vastaukseen.',
  },
};

export function TrustBadge({ status, observability }: { status: VerificationStatus | null; observability?: string | null }) {
  const meta = TRUST_META[status || ''] || TRUST_META.trusted_with_warning;
  return (
    <div className="trust-shell">
      <div className="trust-main">
        <span className={`trust-badge trust-${meta.tone}`}>{meta.label}</span>
        <span className="trust-note">{meta.note}</span>
      </div>
      {observability ? <span className="observability-pill">Mitattavuus: {observability}</span> : null}
    </div>
  );
}
