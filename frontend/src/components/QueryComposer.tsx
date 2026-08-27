import { useEffect, useState } from 'react';

const SAMPLE_QUESTIONS = [
  'Miten asumistuen menot ovat kehittyneet 2020-2024?',
  'Miten puolustusmenot kehittyivät 2018-2024?',
  'Mitkä budjettimomentit kasvoivat eniten 2010-2024?',
  'Arvonlisäveron kehitys 2023-2025',
];

export function QueryComposer({
  initialValue,
  busy,
  onSubmit,
}: {
  initialValue: string;
  busy: boolean;
  onSubmit: (value: string) => void;
}) {
  const [draft, setDraft] = useState(initialValue);

  useEffect(() => {
    setDraft(initialValue);
  }, [initialValue]);

  return (
    <section className="query-card panel">
      <h1>Budjettihaukka</h1>
      <p className="campaign-copy">
        Budjettihaukka tekee julkisesta taloudesta läpinäkyvämpää ja päätöksenteosta ymmärrettävämpää.
        Sen tavoitteena on auttaa näkemään, mihin yhteisiä varoja käytetään, miten priorisoinnit muuttuvat
        ja mitä vaikutuksia niillä voi olla vapauden, vastuun ja tehokkuuden näkökulmasta.
      </p>
      <form
        className="query-form"
        onSubmit={(event) => {
          event.preventDefault();
          const trimmed = draft.trim();
          if (!trimmed || busy) return;
          onSubmit(trimmed);
        }}
      >
        <label htmlFor="question" className="sr-only">
          Kirjoita kysymys
        </label>
        <textarea
          id="question"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          className="query-input"
          rows={4}
          placeholder="Esim. Miten yliopistojen rahoitus on kehittynyt 2010-2024?"
        />
        <div className="query-actions">
          <button type="submit" className="primary-button" disabled={busy}>
            {busy ? 'Analysoidaan…' : 'Hae analyysi'}
          </button>
        </div>
      </form>
      <div className="sample-row">
        {SAMPLE_QUESTIONS.map((question) => (
          <button key={question} type="button" className="sample-chip" onClick={() => { setDraft(question); onSubmit(question); }}>
            {question}
          </button>
        ))}
      </div>
      <p className="query-privacy-note">
        Kysymys ja analyysin tekninen lopputulos tallennetaan palvelun laadun parantamista varten. Älä kirjoita
        kysymykseen henkilötietoja.
      </p>
    </section>
  );
}
