// The side panels around the phone: design system, per-screen notes, and the Tweaks panel.

function SystemRail() {
  return (
    <div style={{ fontFamily: 'var(--font-sans)' }}>
      <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 14 }}>
        Ampeer · design system
      </div>

      {/* Colors */}
      <SectionTitle>Color</SectionTitle>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 1fr)', gap: 6, marginBottom: 6 }}>
        {[
          ['--forest-900','900'],
          ['--forest-700','700'],
          ['--forest-500','500'],
          ['--forest-300','300'],
          ['--sage-100','100'],
          ['--sage-50','50'],
        ].map(([v, n]) => (
          <div key={v} style={{
            aspectRatio: '1', borderRadius: 6, background: `var(${v})`,
            border: '1px solid rgba(0,0,0,0.06)',
            display: 'flex', alignItems: 'flex-end', justifyContent: 'flex-start',
            padding: 3, fontSize: 9,
            color: n === '50' || n === '100' || n === '300' ? 'var(--ink-700)' : 'rgba(255,255,255,0.8)',
            fontFamily: 'var(--font-mono)',
          }}>{n}</div>
        ))}
      </div>
      <div style={{ fontSize: 11, color: 'var(--ink-400)', lineHeight: 1.4 }}>
        Forest primary · sage tints · warm ink neutrals
      </div>

      {/* Type */}
      <SectionTitle top={20}>Type</SectionTitle>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <TypeRow label="Display · 40 / Instrument Serif">
          <span className="t-display" style={{ fontSize: 22, color: 'var(--ink-900)' }}>Let's find your grid.</span>
        </TypeRow>
        <TypeRow label="Body · 15 / Instrument Sans">
          <span style={{ fontSize: 13, color: 'var(--ink-700)' }}>Your postcode tells us which neighbors…</span>
        </TypeRow>
        <TypeRow label="Number · Instrument Serif tabular">
          <span className="t-num" style={{ fontSize: 22, color: 'var(--ink-900)' }}>£180–340</span>
        </TypeRow>
        <TypeRow label="Label · 11 uppercase">
          <span className="t-label" style={{ color: 'var(--ink-500)' }}>Estimated savings</span>
        </TypeRow>
      </div>

      {/* Radius */}
      <SectionTitle top={20}>Radius · shadows</SectionTitle>
      <div style={{ display: 'flex', gap: 8, alignItems: 'flex-end' }}>
        {[
          { r: 10, h: 32 },
          { r: 14, h: 40 },
          { r: 20, h: 48 },
          { r: 28, h: 56 },
        ].map((s, i) => (
          <div key={i} style={{
            height: s.h, width: s.h, borderRadius: s.r,
            background: 'var(--surface)',
            boxShadow: 'var(--shadow-sm)',
            border: '1px solid var(--cream-200)',
            display: 'flex', alignItems: 'flex-end', justifyContent: 'flex-start',
            padding: 4, fontSize: 9, color: 'var(--ink-400)',
            fontFamily: 'var(--font-mono)',
          }}>{s.r}</div>
        ))}
      </div>

      {/* Buttons */}
      <SectionTitle top={20}>Buttons</SectionTitle>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <button className="pw-btn" style={{ height: 40, fontSize: 13 }}>Primary</button>
        <button className="pw-btn pw-btn--ghost" style={{ height: 40, fontSize: 13 }}>Ghost</button>
        <button className="pw-btn" disabled style={{ height: 40, fontSize: 13 }}>Disabled</button>
      </div>
    </div>
  );
}

function SectionTitle({ children, top = 0 }) {
  return (
    <div style={{
      marginTop: top, marginBottom: 8,
      fontSize: 11, textTransform: 'uppercase', letterSpacing: '0.08em',
      color: 'var(--ink-700)', fontWeight: 500,
    }}>{children}</div>
  );
}

function TypeRow({ label, children }) {
  return (
    <div style={{
      padding: '10px 12px',
      background: 'var(--surface)',
      borderRadius: 10,
      border: '1px solid var(--cream-200)',
    }}>
      <div style={{
        fontSize: 9, color: 'var(--ink-400)',
        fontFamily: 'var(--font-mono)', marginBottom: 4,
        textTransform: 'uppercase', letterSpacing: '0.05em',
      }}>{label}</div>
      <div>{children}</div>
    </div>
  );
}

// Notes shown on the right rail for each screen.
const NOTES = {
  0: {
    title: 'Screen 1 · Postcode',
    notes: [
      ['Social proof early', 'Neighbor count resolves within ~600ms of a valid postcode. Turns abstract onboarding into a local, specific moment.'],
      ['Progressive disclosure', 'Community ID is collapsed by default — only ~3% of users need it; surfacing it to everyone adds friction.'],
      ['Trust at the CTA', 'Read-only access footnote under the primary button, in ink-400 at 12px. Never cluttered, always present.'],
    ],
  },
  1: {
    title: 'Screen 2 · Connected',
    notes: [
      ['Earned calm', 'Auto-advances at 2.4s — long enough to register success, short enough to not frustrate.'],
      ['Motion means progress', 'Pulsing rings expand outward, reinforcing "signal dispatched" metaphor without being decorative.'],
      ['Provider badge is neutral', "We don't co-brand the confirmation — Ampeer stays the trusted actor."],
    ],
  },
  2: {
    title: 'Screen 3 · Savings',
    notes: [
      ['The payoff moment', 'Serif numerals at 88px. This is the first time the user sees real value, and the type acknowledges it.'],
      ['Honest range', '£180–£340 is wide on purpose. Point estimates feel like claims; ranges feel like evidence.'],
      ['Comparison bar', 'Standard tariff sits in neutral cream-200; Ampeer fills forest-700. Difference is marked with –£260, not ×%.'],
    ],
  },
  3: {
    title: 'Screen 4 · Profile',
    notes: [
      ['Card = "us working for you"', "Header uses forest gradient — the only heavy color moment, reserved for the user's persona."],
      ['Inline "why"', 'Each row has a tap-to-explain tooltip. Beats a separate FAQ page for trust.'],
      ['Edit is cheap', 'Pencil → inline field → blur to save. No modals; no "are you sure?"'],
    ],
  },
  4: {
    title: 'Screen 5 · Legal',
    notes: [
      ['Reassurance first', 'The savings rationale is pinned above the list — the single most important thing to land.'],
      ['Four scannable lines', 'T&Cs, data, erasure, portability. Each with a one-sentence plain-English summary.'],
      ['One checkbox', 'Combines the agreement into one consent. Legal "atomize everything" patterns punish readers.'],
    ],
  },
  5: {
    title: 'Screen 6 · All set',
    notes: [
      ["Show what's next", 'Three concrete events with times — "Tonight, Tomorrow 7am, Sunday." Specificity builds trust.'],
      ['Motion as activation', "Flowing bolt dots between houses signal 'it's running.' Once the user is on the dashboard, this visual stays quiet."],
      ['Energized you-node', 'Your house is forest-700 and elevated; neighbors are surface-level. Hierarchy without labels.'],
    ],
  },
  loading: {
    title: 'Loading state',
    notes: [
      ['Concrete progress', "Three labeled steps, not a generic spinner. Users understand what's happening even if they can't affect it."],
      ['Bolt at the center', 'A single forest icon inside the ring — keeps brand voice even in utility moments.'],
      ['No cancel button', 'Would invite retry spirals. The error state handles real failures explicitly.'],
    ],
  },
  error: {
    title: 'Error state',
    notes: [
      ['Blame never lands on the user', '"We couldn\'t reach your meter" — actor is Ampeer, not the user.'],
      ['Technical detail, contained', 'Error code is present for support but visually quiet (ink-400 mono).'],
      ['Always a second path', 'Manual setup is offered below the retry CTA. No dead-end screens.'],
    ],
  },
};

function NotesRail({ step }) {
  const data = NOTES[step] || NOTES[0];
  return (
    <div style={{ fontFamily: 'var(--font-sans)' }}>
      <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 14 }}>
        Design notes
      </div>
      <div className="t-display" style={{
        fontSize: 20, color: 'var(--ink-900)', lineHeight: 1.1, marginBottom: 16,
      }}>{data.title}</div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        {data.notes.map(([t, body], i) => (
          <div key={i} style={{
            padding: '12px 14px',
            background: 'var(--surface)',
            border: '1px solid var(--cream-200)',
            borderRadius: 12,
          }}>
            <div style={{ fontSize: 12, fontWeight: 500, color: 'var(--forest-700)', marginBottom: 4 }}>
              {t}
            </div>
            <div style={{ fontSize: 12, color: 'var(--ink-600)', lineHeight: 1.55 }}>
              {body}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Tweaks panel
function TweaksPanel({ tweaks, onChange }) {
  const accents = ['forest', 'emerald', 'teal', 'copper'];
  return (
    <div style={{
      position: 'fixed', bottom: 20, right: 20, zIndex: 1000,
      width: 280, padding: 18, borderRadius: 18,
      background: 'rgba(26,26,23,0.96)',
      color: '#F2EFE7',
      backdropFilter: 'blur(14px)',
      boxShadow: '0 20px 50px rgba(0,0,0,0.35)',
      fontFamily: 'var(--font-sans)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14 }}>
        <span style={{ fontSize: 14, fontWeight: 500 }}>Tweaks</span>
        <span style={{ fontSize: 10, color: 'rgba(255,255,255,0.45)', fontFamily: 'var(--font-mono)', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Live</span>
      </div>

      {/* Accent hue */}
      <TLabel>Accent hue</TLabel>
      <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
        {accents.map(h => {
          const c = ACCENT_MAP[h];
          const active = tweaks.accentHue === h;
          return (
            <button key={h} onClick={() => onChange('accentHue', h)} style={{
              flex: 1, appearance: 'none', border: 0, cursor: 'pointer',
              padding: '6px 0 4px', borderRadius: 10,
              background: active ? 'rgba(255,255,255,0.1)' : 'transparent',
              display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4,
            }}>
              <div style={{
                width: 20, height: 20, borderRadius: 6,
                background: c[700],
                boxShadow: active ? `0 0 0 2px #fff` : `0 0 0 1px rgba(255,255,255,0.15)`,
              }}/>
              <span style={{ fontSize: 10, color: active ? '#fff' : 'rgba(255,255,255,0.5)' }}>{h}</span>
            </button>
          );
        })}
      </div>

      {/* Provider name */}
      <TLabel>Provider name (screen 2)</TLabel>
      <input
        value={tweaks.providerName}
        onChange={e => onChange('providerName', e.target.value)}
        style={{
          width: '100%', padding: '8px 10px', borderRadius: 8,
          background: 'rgba(255,255,255,0.08)',
          border: '1px solid rgba(255,255,255,0.12)',
          color: '#fff', fontSize: 13, fontFamily: 'var(--font-sans)',
          outline: 0, boxSizing: 'border-box', marginBottom: 14,
        }}/>

      {/* State toggles */}
      <TLabel>Overlay state</TLabel>
      <div style={{ display: 'flex', gap: 6, marginBottom: 10 }}>
        <TogBtn active={!tweaks.showLoadingState && !tweaks.showErrorState}
                onClick={() => { onChange('showLoadingState', false); onChange('showErrorState', false); }}>
          Off
        </TogBtn>
        <TogBtn active={tweaks.showLoadingState}
                onClick={() => { onChange('showLoadingState', true); onChange('showErrorState', false); }}>
          Loading
        </TogBtn>
        <TogBtn active={tweaks.showErrorState}
                onClick={() => { onChange('showErrorState', true); onChange('showLoadingState', false); }}>
          Error
        </TogBtn>
      </div>
      <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.45)', lineHeight: 1.4 }}>
        Overrides the current screen with a shared state. Useful for reviewing the pattern.
      </div>
    </div>
  );
}

function TLabel({ children }) {
  return (
    <div style={{
      fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.08em',
      color: 'rgba(255,255,255,0.5)', marginBottom: 8,
    }}>{children}</div>
  );
}
function TogBtn({ children, active, onClick }) {
  return (
    <button onClick={onClick} style={{
      flex: 1, appearance: 'none', border: 0, cursor: 'pointer',
      padding: '7px 0', borderRadius: 8,
      background: active ? '#F2EFE7' : 'rgba(255,255,255,0.06)',
      color: active ? '#1A1A17' : 'rgba(255,255,255,0.7)',
      fontSize: 11, fontFamily: 'var(--font-sans)', fontWeight: 500,
    }}>{children}</button>
  );
}

Object.assign(window, { SystemRail, NotesRail, TweaksPanel });
