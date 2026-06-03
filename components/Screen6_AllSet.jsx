// Onboarding step 6: confirm setup is done and show what happens next.
function Screen6_AllSet({ onNext, onBack }) {
  return (
    <PwScreen step={5} onBack={onBack}>
      <PwPageTitle
        eyebrow="Setup complete"
        title="You're all set."
        subtitle="We'll start trading tomorrow. Your first weekly report lands Sunday."
        size={36}
      />

      {/* Projected numbers for the first month */}
      <div style={{ marginTop: 24 }}>
        <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 12, fontSize: 13 }}>
          Your first month, projected
        </div>
        <div style={{
          display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-lg)',
          overflow: 'hidden',
        }}>
          {[
            { num: '47',  unit: 'neighbors' },
            { num: '£42', unit: 'savings' },
            { num: '32%', unit: 'green mix' },
          ].map((s, i) => (
            <div key={i} style={{
              padding: '18px 12px 16px',
              textAlign: 'center',
              borderLeft: i === 0 ? 0 : '1px solid var(--cream-200)',
            }}>
              <div className="t-num" style={{
                fontSize: 28, color: 'var(--ink-900)', lineHeight: 1,
                letterSpacing: '-0.04em', fontWeight: 600,
              }}>{s.num}</div>
              <div className="t-label" style={{ color: 'var(--ink-500)', marginTop: 8, fontSize: 11 }}>
                {s.unit}
              </div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ marginTop: 28 }}>
        <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 12, fontSize: 13 }}>
          What happens next
        </div>
        <div style={{ padding: '0 4px' }}>
          {[
            { when: 'Tonight',           what: 'EV charges during your cheapest overnight window.',         icon: <IconCar size={18}/> },
            { when: 'Tomorrow, 7am',     what: 'We start routing solar surplus to your neighbors in Fulham.', icon: <IconSolar size={18}/> },
            { when: 'Sunday',            what: 'Your first weekly savings report.',                          icon: <IconDoc size={18}/> },
          ].map((r, i) => (
            <div key={i} style={{
              display: 'flex', gap: 14, padding: '14px 0',
              borderBottom: i < 2 ? '1px solid var(--cream-200)' : 0,
            }}>
              <div style={{
                width: 28, flexShrink: 0, display: 'flex',
                alignItems: 'flex-start', justifyContent: 'center', paddingTop: 2,
                color: 'var(--lime-600)',
              }}>
                {r.icon}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 2, fontSize: 11 }}>
                  {r.when}
                </div>
                <div style={{ fontSize: 14, color: 'var(--ink-900)', lineHeight: 1.45 }}>
                  {r.what}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div style={{ marginTop: 32 }}>
        <PwButton onClick={onNext} icon={<IconArrowRight size={16}/>}>
          Explore Ampeer
        </PwButton>
      </div>
    </PwScreen>
  );
}

Object.assign(window, { Screen6_AllSet });
