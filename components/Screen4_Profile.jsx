// Onboarding step 4: show the user's details so they can confirm them.
function Screen4_Profile({ state, setState, onNext, onBack }) {
  const facts = state.profile || [
    { id: 'solar',  icon: <IconSolar size={16}/>,     label: 'Solar',           value: '8 panels · 3.2 kWp',       why: "Helps us know how much you can trade" },
    { id: 'ev',     icon: <IconCar size={16}/>,       label: 'EV',              value: 'Tesla Model 3',            why: "We schedule charging around your cheapest windows" },
    { id: 'work',   icon: <IconBriefcase size={16}/>, label: 'Work pattern',    value: 'Home 3 days / week',       why: "Your trading schedule adapts to when you're home" },
    { id: 'house',  icon: <IconPeople size={16}/>,    label: 'Household',       value: '2 adults',                 why: "More people at home means higher consumption" },
  ];

  return (
    <PwScreen step={3} onBack={onBack}>
      <PwPageTitle
        eyebrow="Step 3 of 4 — Profile"
        title="Your details"
        subtitle="Your details have been pre-filled with your Octopus account. Tap the info icon to see why we use it."
        size={32}
      />

      <div className="pw-card" style={{ marginTop: 24, overflow: 'visible' }}>
        {/* Name and postcode at the top of the card */}
        <div style={{
          background: 'var(--lime-50)',
          padding: '18px 18px 16px',
          color: 'var(--ink-900)',
          display: 'flex', alignItems: 'center', gap: 14,
          borderBottom: '1px solid var(--lime-100)',
        }}>
          <div style={{
            width: 44, height: 44, borderRadius: 999,
            background: 'var(--lime-500)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            color: 'var(--ink-900)', fontWeight: 600, fontSize: 16,
          }}>SC</div>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 15, fontWeight: 600, color: 'var(--ink-900)', letterSpacing: '-0.005em' }}>Sarah Chen</div>
            <div style={{ fontSize: 12, color: 'var(--ink-600)' }}>
              {state.postcode || 'SW6 3JD'} · {state.area || 'Fulham'}
            </div>
          </div>
          <div style={{
            padding: '4px 8px', borderRadius: 999,
            background: 'var(--ink-900)',
            color: 'var(--lime-400)',
            fontSize: 11, letterSpacing: '0.05em',
            textTransform: 'uppercase',
            fontWeight: 600,
          }}>
            Prosumer
          </div>
        </div>

        {/* List of household details */}
        <div>
          {facts.map((f, i) => (
            <div key={f.id} style={{
              display: 'flex', alignItems: 'center', gap: 12,
              padding: '14px 18px',
              borderTop: i === 0 ? 0 : '1px solid var(--cream-200)',
            }}>
              <div style={{
                width: 32, height: 32, borderRadius: 10,
                background: 'var(--lime-50)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                color: 'var(--ink-900)', flexShrink: 0,
              }}>{f.icon}</div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span className="t-label" style={{ color: 'var(--ink-500)' }}>{f.label}</span>
                  <PwTooltip label={f.why}/>
                </div>
                <div style={{
                  fontSize: 15, color: 'var(--ink-900)', marginTop: 2,
                  fontFamily: 'var(--font-sans)', letterSpacing: '-0.005em',
                }}>{f.value}</div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Note about how the data is used */}
      <div style={{
        marginTop: 16, padding: '12px 14px',
        display: 'flex', gap: 10, alignItems: 'flex-start',
        fontSize: 12, lineHeight: 1.5, color: 'var(--ink-600)',
      }}>
        <div style={{ marginTop: 1, color: 'var(--forest-500)' }}>
          <IconLock size={14}/>
        </div>
        <div>
          Data is only used to optimise trading.
        </div>
      </div>

      <div style={{ marginTop: 20 }}>
        <PwButton onClick={onNext} icon={<IconCheck size={16}/>}>
          Looks right
        </PwButton>
      </div>
    </PwScreen>
  );
}

Object.assign(window, { Screen4_Profile });
