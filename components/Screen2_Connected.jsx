// Onboarding step 2: show that the smart meter is connected.
function Screen2_Connected({ onNext, onBack, provider = 'your energy provider' }) {
  const [ready, setReady] = React.useState(false);
  // Wait a moment before enabling the button so the animation can play.
  React.useEffect(() => {
    const t = setTimeout(() => setReady(true), 900);
    return () => clearTimeout(t);
  }, []);

  return (
    <PwScreen step={1} onBack={onBack}>
      <PwPageTitle
        eyebrow="Step 1 of 4 — Your smart meter"
        title="You're connected."
        subtitle={`Live readings from ${provider} are flowing into Ampeer. No action needed from you.`}
        size={34}
      />

      <div style={{
        marginTop: 12, padding: '32px 20px 28px',
        background: 'var(--surface)',
        borderRadius: 'var(--r-lg)',
        border: '1px solid var(--cream-200)',
        display: 'flex', flexDirection: 'column', alignItems: 'center',
      }}>
        {/* Animated checkmark with rings */}
        <div style={{ position: 'relative', width: 100, height: 100 }}>
          {[0, 0.4, 0.8].map((d, i) => (
            <div key={i} style={{
              position: 'absolute', inset: 0, borderRadius: 999,
              border: '1.5px solid var(--lime-500)',
              animation: `pwRing 2s ease-out ${d}s infinite`,
            }}/>
          ))}
          <div style={{
            position: 'absolute', inset: 0, borderRadius: 999,
            background: 'var(--ink-900)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            boxShadow: '0 10px 30px rgba(0,168,98,0.25)',
          }}>
            <svg width="46" height="46" viewBox="0 0 56 56">
              <path d="M14 29l10 10 18-22"
                    stroke="var(--lime-400)" strokeWidth="4" fill="none"
                    strokeLinecap="round" strokeLinejoin="round"
                    style={{
                      strokeDasharray: 60,
                      strokeDashoffset: 60,
                      animation: 'pwDash .55s ease-out .2s forwards',
                    }}/>
            </svg>
          </div>
        </div>

        {/* Meter ID badge */}
        <div style={{
          marginTop: 24, padding: '10px 14px',
          borderRadius: 'var(--r-pill)',
          background: 'var(--cream-50)',
          border: '1px solid var(--cream-200)',
          display: 'inline-flex', alignItems: 'center', gap: 10,
        }}>
          <div style={{
            width: 20, height: 20, borderRadius: 5,
            background: 'var(--lime-500)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            color: 'var(--ink-900)',
          }}>
            <IconBolt size={12}/>
          </div>
          <span style={{ fontSize: 12, color: 'var(--ink-700)', fontFamily: 'var(--font-sans)', fontWeight: 500 }}>
            MPAN ••••  1847
          </span>
        </div>

        <div style={{
          marginTop: 18, fontSize: 12, color: 'var(--ink-400)',
          display: 'flex', alignItems: 'center', gap: 6,
        }}>
          <div style={{
            width: 6, height: 6, borderRadius: 999, background: 'var(--lime-500)',
            animation: 'pwPulse 1.4s ease-in-out infinite',
          }}/>
          <span style={{ fontSize: 11, fontFamily: 'var(--font-sans)', fontWeight: 500 }}>Receiving data · Verified</span>
        </div>
      </div>

      <div style={{ marginTop: 24 }}>
        <PwButton onClick={onNext} disabled={!ready} icon={<IconArrowRight size={16}/>}>
          {ready ? 'See your savings' : 'Connecting…'}
        </PwButton>
      </div>
    </PwScreen>
  );
}

Object.assign(window, { Screen2_Connected });
