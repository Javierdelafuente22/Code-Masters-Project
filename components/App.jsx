// Main app. Runs the 6-screen onboarding flow plus the loading and error states.

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "showLoadingState": false,
  "showErrorState": false,
  "accentHue": "forest",
  "providerName": "your energy provider",
  "startScreen": 0
}/*EDITMODE-END*/;

// Color sets for each accent option.
const ACCENT_MAP = {
  forest:   { 900:'#0E2A1F', 800:'#153826', 700:'#1F4A37', 600:'#27593F', 500:'#2F6B50', 300:'#7FA291', sage100:'#E8EFE9', sage50:'#F1F5F1' },
  emerald:  { 900:'#06281A', 800:'#0B3A26', 700:'#126B42', 600:'#18835A', 500:'#1FA06B', 300:'#7EC9A8', sage100:'#DFEFE6', sage50:'#EEF8F2' },
  teal:     { 900:'#0A2229', 800:'#0F3340', 700:'#1A5766', 600:'#236C7E', 500:'#2F8498', 300:'#80B3BF', sage100:'#DCEDF1', sage50:'#EDF6F8' },
  copper:   { 900:'#2B1409', 800:'#3A1D0F', 700:'#8A4422', 600:'#A0522B', 500:'#B86738', 300:'#D4A484', sage100:'#F3E4D7', sage50:'#FAF2EA' },
};

function applyAccent(hue) {
  const c = ACCENT_MAP[hue] || ACCENT_MAP.forest;
  const root = document.documentElement.style;
  root.setProperty('--forest-900', c[900]);
  root.setProperty('--forest-800', c[800]);
  root.setProperty('--forest-700', c[700]);
  root.setProperty('--forest-600', c[600]);
  root.setProperty('--forest-500', c[500]);
  root.setProperty('--forest-300', c[300]);
  root.setProperty('--sage-100', c.sage100);
  root.setProperty('--sage-50',  c.sage50);
}

function PeerwayApp() {
  const [tweaks, setTweaks] = React.useState(TWEAK_DEFAULTS);
  const [editMode, setEditMode] = React.useState(false);
  const [step, setStep] = React.useState(() => {
    const saved = Number(localStorage.getItem('pw_step'));
    return Number.isFinite(saved) ? saved : (tweaks.startScreen || 0);
  });
  const [state, setState] = React.useState({});
  const [internalMode, setInternalMode] = React.useState('flow'); // 'flow' | 'loading' | 'error' | 'done'

  React.useEffect(() => localStorage.setItem('pw_step', String(step)), [step]);
  React.useEffect(() => applyAccent(tweaks.accentHue), [tweaks.accentHue]);

  // Let the tweaks panel switch the loading/error overlays on.
  React.useEffect(() => {
    if (tweaks.showLoadingState) setInternalMode('loading');
    else if (tweaks.showErrorState) setInternalMode('error');
    else setInternalMode('flow');
  }, [tweaks.showLoadingState, tweaks.showErrorState]);

  // Talk to the host page so the Tweaks panel can be turned on.
  React.useEffect(() => {
    const onMsg = (e) => {
      const d = e.data || {};
      if (d.type === '__activate_edit_mode')   setEditMode(true);
      if (d.type === '__deactivate_edit_mode') setEditMode(false);
    };
    window.addEventListener('message', onMsg);
    window.parent.postMessage({ type: '__edit_mode_available' }, '*');
    return () => window.removeEventListener('message', onMsg);
  }, []);

  const updateTweak = (key, value) => {
    setTweaks(t => ({ ...t, [key]: value }));
    window.parent.postMessage({ type: '__edit_mode_set_keys', edits: { [key]: value } }, '*');
  };

  const goNext = () => setStep(s => Math.min(5, s + 1));
  const goBack = () => setStep(s => Math.max(0, s - 1));
  const restart = () => { setStep(0); setState({}); setInternalMode('flow'); };

  const screens = [
    <Screen1_Postcode key="s1" state={state} setState={setState} onNext={goNext}/>,
    <Screen2_Connected key="s2" provider={tweaks.providerName} onNext={goNext}/>,
    <Screen3_Savings   key="s3" state={state} onNext={goNext} onBack={goBack}/>,
    <Screen4_Profile   key="s4" state={state} setState={setState} onNext={goNext} onBack={goBack}/>,
    <Screen5_Legal     key="s5" onNext={goNext} onBack={goBack}/>,
    <Screen6_AllSet    key="s6" onNext={() => setInternalMode('done')} onBack={goBack}/>,
  ];

  // Pick what to show inside the phone frame.
  let content;
  if (internalMode === 'loading') {
    content = <LoadingState/>;
  } else if (internalMode === 'error') {
    content = <ErrorState onRetry={() => setInternalMode('flow')} onSkip={() => setInternalMode('flow')}/>;
  } else if (internalMode === 'done') {
    content = <DoneStub onRestart={restart}/>;
  } else {
    content = screens[step];
  }

  return (
    <div style={{
      minHeight: '100vh', background: '#E8E3D6',
      display: 'flex', alignItems: 'flex-start', justifyContent: 'center',
      padding: '40px 24px 80px', gap: 40, boxSizing: 'border-box',
    }}>
      {/* Left rail (design system, hidden on small screens) */}
      <div className="pw-rail" style={{
        width: 260, flexShrink: 0, paddingTop: 20,
      }}>
        <SystemRail/>
      </div>

      {/* Phone */}
      <div style={{ flexShrink: 0, position: 'sticky', top: 40 }}>
        <IOSDevice width={390} height={844}>
          <div key={`${step}-${internalMode}`} style={{ height: '100%' }}>
            {content}
          </div>
        </IOSDevice>

        {/* Bottom controls */}
        <div style={{
          marginTop: 16, display: 'flex', alignItems: 'center',
          justifyContent: 'center', gap: 8,
        }}>
          <RailBtn onClick={() => { setStep(s => Math.max(0, s - 1)); setInternalMode('flow'); }}>
            <IconChevron dir="left" size={14}/>
          </RailBtn>
          {Array.from({ length: 6 }).map((_, i) => (
            <button
              key={i}
              onClick={() => { setStep(i); setInternalMode('flow'); }}
              style={{
                appearance: 'none', border: 0, cursor: 'pointer',
                width: step === i && internalMode === 'flow' ? 24 : 8,
                height: 8, borderRadius: 4,
                background: step === i && internalMode === 'flow' ? 'var(--forest-700)' : 'rgba(26,26,23,0.16)',
                padding: 0, transition: 'all .24s ease',
              }}
              aria-label={`Screen ${i + 1}`}
            />
          ))}
          <RailBtn onClick={() => { setStep(s => Math.min(5, s + 1)); setInternalMode('flow'); }}>
            <IconChevron dir="right" size={14}/>
          </RailBtn>
          <div style={{ width: 16 }}/>
          <RailBtn onClick={() => setInternalMode(m => m === 'loading' ? 'flow' : 'loading')}
                   active={internalMode === 'loading'}>
            <span style={{ fontSize: 11, padding: '0 4px' }}>Loading</span>
          </RailBtn>
          <RailBtn onClick={() => setInternalMode(m => m === 'error' ? 'flow' : 'error')}
                   active={internalMode === 'error'}>
            <span style={{ fontSize: 11, padding: '0 4px' }}>Error</span>
          </RailBtn>
        </div>
        <div style={{
          marginTop: 12, textAlign: 'center',
          fontSize: 11, color: 'rgba(26,26,23,0.45)',
          fontFamily: 'var(--font-mono)',
        }}>
          {internalMode === 'flow'
            ? `Screen ${step + 1} of 6 — ${['Postcode','Connected','Savings','Profile','Legal','All set'][step]}`
            : internalMode.toUpperCase() + ' STATE'}
        </div>
      </div>

      {/* Right rail (design notes) */}
      <div className="pw-rail" style={{
        width: 280, flexShrink: 0, paddingTop: 20,
      }}>
        <NotesRail step={internalMode === 'flow' ? step : internalMode}/>
      </div>

      {/* Tweaks panel */}
      {editMode && (
        <TweaksPanel tweaks={tweaks} onChange={updateTweak}/>
      )}

      <style>{`
        @media (max-width: 1100px) { .pw-rail { display: none; } }
      `}</style>
    </div>
  );
}

function RailBtn({ children, onClick, active }) {
  return (
    <button onClick={onClick} style={{
      appearance: 'none', border: 0, cursor: 'pointer',
      height: 32, minWidth: 32, padding: '0 4px',
      borderRadius: 999,
      background: active ? 'var(--forest-700)' : 'rgba(26,26,23,0.06)',
      color: active ? '#fff' : 'var(--ink-700)',
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      fontFamily: 'var(--font-sans)', fontWeight: 500,
      transition: 'all .14s',
    }}>{children}</button>
  );
}

function DoneStub({ onRestart }) {
  return (
    <div style={{
      height: '100%', display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center', padding: 32,
      textAlign: 'center', background: 'var(--forest-900)', color: '#fff',
    }}>
      <div className="t-display" style={{ fontSize: 28, lineHeight: 1.1 }}>
        Dashboard is where<br/>the app begins.
      </div>
      <p style={{ fontSize: 14, color: 'rgba(255,255,255,0.6)', marginTop: 12, maxWidth: 260 }}>
        This prototype covers onboarding only. Tap below to run through it again.
      </p>
      <button onClick={onRestart} style={{
        marginTop: 24, appearance: 'none', border: 0, cursor: 'pointer',
        padding: '12px 20px', borderRadius: 999,
        background: 'rgba(255,255,255,0.14)', color: '#fff',
        fontSize: 14, fontFamily: 'var(--font-sans)',
      }}>
        Restart onboarding
      </button>
    </div>
  );
}

Object.assign(window, { PeerwayApp });
