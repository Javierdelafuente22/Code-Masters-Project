// The 5-tab app shown in a phone frame, with side rails for notes.
const APP_TWEAKS = /*EDITMODE-BEGIN*/{
  "startTab": "dashboard",
  "houseMode": "sunny",
  "showLiveFlows": true
}/*EDITMODE-END*/;

function PeerwayMainApp() {
  const [tweaks, setTweaks] = React.useState(APP_TWEAKS);
  const [editMode, setEditMode] = React.useState(false);
  const [tab, setTab] = React.useState(() => {
    return localStorage.getItem('pw_main_tab') || tweaks.startTab || 'dashboard';
  });

  React.useEffect(() => localStorage.setItem('pw_main_tab', tab), [tab]);

  // Listen for the edit-mode on/off messages from the parent page.
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

  const renderTab = () => {
    switch (tab) {
      case 'home':      return <HouseTab/>;
      case 'community': return <CommunityTab/>;
      case 'dashboard': return <DashboardTab/>;
      case 'assistant': return <AssistantTab/>;
      case 'profile':   return <ProfileTab/>;
      default:          return <DashboardTab/>;
    }
  };

  return (
    <div
      data-screen-label={tab[0].toUpperCase() + tab.slice(1)}
      style={{
        minHeight: '100vh', background: '#E8E3D6',
        display: 'flex', alignItems: 'flex-start', justifyContent: 'center',
        padding: '40px 24px 80px', gap: 40, boxSizing: 'border-box',
      }}>
      {/* Left rail — tab index */}
      <div className="pw-rail" style={{
        width: 260, flexShrink: 0, paddingTop: 20,
      }}>
        <AppLeftRail active={tab}/>
      </div>

      {/* Phone */}
      <div style={{ flexShrink: 0, position: 'sticky', top: 40 }}>
        <IOSDevice width={390} height={844}>
          <div key={tab} style={{ height: '100%', position: 'relative' }} className="pw-fade-in">
            {renderTab()}
            <TabBar active={tab} onChange={setTab}/>
          </div>
        </IOSDevice>

        <div style={{
          marginTop: 16, textAlign: 'center',
          fontSize: 11, color: 'rgba(26,26,23,0.55)',
          fontFamily: 'var(--font-sans)', letterSpacing: '0.05em',
        }}>
          MAIN APP · {tab.toUpperCase()} TAB
        </div>
        <div style={{
          marginTop: 6, textAlign: 'center',
          fontSize: 12, color: 'rgba(26,26,23,0.5)',
        }}>
          <a href="Peerway Onboarding.html" style={{
            color: 'var(--ink-700)', textDecoration: 'none',
            borderBottom: '1px dotted rgba(26,26,23,0.3)',
          }}>
            ← Back to onboarding
          </a>
        </div>
      </div>

      {/* Right rail — design notes */}
      <div className="pw-rail" style={{
        width: 280, flexShrink: 0, paddingTop: 20,
      }}>
        <AppNotesRail tab={tab}/>
      </div>

      {/* Tweaks panel */}
      {editMode && (
        <AppTweaksPanel tweaks={tweaks} onChange={updateTweak} tab={tab} setTab={setTab}/>
      )}

      <style>{`
        @media (max-width: 1100px) { .pw-rail { display: none; } }
      `}</style>
    </div>
  );
}

// Left rail: list of the five tabs.
function AppLeftRail({ active }) {
  const tabs = [
    { id: 'home',      label: 'Home',      sub: 'Live energy flow' },
    { id: 'community', label: 'Community', sub: 'Anonymized trading map' },
    { id: 'dashboard', label: 'Dashboard', sub: 'Savings check-in · default' },
    { id: 'assistant', label: 'Assistant', sub: 'AI trading agent' },
    { id: 'profile',   label: 'Profile',   sub: 'Account & data' },
  ];
  return (
    <div style={{
      background: '#fff', borderRadius: 'var(--r-md)',
      padding: 18, boxShadow: 'var(--shadow-sm)',
    }}>
      <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 14 }}>
        Five tabs
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
        {tabs.map(t => (
          <div key={t.id} style={{
            padding: '10px 12px', borderRadius: 8,
            background: active === t.id ? 'var(--lime-50)' : 'transparent',
            display: 'flex', alignItems: 'flex-start', gap: 10,
          }}>
            <div style={{
              width: 20, height: 20, borderRadius: 6,
              background: active === t.id ? 'var(--lime-500)' : 'var(--cream-100)',
              color: active === t.id ? 'var(--ink-900)' : 'var(--ink-400)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              flexShrink: 0, marginTop: 1,
              fontSize: 10, fontFamily: 'var(--font-sans)', fontWeight: 600,
            }}>
              {tabs.findIndex(x => x.id === t.id) + 1}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{
                fontSize: 13, fontWeight: active === t.id ? 600 : 500,
                color: 'var(--ink-900)', letterSpacing: '-0.005em',
              }}>
                {t.label}
              </div>
              <div style={{ fontSize: 11, color: 'var(--ink-600)', marginTop: 1 }}>
                {t.sub}
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Right rail: short design notes that change with the current tab.
function AppNotesRail({ tab }) {
  const notes = {
    dashboard: {
      title: 'Dashboard — the habit',
      items: [
        ['Default landing', 'The user opens the app to ask "did I save money?" — anything that delays that answer is friction.'],
        ['Hero + context', 'Big number alone is meaningless. The "23% better" badge converts a number into a feeling.'],
        ['Specific insights', 'Local, human, anti-dashboard copy. "Kettles at St. Mary\'s" beats "7.2 kWh sent."'],
        ['Trust footer', 'Never-worse-off reassurance, always present but quiet. Persistent without being loud.'],
      ],
    },
    home: {
      title: 'Home — living right now',
      items: [
        ['Always captioned', 'No raw kW number appears without plain-English translation. That\'s the contract.'],
        ['Sunny + night states', 'Day = warm cream + glowing panels + outflow. Night = deep forest + inflow from battery.'],
        ['Animated particles', 'Real energy moves along real paths. Feels like looking at the system, not a chart.'],
        ['Day bars', 'Positive = sold, negative = drawn. Current hour is solid, rest is faded.'],
      ],
    },
    community: {
      title: 'Community — your community',
      items: [
        ['Anonymized map', '10 peers, stylized positions, no names. Emoji hints at type (home/shop/school).'],
        ['Collective framing', 'Individual wins matter, but "340 kWh = train trip to Edinburgh" is what shares.'],
        ['Live trades ticker', 'Top-right pill keeps the map feeling real-time without demanding attention.'],
        ['Invite = growth loop', 'Bottom CTA mirrors Monzo/Wise referral. £10 to each side is the right asymmetry.'],
      ],
    },
    assistant: {
      title: 'Assistant — special feature',
      items: [
        ['Mission check pattern', 'AI returns the FULL plan, not a yes. Every action is explicit before confirmation.'],
        ['Voice > text', 'Mic is the primary affordance. Text falls back when voice isn\'t appropriate.'],
        ['Prompt chips', 'Four realistic intents above input reduce cold-start paralysis.'],
        ['Intelligence = new screen', 'Connecting calendar/email is consequential. Dedicated page, not a modal.'],
        ['"Won\'t do" list', 'Privacy statement is louder than permissions. On dark ink card for emphasis.'],
      ],
    },
    profile: {
      title: 'Profile — escape hatches',
      items: [
        ['Pause trading', 'Soft exit. Most users who want to leave actually want to pause.'],
        ['GDPR as a section', 'Rights are features, not buried. Export, rectify, withdraw as first-class rows.'],
        ['Two-click delete', 'Red confirm requires a second tap. No typed-phrase theater, but also not one-tap.'],
        ['Meta footer', 'Version + build lets support reproduce bugs. Mono, tiny, 400 color.'],
      ],
    },
  }[tab] || { title: '', items: [] };

  return (
    <div style={{
      background: '#fff', borderRadius: 'var(--r-md)',
      padding: 18, boxShadow: 'var(--shadow-sm)',
    }}>
      <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 4 }}>
        Design notes
      </div>
      <div style={{
        fontSize: 14, fontWeight: 600, color: 'var(--ink-900)',
        letterSpacing: '-0.01em', marginBottom: 14,
      }}>
        {notes.title}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        {notes.items.map(([k, v]) => (
          <div key={k}>
            <div style={{
              fontSize: 12, fontWeight: 600, color: 'var(--ink-900)',
              letterSpacing: '-0.005em', marginBottom: 3,
            }}>
              {k}
            </div>
            <div style={{
              fontSize: 12, color: 'var(--ink-600)', lineHeight: 1.5,
              textWrap: 'pretty',
            }}>
              {v}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Little panel for switching the start tab while in edit mode.
function AppTweaksPanel({ tweaks, onChange, tab, setTab }) {
  return (
    <div style={{
      position: 'fixed', bottom: 20, right: 20, zIndex: 100,
      width: 260,
      background: '#fff', borderRadius: 12,
      boxShadow: '0 20px 50px rgba(0,0,0,0.2)',
      padding: 16,
    }}>
      <div style={{
        fontSize: 13, fontWeight: 600, color: 'var(--ink-900)',
        marginBottom: 12, letterSpacing: '-0.005em',
      }}>
        Tweaks
      </div>

      <TweakSection label="Start tab">
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 4 }}>
          {['home','community','dashboard','assistant','profile'].map(t => (
            <button key={t} onClick={() => { setTab(t); onChange('startTab', t); }}
                    style={{
                      appearance: 'none', border: 0, cursor: 'pointer',
                      padding: '6px 4px', borderRadius: 6, fontSize: 10,
                      textTransform: 'capitalize',
                      background: tab === t ? 'var(--ink-900)' : 'var(--cream-100)',
                      color: tab === t ? '#fff' : 'var(--ink-700)',
                      fontFamily: 'var(--font-sans)', fontWeight: 500,
                    }}>
              {t}
            </button>
          ))}
        </div>
      </TweakSection>
    </div>
  );
}

function TweakSection({ label, children }) {
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{
        fontSize: 10, fontFamily: 'var(--font-sans)',
        color: 'var(--ink-400)', textTransform: 'uppercase',
        letterSpacing: '0.03em', marginBottom: 6,
      }}>
        {label}
      </div>
      {children}
    </div>
  );
}

Object.assign(window, { PeerwayMainApp });
