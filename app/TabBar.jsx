// Bottom tab bar with the five app tabs.
const TABS = [
  { id: 'home',      label: 'Home',      Icon: null },
  { id: 'community', label: 'Community', Icon: null },
  { id: 'dashboard', label: 'Dashboard', Icon: null },
  { id: 'assistant', label: 'Assistant', Icon: null },
  { id: 'profile',   label: 'Profile',   Icon: null },
];

function TabBar({ active, onChange }) {
  const iconFor = (id, filled) => {
    const map = {
      home:      <IconTabHome size={22}      filled={filled}/>,
      community: <IconTabCommunity size={22} filled={filled}/>,
      dashboard: <IconTabDashboard size={22} filled={filled}/>,
      assistant: <IconTabAssistant size={22} filled={filled}/>,
      profile:   <IconTabProfile size={22}   filled={filled}/>,
    };
    return map[id];
  };

  return (
    <div style={{
      position: 'absolute', bottom: 0, left: 0, right: 0,
      height: 83,
      boxSizing: 'border-box',
      background: '#fff',
      borderTop: '1px solid var(--cream-200)',
      padding: '8px 8px 12px',
      display: 'flex', alignItems: 'center', justifyContent: 'space-around',
      boxShadow: '0 -4px 20px rgba(10,12,11,0.04)',
      zIndex: 10,
    }}>
      {TABS.map(t => {
        const isActive = active === t.id;
        return (
          <button
            key={t.id}
            onClick={() => onChange(t.id)}
            style={{
              appearance: 'none', border: 0, background: 'transparent',
              cursor: 'pointer', padding: '2px 4px',
              display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 2,
              width: 64,
              color: isActive ? 'var(--ink-900)' : 'var(--ink-400)',
              transition: 'color .16s ease',
              position: 'relative',
            }}
          >
            <div style={{
              display: 'flex', alignItems: 'center', justifyContent: 'center', height: 22,
            }}>
              {iconFor(t.id, isActive)}
            </div>
            <span style={{
              fontSize: 10, fontWeight: isActive ? 600 : 500,
              letterSpacing: '-0.005em',
              fontFamily: 'var(--font-sans)',
            }}>
              {t.label}
            </span>
            {isActive && (
              <div style={{
                position: 'absolute', top: -2, width: 4, height: 4, borderRadius: 999,
                background: 'var(--lime-500)',
              }}/>
            )}
          </button>
        );
      })}
    </div>
  );
}

// Shared page header used at the top of each tab.
function TabHeader({ eyebrow, title, right, subtitle }) {
  const isMobile =
    window.matchMedia('(max-width: 600px) and (pointer: coarse)').matches ||
    window.matchMedia('(display-mode: standalone)').matches ||
    window.navigator.standalone === true;

  return (
    <div style={{
      padding: isMobile ? '26px 24px 16px' : '66px 24px 16px',
      background: 'var(--cream-50)',
      position: 'sticky', top: 0, zIndex: 2,
    }}>
      <div style={{
        display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between',
        gap: 12, marginBottom: subtitle ? 4 : 0,
      }}>
        <div style={{ flex: 1, minWidth: 0 }}>

          <h1 className="t-title" style={{
            fontSize: 26, lineHeight: 1.08, margin: 0,
            color: 'var(--ink-900)', fontWeight: 600, letterSpacing: '-0.025em',
          }}>
            {title}
          </h1>
        </div>
        {right}
      </div>
      {subtitle && (
        <p style={{
          fontSize: 13, color: 'var(--ink-600)', margin: '6px 0 0',
          maxWidth: 320, lineHeight: 1.45,
        }}>{subtitle}</p>
      )}
    </div>
  );
}

Object.assign(window, { TabBar, TabHeader, TABS });
