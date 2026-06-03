// Onboarding step 1: welcome the user and confirm their postcode.
function Screen1_Postcode({ state, setState, onNext }) {
  const [postcode] = React.useState(state.postcode || 'SW6 3JD');
  const [communityOpen, setCommunityOpen] = React.useState(!!state.communityId);
  const [communityId, setCommunityId] = React.useState(state.communityId || '');
  const [peerInfoOpen, setPeerInfoOpen] = React.useState(false);

  // Example area info to show on screen.
  const areaData = { area: 'London', neighbors: 47 };

  const handleNext = () => {
    setState(s => ({ ...s, postcode: postcode.toUpperCase(), communityId, area: areaData.area }));
    onNext();
  };

  return (
    <PwScreen step={0}>
      <div style={{ marginBottom: 28 }}/>
      <PwPageTitle
        eyebrow=""
        title="Welcome to Ampeer."
        subtitle="You have been securely redirected from your Octopus online account. You are a few steps away from sharing energy with your neighbours."
        size={34}
      />

      {/* Card showing the user's area and nearby neighbours */}
      <div style={{ marginTop: 36 }}>
          <div style={{
            background: 'var(--lime-50)',
            border: '1px solid var(--lime-100)',
            borderRadius: 'var(--r-md)',
            padding: '14px 16px',
            display: 'flex', alignItems: 'center', gap: 12,
          }}>
            <div style={{ display: 'flex' }}>
              {['#2F6B50', '#7FA291', '#1F4A37', '#B8713A'].map((c, i) => (
                <div key={i} style={{
                  width: 24, height: 24, borderRadius: 999, background: c,
                  border: '2px solid var(--lime-50)',
                  marginLeft: i === 0 ? 0 : -8,
                  boxShadow: '0 1px 2px rgba(0,0,0,0.06)',
                }}/>
              ))}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div className="t-body" style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 500 }}>
                You're in <span style={{ fontWeight: 600 }}>{areaData.area}</span>
              </div>
              <div className="t-body" style={{ fontSize: 13, color: 'var(--ink-600)', marginTop: 1 }}>
                <span className="t-num" style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 600 }}>{areaData.neighbors}</span> neighbours are trading nearby
              </div>
            </div>
          </div>
      </div>

      {/* Tap to learn what peer trading is */}
      <div style={{ marginTop: 30 }}>
        <button onClick={() => setPeerInfoOpen(o => !o)} style={{
          appearance: 'none', background: 'transparent', border: 0, padding: 0,
          display: 'inline-flex', alignItems: 'center', gap: 6,
          color: 'var(--ink-700)', fontSize: 14, fontFamily: 'var(--font-sans)',
          fontWeight: 500, cursor: 'pointer',
          paddingBottom: 2,
        }}>
          <span>What is peer trading?</span>
          <div style={{
            transform: `rotate(${peerInfoOpen ? 90 : 0}deg)`,
            transition: 'transform .22s ease',
          }}>
            <IconChevron size={14}/>
          </div>
        </button>
        {peerInfoOpen && (
          <div className="pw-fade-in" style={{
            marginTop: 12, padding: '14px 16px',
            background: 'var(--surface)',
            border: '1px solid var(--cream-200)',
            borderRadius: 'var(--r-md)',
            fontSize: 13, lineHeight: 1.55, color: 'var(--ink-600)',
          }}>
            Peer trading allows you to <span style={{ fontWeight: 600, color: 'var(--ink-900)' }}>buy and sell electricity with your neighbors at cheaper rates</span>. It lowers your bills while creating a cleaner, more resilient energy grid. Ampeer automates this process, finding the best trades that match your household's needs.
          </div>
        )}
      </div>

      {/* Optional community ID field */}
      <div style={{ marginTop: 16 }}>
        <button onClick={() => setCommunityOpen(o => !o)} style={{
          appearance: 'none', background: 'transparent', border: 0, padding: 0,
          display: 'flex', alignItems: 'center', gap: 6,
          color: 'var(--ink-700)', fontSize: 14, fontFamily: 'var(--font-sans)', fontWeight: 500,
          cursor: 'pointer',
        }}>
          <span>Joining a specific community?</span>
          <div style={{
            transform: `rotate(${communityOpen ? 90 : 0}deg)`,
            transition: 'transform .22s ease',
          }}>
            <IconChevron size={14}/>
          </div>
        </button>
        {communityOpen && (
          <div className="pw-fade-in" style={{ marginTop: 12 }}>
            <input
              className="pw-input"
              value={communityId}
              onChange={e => setCommunityId(e.target.value)}
              placeholder="Community ID (optional)"
              style={{ height: 48, fontSize: 15 }}
            />
          </div>
        )}
      </div>

      {/* Main button to continue */}
      <div style={{ marginTop: 40 }}>
        <PwButton onClick={handleNext} icon={<IconArrowRight size={16}/>}>
          Connect your smart meter
        </PwButton>
        <p style={{
          margin: '14px 4px 0', fontSize: 12, lineHeight: 1.5,
          color: 'var(--ink-400)', display: 'flex', alignItems: 'center', gap: 6,
        }}>
          {/*<IconLock size={12}/>
          We use read-only access via your approved provider.*/}
        </p>
      </div>
    </PwScreen>
  );
}

Object.assign(window, { Screen1_Postcode });
