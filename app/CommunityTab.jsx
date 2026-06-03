// Community tab: shows a live map of nearby neighbours and how energy is traded between them.
function CommunityTab({ highlight, onClearHighlight, weatherState }) {
  const activeKind = weatherState?.activeKind || 'sunny';
  const [tick, setTick] = React.useState(0);
  const [glowing, setGlowing] = React.useState(false);
  const [inviteMethod, setInviteMethod] = React.useState(null); // how the invite was sent
  const [popup, setPopup] = React.useState(null); // the tapped node's pop-up, or null

  const handleInvite = async () => {
    const url = 'https://www.ampeerenergy.com';
    try {
      if (navigator.share) {
        await navigator.share({
          title: 'Join me on Ampeer',
          text: 'I\'ve been trading solar energy with my neighbours on Ampeer. Join and we both earn £10!',
          url,
        });
        setInviteMethod('share');
      } else {
        await navigator.clipboard.writeText(url);
        setInviteMethod('clipboard');
      }
    } catch (e) {
      // user cancelled the share, so do nothing
    }
  };

  React.useEffect(() => {
    let raf;
    const start = performance.now();
    const loop = () => {
      setTick((performance.now() - start) / 1000);
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, []);

  React.useEffect(() => {
    if (!highlight) return;
    onClearHighlight && onClearHighlight(); // clear the flag so the glow doesn't replay on revisit
    setGlowing(true);
    const t = setTimeout(() => setGlowing(false), 4000);
    return () => clearTimeout(t);
  }, [highlight]);

  return (
    <div className="pw-screen">
      <style>{`
        @keyframes pwMapGlow {
          0%   { box-shadow: var(--shadow-sm); }
          30%  { box-shadow: var(--shadow-sm), 0 0 0 3px rgba(0,192,111,0.6), 0 0 32px rgba(0,192,111,0.25); }
          40%  { box-shadow: var(--shadow-sm), 0 0 0 3px rgba(0,192,111,0.6), 0 0 32px rgba(0,192,111,0.25); }
          100% { box-shadow: var(--shadow-sm); }
        }
      `}</style>
      <TabHeader
        eyebrow="Community"
        title="Your peers" />

      <div style={{ padding: '0 0 120px' }}>
        <div className="t-label" style={{ color: 'var(--ink-500)', fontSize: 13, padding: '0 24px 14px' }}>
          Anonymous trades within 5km
        </div>
        {/* map of nearby members and their trades */}
        <div style={{
          margin: '0 16px',
          height: 300, position: 'relative',
          borderRadius: 'var(--r-lg)',
          background: activeKind === 'night'
            ? 'linear-gradient(180deg, #1F2940 0%, #0F1828 100%)'
            : activeKind === 'rainy'
              ? 'linear-gradient(180deg, #BFC9D2 0%, #95A4B0 100%)'
              : activeKind === 'cloudy'
                ? 'linear-gradient(180deg, #E8E4DA 0%, #DCE3DA 100%)'
                : 'linear-gradient(180deg, #EEF4E8 0%, #E4EEDC 100%)',
          overflow: 'hidden',
          boxShadow: 'var(--shadow-sm)',
          animation: glowing ? 'pwMapGlow 3.2s ease-in-out forwards' : 'none',
          transition: 'background 0.6s ease',
        }}>
          <CommunityMap tick={tick} kind={activeKind} onTap={setPopup} />

          {popup && (
            <NodeInsightPopup
              title={popup.title}
              insight={popup.insight}
              onClose={() => setPopup(null)}
            />
          )}

          {/* Legend */}
          <div style={{
            position: 'absolute', top: 14, left: 14,
            padding: '8px 10px',
            background: 'rgba(255,255,255,0.85)',
            backdropFilter: 'blur(6px)',
            WebkitBackdropFilter: 'blur(6px)',
            borderRadius: 10,
            display: 'flex', flexDirection: 'column', gap: 4,
            fontSize: 10, fontFamily: 'var(--font-sans)',
            color: 'var(--ink-700)', letterSpacing: '0.02em'
          }}>
            <LegendItem color="var(--lime-500)" label="YOU" />
            <LegendItem color="var(--ink-900)" label="PEER" />
            <LegendItem color="#6B7370" label="GRID" />
          </div>

          {/* live trades-per-minute badge */}
          <div style={{
            position: 'absolute', top: 14, right: 14,
            padding: '6px 10px', background: 'var(--ink-900)', color: '#fff',
            borderRadius: 999, display: 'flex', alignItems: 'center', gap: 6,
            fontSize: 11, fontFamily: 'var(--font-sans)', letterSpacing: '0.02em'
          }}>
            <span style={{
              width: 6, height: 6, borderRadius: 999, background: 'var(--lime-400)',
              animation: 'pwPulse 1.4s ease-in-out infinite'
            }} />
            <span>LIVE  ·  7 TRADES / MIN</span>
          </div>
        </div>

        {/* the community's total CO2 saved this month */}
        <div style={{ padding: '24px 24px 0' }}>
          <div style={{
            padding: 18,
            background: 'var(--ink-900)',
            color: '#fff',
            borderRadius: 'var(--r-lg)',
            position: 'relative', overflow: 'hidden'
          }}>
            <div className="t-label" style={{ color: 'rgba(255,255,255,0.5)', marginBottom: 8, fontSize: 13 }}>
              Collective impact this month
            </div>
            <div style={{
              display: 'flex', alignItems: 'baseline', gap: 6,
              marginBottom: 6
            }}>
              <span className="t-num" style={{
                fontSize: 36, lineHeight: 1, color: '#fff',
                letterSpacing: '-0.04em', fontWeight: 600
              }}>800</span>
              <span style={{ fontSize: 16, color: 'rgba(255,255,255,0.7)', fontWeight: 500 }}>kg CO2 offset</span>
            </div>
            <div style={{
              fontSize: 13, color: 'rgba(255,255,255,0.75)',
              lineHeight: 1.4, textWrap: 'pretty'
            }}>
              Equivalent to a <span style={{ color: 'var(--lime-400)', fontWeight: 600 }}>London → Edinburgh</span> train trip for everyone in the community.
            </div>

            {/* small decorative line graph */}
            <svg viewBox="0 0 120 30" width="120" height="30"
            style={{ position: 'absolute', right: 16, top: 45, opacity: 0.35 }}>
              <path d="M0,22 L15,18 L30,14 L45,18 L60,10 L75,14 L90,6 L105,9 L120,4"
              fill="none" stroke="var(--lime-400)" strokeWidth="1.5"
              strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
        </div>

        {/* who you've traded with this month */}
        <div style={{ padding: '20px 24px 0' }}>
          <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 12, fontSize: 13 }}>
            This month you've traded with
          </div>
          <div style={{
            display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10
          }}>
            <BreakdownTile n="3" label="Homes" type="home" />
            <BreakdownTile n="1" label="School" type="school" />
            <BreakdownTile n="2" label="Shops" type="shop" />
          </div>
        </div>

        {/* breakdown of the kinds of members in the community */}
        <div style={{ padding: '20px 24px 0' }}>
          <div style={{
            padding: 16,
            background: 'var(--surface)',
            border: '1px solid var(--cream-200)',
            borderRadius: 'var(--r-md)'
          }}>
            <div style={{
              display: 'flex', justifyContent: 'space-between',
              marginBottom: 12, alignItems: 'baseline'
            }}>
              <span className="t-label" style={{ color: 'var(--ink-500)', fontSize: 13 }}>
                Community composition
              </span>
              <span style={{ fontSize: 11, color: 'var(--ink-600)', fontFamily: 'var(--font-sans)' }}>
                47 members
              </span>
            </div>
            <div style={{ display: 'flex', height: 8, borderRadius: 4, overflow: 'hidden' }}>
              <div style={{ flex: 24, background: 'var(--lime-500)' }} />
              <div style={{ flex: 12, background: 'var(--forest-700)' }} />
              <div style={{ flex: 8, background: '#3a5a4a' }} />
              <div style={{ flex: 3, background: 'var(--cream-200)' }} />
            </div>
            <div style={{
              display: 'flex', justifyContent: 'space-between',
              fontSize: 11, color: 'var(--ink-600)', marginTop: 10,
              flexWrap: 'wrap', gap: 8
            }}>
              <Swatch color="var(--lime-500)" label="Prosumers 24" />
              <Swatch color="var(--forest-700)" label="Consumers 12" />
              <Swatch color="#3a5a4a" label="Shops 8" />
              <Swatch color="var(--cream-200)" label="School 3" />
            </div>
          </div>
        </div>

        {/* button to invite a neighbour */}
        <div style={{ padding: '24px 24px 0' }}>
          {inviteMethod ? (
            <div className="pw-fade-in" style={{
              width: '100%', minHeight: 56,
              display: 'flex', alignItems: 'center', gap: 14,
              padding: '14px 20px',
              background: inviteMethod === 'clipboard' ? 'var(--ink-900)' : 'var(--lime-500)',
              borderRadius: 'var(--r-md)',
              boxSizing: 'border-box',
            }}>
              <div style={{
                width: 32, height: 32, borderRadius: 999,
                background: inviteMethod === 'clipboard' ? 'rgba(0,192,111,0.20)' : 'rgba(0,0,0,0.15)',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0, color: inviteMethod === 'clipboard' ? 'var(--lime-400)' : 'var(--ink-900)',
              }}>
                <IconCheck size={14}/>
              </div>
              <div>
                {inviteMethod === 'clipboard' ? (
                  <>
                    <div style={{ fontSize: 14, fontWeight: 600, color: '#fff' }}>
                      Invite link copied to clipboard!
                    </div>
                    <div style={{ fontSize: 11, color: 'rgba(255,255,255,0.6)', fontWeight: 500, marginTop: 1 }}>
                      You'll receive £10 when they join
                    </div>
                  </>
                ) : (
                  <>
                    <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--ink-900)' }}>
                      Invite link ready to share — nice one!
                    </div>
                    <div style={{ fontSize: 11, color: 'rgba(0,0,0,0.5)', fontWeight: 500, marginTop: 1 }}>
                      £10 added to your account when they join
                    </div>
                  </>
                )}
              </div>
            </div>
          ) : (
            <button onClick={handleInvite} className="pw-btn pw-btn-primary" style={{
              width: '100%', height: 56,
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '0 20px'
            }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div style={{
                  width: 32, height: 32, borderRadius: 999,
                  background: 'rgba(0,192,111,0.20)',
                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                  color: 'var(--lime-400)'
                }}>
                  <IconPlus size={14} />
                </div>
                <div style={{ textAlign: 'left' }}>
                  <div style={{ fontSize: 14, fontWeight: 600 }}>Invite a neighbor</div>
                  <div style={{
                    fontSize: 11, color: 'rgba(255,255,255,0.6)', fontWeight: 500,
                    letterSpacing: '-0.005em', marginTop: 1
                  }}>
                    You both earn £10 when they join
                  </div>
                </div>
              </div>
              <IconChevron size={16} />
            </button>
          )}
        </div>
      </div>
    </div>);

}

function LegendItem({ color, label }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ width: 8, height: 8, borderRadius: 2, background: color }} />
      <span>{label}</span>
    </div>);

}

function BreakdownTile({ n, label, type }) {
  const icon = {
    home: (
      <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
        stroke="var(--ink-700)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
        style={{ display: 'block', margin: '0 auto 6px' }}>
        <path d="M5 9.77746V16.2C5 17.8802 5 18.7203 5.32698 19.362C5.6146 19.9265 6.07354 20.3854 6.63803 20.673C7.27976 21 8.11984 21 9.8 21H14.2C15.8802 21 16.7202 21 17.362 20.673C17.9265 20.3854 18.3854 19.9265 18.673 19.362C19 18.7203 19 17.8802 19 16.2V5.00002M21 12L15.5668 5.96399C14.3311 4.59122 13.7133 3.90484 12.9856 3.65144C12.3466 3.42888 11.651 3.42893 11.0119 3.65159C10.2843 3.90509 9.66661 4.59157 8.43114 5.96452L3 12M14 21V15H10V21"/>
      </svg>
    ),
    shop: (
      <svg width="24" height="24" viewBox="0 0 24 24" fill="none"
        stroke="var(--ink-700)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
        style={{ display: 'block', margin: '0 auto 6px' }}>
        <path d="M21 18L20.1703 11.7771C20.0391 10.7932 19.9735 10.3012 19.7392 9.93082C19.5327 9.60444 19.2362 9.34481 18.8854 9.1833C18.4873 9 17.991 9 16.9983 9H7.00165C6.00904 9 5.51274 9 5.11461 9.1833C4.76381 9.34481 4.46727 9.60444 4.26081 9.93082C4.0265 10.3012 3.96091 10.7932 3.82972 11.7771L3 18M21 18H3M21 18V19.4C21 19.9601 21 20.2401 20.891 20.454C20.7951 20.6422 20.6422 20.7951 20.454 20.891C20.2401 21 19.9601 21 19.4 21H4.6C4.03995 21 3.75992 21 3.54601 20.891C3.35785 20.7951 3.20487 20.6422 3.10899 20.454C3 20.2401 3 19.9601 3 19.4V18M7.5 12V12.01M10.5 12V12.01M9 15V15.01M12 15V15.01M15 15V15.01M13.5 12V12.01M16.5 12V12.01M9 9V6M5.8 6H12.2C12.48 6 12.62 6 12.727 5.9455C12.8211 5.89757 12.8976 5.82108 12.9455 5.727C13 5.62004 13 5.48003 13 5.2V3.8C13 3.51997 13 3.37996 12.9455 3.273C12.8976 3.17892 12.8211 3.10243 12.727 3.0545C12.62 3 12.48 3 12.2 3H5.8C5.51997 3 5.37996 3 5.273 3.0545C5.17892 3.10243 5.10243 3.17892 5.0545 3.273C5 3.37996 5 3.51997 5 3.8V5.2C5 5.48003 5 5.62004 5.0545 5.727C5.10243 5.82108 5.17892 5.89757 5.273 5.9455C5.37996 6 5.51997 6 5.8 6Z"/>
      </svg>
    ),
    school: (
      <svg width="24" height="24" viewBox="0 0 512 512" fill="var(--ink-700)"
        style={{ display: 'block', margin: '0 auto 6px' }}>
        <path d="M505.837,180.418L279.265,76.124c-7.349-3.385-15.177-5.093-23.265-5.093c-8.088,0-15.914,1.708-23.265,5.093L6.163,180.418C2.418,182.149,0,185.922,0,190.045s2.418,7.896,6.163,9.627l226.572,104.294c7.349,3.385,15.177,5.101,23.265,5.101c8.088,0,15.916-1.716,23.267-5.101l178.812-82.306v82.881c-7.096,0.8-12.63,6.84-12.63,14.138c0,6.359,4.208,11.864,10.206,13.618l-12.092,79.791h55.676l-12.09-79.791c5.996-1.754,10.204-7.259,10.204-13.618c0-7.298-5.534-13.338-12.63-14.138v-95.148l21.116-9.721c3.744-1.731,6.163-5.504,6.163-9.627S509.582,182.149,505.837,180.418z"/>
        <path d="M256,346.831c-11.246,0-22.143-2.391-32.386-7.104L112.793,288.71v101.638c0,22.314,67.426,50.621,143.207,50.621c75.782,0,143.209-28.308,143.209-50.621V288.71l-110.827,51.017C278.145,344.44,267.25,346.831,256,346.831z"/>
      </svg>
    ),
  }[type];

  return (
    <div style={{
      padding: 12,
      background: 'var(--surface)',
      border: '1px solid var(--cream-200)',
      borderRadius: 'var(--r-md)',
      textAlign: 'center'
    }}>
      {icon}
      <div className="t-num" style={{
        fontSize: 22, color: 'var(--ink-900)', fontWeight: 600,
        letterSpacing: '-0.03em', lineHeight: 1
      }}>{n}</div>
      <div style={{
        fontSize: 11, color: 'var(--ink-600)', marginTop: 4,
        letterSpacing: '-0.005em'
      }}>{label}</div>
    </div>);

}

function Swatch({ color, label }) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 5 }}>
      <span style={{ width: 8, height: 8, borderRadius: 2, background: color }} />
      {label}
    </span>);

}

// the ten neighbours shown on the map, with their position and type
const PEERS = [
{ x: 72, y: 92, type: 'shop' },
{ x: 130, y: 70, type: 'home' },
{ x: 200, y: 84, type: 'shop' },
{ x: 268, y: 106, type: 'home' },
{ x: 320, y: 156, type: 'school' },
{ x: 295, y: 246, type: 'home' },
{ x: 230, y: 270, type: 'shop' },
{ x: 150, y: 260, type: 'home' },
{ x: 80, y: 234, type: 'home' },
{ x: 48, y: 174, type: 'home' }];

const GRID_NODE = { x: 220, y: 185 };
const YOU_NODE = { x: 140, y: 160 };

// the short message shown when each neighbour is tapped (same order as PEERS above)
const PEER_INSIGHTS = [
  'Local shops often need steady power during the day.',
  'Homes nearby usually use extra solar energy in the evening.',
  'More shop owners are buying solar panels to power their operations.',
  'Shorter distances mean less energy is lost across the grid.',
  'Schools are a strong match for daytime solar generation.',
  'Your surplus is shared first with nearby homes before the grid.',
  'Local production helps keep more renewable energy in the network.',
  'Energy is routed automatically based on local demand and prices.',
  'Trading energy locally reduces pressure on the national grid.',
  'Homes with EVs often need extra energy at night.',
];
const TYPE_TITLE = { home: 'Home', shop: 'Shop', school: 'School' };
const GRID_INSIGHT = 'The grid balances supply and demand when local trading is not enough.';

function CommunityMap({ tick, kind, onTap }) {
  const youInsight = kind === 'night'
    ? "You're drawing from your battery overnight."
    : kind === 'rainy'
      ? "You're running on battery and grid power — nothing to share today."
      : kind === 'cloudy'
        ? "You're sharing a small surplus when your panels can spare it."
        : "You're exporting surplus solar to nearby neighbours.";
  // which energy lines to show depends on the weather
  const activeFlows = React.useMemo(() => {
    if (kind === 'night') {
      return [
        { from: GRID_NODE, to: PEERS[1], color: '#7a9cc2' },
        { from: GRID_NODE, to: PEERS[5], color: '#7a9cc2' },
        { from: GRID_NODE, to: PEERS[8], color: '#7a9cc2' },
      ];
    }
    if (kind === 'rainy') {
      return [
        { from: GRID_NODE, to: PEERS[1], color: '#5b8aa6' },
        { from: GRID_NODE, to: PEERS[5], color: '#5b8aa6' },
        { from: GRID_NODE, to: PEERS[8], color: '#5b8aa6' },
        { from: GRID_NODE, to: PEERS[3], color: '#5b8aa6' },
      ];
    }
    if (kind === 'cloudy') {
      return [
        { from: YOU_NODE,  to: PEERS[1], color: '#00A862' },
        { from: YOU_NODE,  to: PEERS[6], color: '#00A862' },
        { from: GRID_NODE, to: PEERS[4], color: '#8aa69b' },
        { from: GRID_NODE, to: PEERS[2], color: '#8aa69b' },
        { from: PEERS[3],  to: PEERS[5], color: '#6FCBA0' }, // a trade between two neighbours
      ];
    }
    return [
      { from: YOU_NODE,  to: PEERS[1], color: '#00C06F' },
      { from: YOU_NODE,  to: PEERS[6], color: '#00A862' },
      { from: YOU_NODE,  to: PEERS[8], color: '#00C06F' },
      // trades between close-by neighbours
      { from: PEERS[0],  to: PEERS[1], color: '#6FCBA0' },
      { from: PEERS[3],  to: PEERS[4], color: '#6FCBA0' },
      { from: PEERS[5],  to: PEERS[6], color: '#6FCBA0' },
    ];
  }, [kind]);

  return (
    <svg viewBox="0 0 360 295" style={{ width: '100%', height: '100%', display: 'block' }}>
      <defs>
        <radialGradient id="youGlow" cx="0.5" cy="0.5" r="0.5">
          <stop offset="0%" stopColor="#00C06F" stopOpacity="0.4" />
          <stop offset="100%" stopColor="#00C06F" stopOpacity="0" />
        </radialGradient>
        <pattern id="mapGrid" width="20" height="20" patternUnits="userSpaceOnUse">
          <path d="M 20 0 L 0 0 0 20" fill="none" stroke="rgba(0,168,98,0.08)" strokeWidth="0.5" />
        </pattern>
      </defs>

      {/* faint grid background */}
      <rect width="360" height="295" fill="url(#mapGrid)" />

      {/* roads */}
      <path d="M -20 180 Q 100 160 200 175 T 380 160"
      fill="none" stroke="rgba(0,168,98,0.10)" strokeWidth="18" strokeLinecap="round" />
      <path d="M 180 -20 Q 195 100 190 180 T 210 360"
      fill="none" stroke="rgba(0,168,98,0.10)" strokeWidth="14" strokeLinecap="round" />

      {/* shaded area marking the neighbourhood */}
      <path d="M 40 120 Q 60 60 180 70 Q 320 60 330 180 Q 340 280 180 290 Q 40 300 30 180 Z"
      fill="rgba(0,168,98,0.04)" stroke="rgba(0,168,98,0.15)"
      strokeWidth="1" strokeDasharray="3 5" />

      {/* faint lines linking every neighbour to the grid */}
      {PEERS.map((p, i) =>
      <line key={`c${i}`} x1={p.x} y1={p.y} x2={GRID_NODE.x} y2={GRID_NODE.y}
      stroke="rgba(14,42,31,0.08)" strokeWidth="0.8" />
      )}

      {/* the moving energy lines */}
      {activeFlows.map((f, i) =>
      <AnimatedFlow key={i} from={f.from} to={f.to} color={f.color} tick={tick} delay={i * 0.3} />
      )}

      {/* the grid in the middle */}
      <g transform={`translate(${GRID_NODE.x}, ${GRID_NODE.y})`}
        onClick={() => onTap({ title: 'Grid', insight: GRID_INSIGHT })}
        style={{ cursor: 'pointer' }}>
        <circle r="22" fill="#fff" stroke="#6B7370" strokeWidth="1.4" />
        <g transform="translate(-12, -12)" color="#6B7370">
          <IconGrid size={24} />
        </g>
      </g>

      {/* the neighbour icons */}
      {PEERS.map((p, i) =>
      <PeerNode key={i} x={p.x} y={p.y} type={p.type} pulse={0.6 + 0.4 * Math.sin(tick * 2 + i)}
        onTap={() => onTap({ title: TYPE_TITLE[p.type], insight: PEER_INSIGHTS[i] })} />
      )}

      {/* the "you" marker in the centre */}
      <g transform={`translate(${YOU_NODE.x}, ${YOU_NODE.y})`}
        onClick={() => onTap({ title: 'You', insight: youInsight })}
        style={{ cursor: 'pointer' }}>
        <circle r={30 + Math.sin(tick * 2) * 2} fill="url(#youGlow)" />
        <circle r="19" fill="var(--lime-500)" stroke="#fff" strokeWidth="2" />
        <text x="0" y="4" textAnchor="middle" fontSize="10" fontFamily="Inter, Geist, sans-serif"
        fill="var(--ink-900)" fontWeight="700" letterSpacing="0.03em">
          YOU
        </text>
      </g>
    </svg>);

}

function PeerNode({ x, y, type, pulse, onTap }) {
  const icon = {
    home: (
      <svg x="-9" y="-9" width="18" height="18" viewBox="0 0 24 24" fill="none"
        stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M5 9.77746V16.2C5 17.8802 5 18.7203 5.32698 19.362C5.6146 19.9265 6.07354 20.3854 6.63803 20.673C7.27976 21 8.11984 21 9.8 21H14.2C15.8802 21 16.7202 21 17.362 20.673C17.9265 20.3854 18.3854 19.9265 18.673 19.362C19 18.7203 19 17.8802 19 16.2V5.00002M21 12L15.5668 5.96399C14.3311 4.59122 13.7133 3.90484 12.9856 3.65144C12.3466 3.42888 11.651 3.42893 11.0119 3.65159C10.2843 3.90509 9.66661 4.59157 8.43114 5.96452L3 12M14 21V15H10V21"/>
      </svg>
    ),
    shop: (
      <svg x="-9" y="-9" width="18" height="18" viewBox="0 0 24 24" fill="none"
        stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M21 18L20.1703 11.7771C20.0391 10.7932 19.9735 10.3012 19.7392 9.93082C19.5327 9.60444 19.2362 9.34481 18.8854 9.1833C18.4873 9 17.991 9 16.9983 9H7.00165C6.00904 9 5.51274 9 5.11461 9.1833C4.76381 9.34481 4.46727 9.60444 4.26081 9.93082C4.0265 10.3012 3.96091 10.7932 3.82972 11.7771L3 18M21 18H3M21 18V19.4C21 19.9601 21 20.2401 20.891 20.454C20.7951 20.6422 20.6422 20.7951 20.454 20.891C20.2401 21 19.9601 21 19.4 21H4.6C4.03995 21 3.75992 21 3.54601 20.891C3.35785 20.7951 3.20487 20.6422 3.10899 20.454C3 20.2401 3 19.9601 3 19.4V18M7.5 12V12.01M10.5 12V12.01M9 15V15.01M12 15V15.01M15 15V15.01M13.5 12V12.01M16.5 12V12.01M9 9V6M5.8 6H12.2C12.48 6 12.62 6 12.727 5.9455C12.8211 5.89757 12.8976 5.82108 12.9455 5.727C13 5.62004 13 5.48003 13 5.2V3.8C13 3.51997 13 3.37996 12.9455 3.273C12.8976 3.17892 12.8211 3.10243 12.727 3.0545C12.62 3 12.48 3 12.2 3H5.8C5.51997 3 5.37996 3 5.273 3.0545C5.17892 3.10243 5.10243 3.17892 5.0545 3.273C5 3.37996 5 3.51997 5 3.8V5.2C5 5.48003 5 5.62004 5.0545 5.727C5.10243 5.82108 5.17892 5.89757 5.273 5.9455C5.37996 6 5.51997 6 5.8 6Z"/>
      </svg>
    ),
    school: (
      <svg x="-9" y="-9" width="18" height="18" viewBox="0 0 512 512" fill="#fff">
        <path d="M505.837,180.418L279.265,76.124c-7.349-3.385-15.177-5.093-23.265-5.093c-8.088,0-15.914,1.708-23.265,5.093L6.163,180.418C2.418,182.149,0,185.922,0,190.045s2.418,7.896,6.163,9.627l226.572,104.294c7.349,3.385,15.177,5.101,23.265,5.101c8.088,0,15.916-1.716,23.267-5.101l178.812-82.306v82.881c-7.096,0.8-12.63,6.84-12.63,14.138c0,6.359,4.208,11.864,10.206,13.618l-12.092,79.791h55.676l-12.09-79.791c5.996-1.754,10.204-7.259,10.204-13.618c0-7.298-5.534-13.338-12.63-14.138v-95.148l21.116-9.721c3.744-1.731,6.163-5.504,6.163-9.627S509.582,182.149,505.837,180.418z"/>
        <path d="M256,346.831c-11.246,0-22.143-2.391-32.386-7.104L112.793,288.71v101.638c0,22.314,67.426,50.621,143.207,50.621c75.782,0,143.209-28.308,143.209-50.621V288.71l-110.827,51.017C278.145,344.44,267.25,346.831,256,346.831z"/>
      </svg>
    ),
  }[type];

  return (
    <g transform={`translate(${x}, ${y})`} onClick={onTap} style={{ cursor: 'pointer' }}>
      <circle r="16" fill="var(--ink-900)" opacity={0.9} />
      <circle r="16" fill="none" stroke="var(--lime-400)" strokeWidth="1" opacity={pulse * 0.5} />
      {icon}
    </g>);

}

function AnimatedFlow({ from, to, color, tick, delay = 0 }) {
  const ref = React.useRef();
  const [len, setLen] = React.useState(0);
  React.useEffect(() => {
    if (ref.current) setLen(ref.current.getTotalLength());
  }, [from, to]);

  // work out a gently curved line between the two points
  const mx = (from.x + to.x) / 2;
  const my = (from.y + to.y) / 2;
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const dist = Math.sqrt(dx * dx + dy * dy);
  const curve = dist * 0.2;
  const nx = -dy / dist * curve;
  const ny = dx / dist * curve;
  const d = `M ${from.x} ${from.y} Q ${mx + nx} ${my + ny} ${to.x} ${to.y}`;

  const pts = [];
  if (len) {
    for (let i = 0; i < 3; i++) {
      const p = ((tick + delay) * 0.22 + i / 3) % 1 * len;
      if (ref.current) {
        const pt = ref.current.getPointAtLength(p);
        pts.push({ x: pt.x, y: pt.y });
      }
    }
  }

  return (
    <g>
      <path ref={ref} d={d} fill="none" stroke={color} strokeOpacity="0.22" strokeWidth="1" strokeDasharray="2 3" />
      {pts.map((p, i) =>
      <circle key={i} cx={p.x} cy={p.y} r={2.5} fill={color}>
          <animate attributeName="opacity" values="0;1;0" dur="4.5s" repeatCount="indefinite" begin={`${i * 1.5}s`} />
        </circle>
      )}
    </g>);

}

function NodeInsightPopup({ title, insight, onClose }) {
  return (
    <div onClick={onClose} style={{
      position: 'absolute', inset: 0,
      background: 'rgba(20,28,24,0.32)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 5, borderRadius: 'var(--r-lg)',
      padding: 20,
    }}>
      <div onClick={(e) => e.stopPropagation()} style={{
        width: 240, padding: '14px 16px 14px',
        background: 'var(--surface)',
        borderRadius: 'var(--r-md)',
        border: '1px solid var(--cream-200)',
        boxShadow: '0 12px 36px rgba(0,0,0,0.22), 0 2px 8px rgba(0,0,0,0.08)',
        position: 'relative',
        fontFamily: 'var(--font-sans)',
      }}>
        <button onClick={onClose} aria-label="Close" style={{
          position: 'absolute', top: 6, right: 6,
          width: 26, height: 26, borderRadius: 999, border: 0,
          background: 'transparent', cursor: 'pointer',
          color: 'var(--ink-500)', fontSize: 18, lineHeight: 1,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          padding: 0,
        }}>×</button>
        <div style={{
          fontSize: 14, fontWeight: 600,
          color: 'var(--ink-900)', letterSpacing: '-0.01em',
          marginBottom: 10, paddingRight: 22,
        }}>{title}</div>
        <div style={{
          fontSize: 12, color: 'var(--ink-600)',
          lineHeight: 1.45,
          display: 'flex', alignItems: 'flex-start', gap: 6,
        }}>
          <span style={{ color: 'var(--lime-600)', marginTop: 1, flexShrink: 0, display: 'inline-flex' }}>
            <IconSparkle size={12}/>
          </span>
          <span>{insight}</span>
        </div>
      </div>
    </div>);

}

Object.assign(window, { CommunityTab });