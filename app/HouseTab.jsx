// Home tab: shows the live energy flow for your house and lets you tap each icon for details.
// The weather comes from the app shell so the same setting also drives the Community tab.
const WEEK_SAVED = 8.40; // also shown on the Dashboard
const PRICES = { importP: 20, exportP: 10, p2p: 15 }; // prices in pence per kWh
const BATTERY_SOC = 40; // battery charge, as a percentage

function sceneFor(kind) {
  if (kind === 'sunny') return {
    solarKw: 3.8, useKw: 1.2, thirdKw: 2.6, thirdLabel: 'Surplus', prodPct: 90,
    caption: 'You are generating more than you use. The extra is powering 2 neighbours.',
    community: 'surplus', batteryState: 'charging',
  };
  if (kind === 'cloudy') return {
    solarKw: 1.7, useKw: 1.2, thirdKw: 0.5, thirdLabel: 'Surplus', prodPct: 40,
    caption: 'Cloudy day — modest production. Small surplus is supporting 1 neighbour.',
    community: 'surplus', batteryState: 'charging',
  };
  if (kind === 'night') return {
    solarKw: 0, useKw: 0.6, thirdKw: 0.6, thirdLabel: 'Battery', prodPct: 0,
    caption: 'Night-time — the grid is supporting the community.',
    community: 'shortage', batteryState: 'discharging',
  };
  return {
    solarKw: 0, useKw: 1.2, thirdKw: 0.8, thirdLabel: 'Battery', prodPct: 0,
    caption: 'No solar today. The grid is supporting your community.',
    community: 'shortage', batteryState: 'discharging',
  };
}

const CANVAS_BG = {
  sunny:  'linear-gradient(180deg, #F5EFE0 0%, #E8F2E4 100%)',
  cloudy: 'linear-gradient(180deg, #E8E4DA 0%, #DCE3DA 100%)',
  rainy:  'linear-gradient(180deg, #BFC9D2 0%, #95A4B0 100%)',
  night:  'linear-gradient(180deg, #1F2940 0%, #0F1828 100%)',
};

function HouseTab({ onNavigate, highlight, onClearHighlight, weatherState }) {
  const { weather, override, setOverride, liveKind, activeKind, activeTemp,
          isLoading, hasError, isLive } = weatherState;
  const [tick, setTick] = React.useState(0);
  const [glowing, setGlowing] = React.useState(false);
  const [hovBanner, setHovBanner] = React.useState(false);
  const [hovSavings, setHovSavings] = React.useState(false);
  const [showOverride, setShowOverride] = React.useState(false);
  const [popup, setPopup] = React.useState(null); // which icon's pop-up is open, or null
  const [prices, setPrices] = React.useState(PRICES); // live prices if available, otherwise the fallback above

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
    onClearHighlight && onClearHighlight();
    setGlowing(true);
    const t = setTimeout(() => setGlowing(false), 4000);
    return () => clearTimeout(t);
  }, [highlight]);

  // Get the live London electricity prices from Octopus. If anything fails, we keep the fallback prices.
  React.useEffect(() => {
    let cancelled = false;
    const now = new Date();
    const periodFrom = new Date(now.getTime() - 2 * 60 * 60 * 1000).toISOString();
    const periodTo   = new Date(now.getTime() + 2 * 60 * 60 * 1000).toISOString();
    const qs = `?period_from=${encodeURIComponent(periodFrom)}&period_to=${encodeURIComponent(periodTo)}`;

    const pickCurrent = (data) => {
      const results = data?.results || [];
      const hit = results.find(r => {
        const f = new Date(r.valid_from).getTime();
        const t = new Date(r.valid_to).getTime();
        return f <= now.getTime() && now.getTime() < t;
      });
      return hit?.value_inc_vat;
    };

    const pickLatestActive = (products, predicate) => {
      const live = (products || []).filter(p =>
        p.code && predicate(p.code) && !p.available_to
      );
      live.sort((a, b) =>
        new Date(b.available_from || 0).getTime() - new Date(a.available_from || 0).getTime()
      );
      return live[0]?.code;
    };

    fetch('https://api.octopus.energy/v1/products/?brand=OCTOPUS_ENERGY&page_size=100')
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (cancelled) return null;
        const products = data?.results || [];
        const importCode = pickLatestActive(products, c => c.includes('AGILE') && !c.includes('OUTGOING'))
                         || 'AGILE-FLEX-22-11-25';
        const exportCode = pickLatestActive(products, c => c.includes('AGILE') && c.includes('OUTGOING'))
                         || 'AGILE-OUTGOING-19-05-13';
        console.log('[Octopus Agile] resolved products', { importCode, exportCode });
        const importUrl = `https://api.octopus.energy/v1/products/${importCode}/electricity-tariffs/E-1R-${importCode}-C/standard-unit-rates/${qs}`;
        const exportUrl = `https://api.octopus.energy/v1/products/${exportCode}/electricity-tariffs/E-1R-${exportCode}-C/standard-unit-rates/${qs}`;
        return Promise.all([
          fetch(importUrl).then(r => r.ok ? r.json() : null).catch(() => null),
          fetch(exportUrl).then(r => r.ok ? r.json() : null).catch(() => null),
        ]);
      })
      .then(pair => {
        if (cancelled || !pair) return;
        const [imp, exp] = pair;
        const ip = pickCurrent(imp);
        const ep = pickCurrent(exp);
        console.log('[Octopus Agile]', { import: ip, export: ep, importRaw: imp, exportRaw: exp });
        if (typeof ip === 'number' && typeof ep === 'number') {
          setPrices({ importP: ip, exportP: ep, p2p: (ip + ep) / 2 });
        }
      })
      .catch(err => { console.warn('[Octopus Agile] fetch failed', err); });
    return () => { cancelled = true; };
  }, []);

  const scene = sceneFor(activeKind);

  if (showOverride) {
    return (
      <OverridePanel
        override={override} onChange={setOverride}
        liveKind={liveKind} isLive={isLive} isLoading={isLoading} hasError={hasError}
        onBack={() => setShowOverride(false)}
      />
    );
  }

  return (
    <div className="pw-screen">
      <style>{`
        @keyframes pwHouseMapGlow {
          0%   { box-shadow: var(--shadow-sm); }
          30%  { box-shadow: var(--shadow-sm), 0 0 0 3px rgba(0,192,111,0.6), 0 0 32px rgba(0,192,111,0.25); }
          40%  { box-shadow: var(--shadow-sm), 0 0 0 3px rgba(0,192,111,0.6), 0 0 32px rgba(0,192,111,0.25); }
          100% { box-shadow: var(--shadow-sm); }
        }
        @keyframes pwLivePulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50%      { opacity: 0.5; transform: scale(1.15); }
        }
      `}</style>
      <TabHeader eyebrow="Home" title="Your home"/>

      <div style={{
        padding: '0 24px 14px',
        display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10,
      }}>
        <div className="t-label" style={{ color: 'var(--ink-500)', fontSize: 13 }}>
          Live energy flow
        </div>
        <WeatherPill
          isLoading={isLoading}
          override={override}
          onTap={() => setShowOverride(true)}
        />
      </div>

      <div style={{ padding: '0 0 90px' }}>
        {/* the energy-flow picture of the house */}
        <div style={{
          margin: '0 16px',
          height: 360, position: 'relative',
          borderRadius: 'var(--r-lg)',
          background: CANVAS_BG[activeKind],
          overflow: 'hidden',
          boxShadow: 'var(--shadow-sm)',
          animation: glowing ? 'pwHouseMapGlow 3.2s ease-in-out forwards' : 'none',
          transition: 'background 0.6s ease',
        }}>
          <HouseScene tick={tick} kind={activeKind} onTap={setPopup}/>

          {/* small hint telling the user the icons can be tapped */}
          <div style={{
            position: 'absolute', top: 12, right: 12,
            padding: '5px 10px 5px 8px',
            background: 'rgba(255,255,255,0.85)',
            backdropFilter: 'blur(6px)',
            WebkitBackdropFilter: 'blur(6px)',
            border: '1px solid var(--cream-200)',
            borderRadius: 999,
            display: 'inline-flex', alignItems: 'center', gap: 6,
            fontSize: 11, color: 'var(--ink-700)',
            fontFamily: 'var(--font-sans)', fontWeight: 500,
            letterSpacing: '-0.005em',
            pointerEvents: 'none',
            boxShadow: '0 1px 4px rgba(0,0,0,0.06)',
            zIndex: 2,
          }}>
            <span>Tap any icon for details</span>
          </div>

          {popup && (
            <NodePopup
              kind={popup}
              weatherKind={activeKind}
              temp={activeTemp}
              scene={scene}
              prices={prices}
              onClose={() => setPopup(null)}
              onNavigate={onNavigate}
            />
          )}
        </div>

        {/* banner that explains what's happening and links to the Community tab */}
        <div style={{ padding: '20px 24px 0' }}>
          <button onClick={() => onNavigate && onNavigate('community', true)}
            onMouseEnter={() => setHovBanner(true)} onMouseLeave={() => setHovBanner(false)}
            style={{
              appearance: 'none', border: '1px solid var(--cream-200)', cursor: 'pointer', width: '100%', textAlign: 'left',
              padding: '10px 14px',
              background: hovBanner ? 'var(--ink-900)' : 'var(--surface)',
              borderRadius: 'var(--r-md)',
              display: 'flex', alignItems: 'center', gap: 10,
              fontFamily: 'var(--font-sans)',
              transition: 'background .15s, color .15s',
            }}>
            <div style={{
              width: 28, height: 28, borderRadius: 8,
              background: hovBanner ? 'rgba(0,192,111,0.20)' : 'var(--lime-50)',
              color: hovBanner ? 'var(--lime-400)' : 'var(--lime-600)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              flexShrink: 0,
            }}>
              <IconBolt size={13}/>
            </div>
            <div style={{
              flex: 1,
              fontSize: 13, fontWeight: 500,
              color: hovBanner ? '#fff' : 'var(--ink-900)', letterSpacing: '-0.005em',
              textWrap: 'pretty',
            }}>
              {scene.caption}
            </div>
            <IconChevron size={13} style={{ color: hovBanner ? '#fff' : 'var(--ink-400)', flexShrink: 0 }}/>
          </button>
        </div>

        {/* the three live numbers: generating, using, and surplus/battery */}
        <div style={{ padding: '20px 24px 0' }}>
          <div className="t-label" style={{ color: 'var(--ink-500)', marginBottom: 10, fontSize: 13 }}>
            Live readouts
          </div>
          <div style={{
            display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10,
          }}>
            <Readout icon={<IconSolar size={14}/>}      label="Generating"      value={scene.solarKw.toFixed(1)} unit="kW" accent={scene.solarKw > 0}/>
            <Readout icon={<IconHome size={14}/>}       label="Using"           value={scene.useKw.toFixed(1)}   unit="kW"/>
            <Readout icon={<IconArrowRight size={14}/>} label={scene.thirdLabel} value={scene.thirdKw.toFixed(1)} unit="kW" accent/>
          </div>
        </div>

        {/* link to the Dashboard showing money saved this week */}
        <div style={{ padding: '12px 24px 0' }}>
          <button onClick={() => onNavigate && onNavigate('dashboard')}
            onMouseEnter={() => setHovSavings(true)} onMouseLeave={() => setHovSavings(false)}
            style={{
              appearance: 'none', border: '1px solid var(--cream-200)', cursor: 'pointer', width: '100%', textAlign: 'left',
              padding: '10px 14px',
              background: hovSavings ? 'var(--ink-900)' : 'var(--surface)',
              borderRadius: 'var(--r-md)',
              display: 'flex', alignItems: 'center', gap: 10,
              fontFamily: 'var(--font-sans)',
              transition: 'background .15s, color .15s',
            }}>
            <div style={{
              width: 28, height: 28, borderRadius: 8,
              background: hovSavings ? 'rgba(0,192,111,0.20)' : 'var(--lime-50)',
              color: hovSavings ? 'var(--lime-400)' : 'var(--lime-600)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              flexShrink: 0, fontSize: 13, fontWeight: 700,
              fontFamily: 'var(--font-sans)',
            }}>
              £
            </div>
            <span style={{
              flex: 1, fontSize: 13, fontWeight: 500,
              color: hovSavings ? '#fff' : 'var(--ink-900)', letterSpacing: '-0.005em',
            }}>
              £{WEEK_SAVED.toFixed(2)} saved this week. See how
            </span>
            <IconChevron size={13} style={{ color: hovSavings ? '#fff' : 'var(--ink-400)', flexShrink: 0 }}/>
          </button>
        </div>
      </div>
    </div>
  );
}

function Readout({ icon, label, value, unit, accent }) {
  return (
    <div style={{
      padding: '8px 12px',
      background: accent ? 'var(--lime-50)' : 'var(--surface)',
      border: '1px solid ' + (accent ? 'var(--lime-100)' : 'var(--cream-200)'),
      borderRadius: 'var(--r-md)',
    }}>
      <div style={{
        display: 'flex', alignItems: 'center', gap: 5,
        color: accent ? 'var(--lime-600)' : 'var(--ink-500)', marginBottom: 3,
      }}>
        {icon}
        <span className="t-label" style={{ fontSize: 11 }}>{label}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 2 }}>
        <span className="t-num" style={{
          fontSize: 17, color: 'var(--ink-900)', fontWeight: 600,
          letterSpacing: '-0.03em',
        }}>{value}</span>
        <span style={{ fontSize: 11, color: 'var(--ink-400)', fontWeight: 500 }}>{unit}</span>
      </div>
    </div>
  );
}

function WeatherPill({ isLoading, override, onTap }) {
  let dotColor, text;
  if (override)         { dotColor = '#E4A23A'; text = `Demo · ${override}`; }
  else if (isLoading)   { dotColor = '#9CA3A0'; text = 'Connecting…'; }
  else                  { dotColor = '#00C06F'; text = 'Connected - click for demo'; }
  const pulse = !override && !isLoading;

  return (
    <button onClick={onTap} style={{
      appearance: 'none', border: '1px solid var(--cream-200)',
      background: 'var(--surface)', cursor: 'pointer',
      padding: '5px 10px 5px 8px', borderRadius: 999,
      display: 'inline-flex', alignItems: 'center', gap: 6,
      fontSize: 11, color: 'var(--ink-700)',
      fontFamily: 'var(--font-sans)', fontWeight: 500,
      letterSpacing: '-0.005em', whiteSpace: 'nowrap',
    }}>
      <span style={{
        width: 7, height: 7, borderRadius: 999, background: dotColor,
        animation: pulse ? 'pwLivePulse 2s ease-in-out infinite' : 'none',
        flexShrink: 0,
      }}/>
      <span>{text}</span>
    </button>
  );
}

function OverridePanel({ override, onChange, liveKind, isLive, isLoading, hasError, onBack }) {
  const options = ['sunny', 'cloudy', 'rainy', 'night'];
  const sliderValue = options.indexOf(override || liveKind);
  // only count it as a demo override if it's different from the real weather
  const effectiveOverride = override && override !== liveKind ? override : null;

  return (
    <div className="pw-screen">
      <TabHeader eyebrow="Home" title="Weather demo"/>

      <div style={{ padding: '0 24px 90px' }}>
        <button onClick={onBack} style={{
          appearance: 'none', border: 0, background: 'transparent',
          display: 'flex', alignItems: 'center', gap: 6,
          color: 'var(--ink-700)', fontSize: 14, fontWeight: 500,
          fontFamily: 'var(--font-sans)', cursor: 'pointer',
          padding: '4px 0', marginBottom: 16,
        }}>
          <IconChevron dir="left" size={14}/>
          <span>Back to home</span>
        </button>

        <p style={{
          fontSize: 14, lineHeight: 1.55, color: 'var(--ink-700)',
          margin: '0 0 24px',
        }}>
          Only for demo purposes, move the slider to switch between sunny, cloudy, rainy and night. Tap "Reset" in the status box to restore real-time weather data from Met UK.
        </p>

        <div style={{
          padding: '12px 16px', marginBottom: 22,
          background: 'var(--surface)', border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          display: 'flex', alignItems: 'center', gap: 12,
        }}>
          <span style={{
            width: 8, height: 8, borderRadius: 999,
            background: effectiveOverride ? '#E4A23A' : (isLive ? '#00C06F' : (hasError ? '#D4524B' : '#9CA3A0')),
            flexShrink: 0,
          }}/>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div className="t-label" style={{ color: 'var(--ink-500)', fontSize: 11, marginBottom: 2 }}>Status</div>
            <div style={{ fontSize: 13, color: 'var(--ink-900)', fontWeight: 500 }}>
              {effectiveOverride
                ? `Demo override · ${effectiveOverride}`
                : isLoading ? 'Connecting to weather…'
                : hasError  ? 'Could not reach weather (using sunny)'
                : `Live · ${liveKind} in London`}
            </div>
          </div>
          {effectiveOverride && (
            <button onClick={() => onChange(null)} style={{
              appearance: 'none', cursor: 'pointer',
              border: '1px solid var(--cream-200)',
              background: 'var(--cream-100)',
              padding: '5px 12px', borderRadius: 999,
              fontSize: 11, color: 'var(--ink-700)',
              fontFamily: 'var(--font-sans)', fontWeight: 500,
              letterSpacing: '-0.005em',
              flexShrink: 0,
            }}>
              Reset
            </button>
          )}
        </div>

        <div className="t-label" style={{ color: 'var(--ink-500)', fontSize: 11, marginBottom: 14 }}>
          Override
        </div>

        <div style={{
          padding: '20px 16px 16px',
          background: 'var(--surface)', border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          marginBottom: 14,
        }}>
          <div style={{
            display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)',
            fontSize: 11, color: 'var(--ink-500)',
            fontFamily: 'var(--font-sans)', fontWeight: 500,
            letterSpacing: '0.04em', textTransform: 'uppercase',
            marginBottom: 10,
          }}>
            <span style={{ textAlign: 'left' }}>Sunny</span>
            <span style={{ textAlign: 'center' }}>Cloudy</span>
            <span style={{ textAlign: 'center' }}>Rainy</span>
            <span style={{ textAlign: 'right' }}>Night</span>
          </div>
          <input
            type="range" min="0" max="3" step="1" value={sliderValue}
            onChange={(e) => onChange(options[+e.target.value])}
            style={{ width: '100%', accentColor: 'var(--lime-500)' }}
          />
          <div style={{
            marginTop: 14, fontSize: 13,
            color: 'var(--ink-700)', fontFamily: 'var(--font-sans)',
          }}>
            Showing: <span style={{ color: 'var(--ink-900)', fontWeight: 600, textTransform: 'capitalize' }}>
              {override || liveKind}
            </span> {effectiveOverride ? '(override)' : '(live)'}
          </div>
        </div>

        <button
          onClick={onBack}
          style={{
            appearance: 'none', cursor: 'pointer',
            width: '100%', padding: '12px 14px',
            background: 'var(--ink-900)',
            color: '#fff',
            border: '1px solid var(--ink-900)',
            borderRadius: 'var(--r-md)',
            fontSize: 14, fontWeight: 500,
            fontFamily: 'var(--font-sans)',
            letterSpacing: '-0.005em',
          }}>
          Back to home
        </button>
      </div>
    </div>
  );
}

function ArrowUp({ size = 10 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 10 10" style={{ display: 'inline-block', verticalAlign: 'middle' }}>
      <path d="M5 1.5 L9 7.5 L1 7.5 Z" fill="#3FA86C"/>
    </svg>
  );
}

function ArrowDown({ size = 10 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 10 10" style={{ display: 'inline-block', verticalAlign: 'middle' }}>
      <path d="M5 8.5 L9 2.5 L1 2.5 Z" fill="#C24A45"/>
    </svg>
  );
}

function NodePopup({ kind, weatherKind, temp, scene, prices, onClose, onNavigate }) {
  const labels = { sunny: 'Clear / sunny', cloudy: 'Cloudy', rainy: 'Rainy', night: 'Night' };
  const fmtP = (n) => n.toFixed(1);

  const homeInsight = weatherKind === 'night'
    ? 'It\'s night-time — your battery is powering your home.'
    : weatherKind === 'rainy'
      ? 'Your home is running on battery power.'
      : weatherKind === 'cloudy'
        ? 'Your panels cover everything you use, with a small surplus for the battery and a neighbour.'
        : 'Your panels are powering your home, charging the battery, and sharing with neighbours.';

  // pick the battery message and arrow based on whether it's charging or discharging
  const battState = scene.batteryState;
  const batteryInsight = battState === 'discharging' ? 'Discharging — powering your home.'
                       : battState === 'holding'     ? 'Holding — battery is steady, no energy moving.'
                                                     : 'Charging — topping up from your panels.';
  const battArrow = battState === 'discharging' ? <ArrowDown/>
                  : battState === 'holding'     ? null
                                                : <ArrowUp/>;

  const ip = prices.importP;
  const gridBucket = ip < 18 ? 'Prices are cheap right now.'
                   : ip > 26 ? 'Prices are higher than usual.'
                   : 'Prices are around average right now.';
  const gridInsight = `Reading real-time Agile tariffs. ${gridBucket}`;

  const communityInsight = weatherKind === 'night'
    ? 'Your neighbours are running on the grid overnight.'
    : weatherKind === 'rainy'
      ? 'Your neighbours are buying from the grid today.'
      : weatherKind === 'cloudy'
        ? 'You and the grid are keeping the community supplied.'
        : 'Your neighbours are buying your surplus.';

  const map = {
    sun: {
      title: 'Weather',
      icon: <IconSolar size={14}/>,
      rows: [
        ['Temperature', `${temp}°C`],
        ['Conditions', labels[weatherKind]],
      ],
      insight: 'Reading real-time Met data.',
    },
    home: {
      title: 'Your home',
      icon: <IconHome size={14}/>,
      rows: [
        ['Solar production', `${scene.prodPct}%`],
      ],
      insight: homeInsight,
    },
    battery: {
      title: 'Battery',
      icon: <IconBattery size={14}/>,
      rows: [
        ['State of charge', (
          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4 }}>
            {battArrow}
            <span>{BATTERY_SOC}%</span>
          </span>
        )],
      ],
      insight: batteryInsight,
    },
    grid: {
      title: 'Grid',
      icon: <IconBolt size={14}/>,
      rows: [
        ['Import Price', `${fmtP(prices.importP)}p / kWh`],
        ['Export Price', `${fmtP(prices.exportP)}p / kWh`],
      ],
      insight: gridInsight,
    },
    community: {
      title: 'Community',
      icon: <IconBolt size={14}/>,
      rows: [
        ['Peer-to-peer price', `${fmtP(prices.p2p)}p / kWh`],
      ],
      note: 'The peer price is the midpoint between the grid’s import and export prices.',
      insight: communityInsight,
      cta: { label: 'Explore your community', onClick: () => { onClose(); onNavigate && onNavigate('community'); } },
    },
  };
  const c = map[kind];
  if (!c) return null;

  return (
    <div onClick={onClose} style={{
      position: 'absolute', inset: 0,
      background: 'rgba(20,28,24,0.32)',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 5, borderRadius: 'var(--r-lg)',
      padding: 20,
    }}>
      <div onClick={(e) => e.stopPropagation()} style={{
        width: 240, padding: '14px 16px 12px',
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
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
          <div style={{
            width: 26, height: 26, borderRadius: 7,
            background: 'var(--lime-50)', color: 'var(--lime-600)',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            flexShrink: 0,
          }}>{c.icon}</div>
          <div style={{
            fontSize: 14, fontWeight: 600,
            color: 'var(--ink-900)', letterSpacing: '-0.01em',
          }}>{c.title}</div>
        </div>
        {c.rows.map(([label, value], i) => (
          <div key={i} style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '8px 0',
            borderTop: i === 0 ? 0 : '1px solid var(--cream-200)',
          }}>
            <span style={{ fontSize: 13, color: 'var(--ink-600)' }}>{label}</span>
            <span style={{
              fontSize: 13, color: 'var(--ink-900)', fontWeight: 600,
              fontFamily: 'var(--font-sans)',
            }}>{value}</span>
          </div>
        ))}
        {c.note && (
          <div style={{
            marginTop: 4, paddingTop: 10,
            borderTop: '1px solid var(--cream-200)',
            fontSize: 12, color: 'var(--ink-600)',
            lineHeight: 1.45,
          }}>
            {c.note}
          </div>
        )}
        {c.insight && (
          <div style={{
            marginTop: 4, paddingTop: 10,
            borderTop: '1px solid var(--cream-200)',
            fontSize: 12, color: 'var(--ink-600)',
            lineHeight: 1.45, fontStyle: 'italic',
            display: 'flex', alignItems: 'flex-start', gap: 6,
          }}>
            <span style={{ color: 'var(--lime-600)', marginTop: 1, flexShrink: 0, display: 'inline-flex' }}>
              <IconSparkle size={12}/>
            </span>
            <span>{c.insight}</span>
          </div>
        )}
        {c.cta && (
          <button onClick={c.cta.onClick} style={{
            appearance: 'none', cursor: 'pointer', width: '100%',
            marginTop: 16, padding: '10px 12px',
            background: 'var(--ink-900)', color: '#fff',
            border: 0, borderRadius: 'var(--r-md)',
            fontSize: 13, fontWeight: 500,
            fontFamily: 'var(--font-sans)', letterSpacing: '-0.005em',
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
          }}>
            <span>{c.cta.label}</span>
            <IconChevron size={12}/>
          </button>
        )}
      </div>
    </div>
  );
}

// Draws the house picture: sun at the top, house in the middle, and battery,
// grid and community along the bottom. The numbers below set where each one sits and its size.
function HouseScene({ tick, kind, onTap }) {
  const HOUSE_X = 180, HOUSE_Y = 155;
  const BATT_X  = 55,  BATT_Y = 285;
  const GRID_X  = 180, GRID_Y = 290;
  const COMM_X  = 305, COMM_Y = 285;
  const SUN_X   = 180, SUN_Y  = 55;
  const BATT_SCALE = 1.0;
  const GRID_SCALE = 0.9;
  const COMM_SCALE = 1.0;
  const isOff = kind === 'rainy' || kind === 'night'; // rainy or night means no solar
  const batteryState = isOff ? 'discharging' : 'charging';
  const battFill   = batteryState === 'discharging' ? '#C29670' : '#86A893';
  const RAIN_BLUE  = '#2F5C84';
  const NIGHT_BLUE = '#6F8FB8';
  const GREY_FLOW  = '#7B8689';
  const flowBlue   = kind === 'night' ? NIGHT_BLUE : RAIN_BLUE;
  const labelColor = kind === 'night' ? '#E8EDF5' : 'var(--ink-900)';

  const tap = (id) => (e) => { e.stopPropagation(); onTap(id); };
  const tappableStyle = { cursor: 'pointer' };

  const showSun = kind === 'sunny' || kind === 'cloudy';

  return (
    <svg viewBox="0 0 360 360" style={{ width: '100%', height: '100%', display: 'block' }}>
      <defs>
        <radialGradient id="sunGlow" cx="0.5" cy="0.5" r="0.5">
          <stop offset="0%"  stopColor="#FFE8A0" stopOpacity="0.9"/>
          <stop offset="60%" stopColor="#FFD46A" stopOpacity="0.2"/>
          <stop offset="100%" stopColor="#FFD46A" stopOpacity="0"/>
        </radialGradient>
        <linearGradient id="roofGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%"  stopColor="#2A3A34"/>
          <stop offset="100%" stopColor="#1A2A24"/>
        </linearGradient>
        <linearGradient id="panelGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%"  stopColor="#1a3c2e"/>
          <stop offset="100%" stopColor="#0e2a1f"/>
        </linearGradient>
      </defs>

      {/* SUN (sunny + cloudy) */}
      {showSun && (
        <g style={tappableStyle} onClick={tap('sun')}>
          <rect x={SUN_X - 50} y={SUN_Y - 50} width="100" height="100" fill="transparent"/>
          <circle cx={SUN_X} cy={SUN_Y} r="44" fill="url(#sunGlow)" opacity={kind === 'cloudy' ? 0.55 : 1}/>
          <circle cx={SUN_X} cy={SUN_Y} r="20" fill="#FFC94A" opacity={kind === 'cloudy' ? 0.85 : 1}/>
          {[0, 45, 90, 135, 180, 225, 270, 315].map(a => {
            const r1 = 26 + Math.sin(tick * 2 + a) * 1.5;
            const r2 = 32 + Math.sin(tick * 2 + a) * 1.5;
            const x1 = SUN_X + Math.cos(a * Math.PI/180) * r1;
            const y1 = SUN_Y + Math.sin(a * Math.PI/180) * r1;
            const x2 = SUN_X + Math.cos(a * Math.PI/180) * r2;
            const y2 = SUN_Y + Math.sin(a * Math.PI/180) * r2;
            return <line key={a} x1={x1} y1={y1} x2={x2} y2={y2}
                         stroke="#FFC94A" strokeWidth="2.5" strokeLinecap="round"
                         opacity={kind === 'cloudy' ? 0.7 : 1}/>;
          })}
        </g>
      )}

      {/* CLOUDS — a few on cloudy days, more on rainy ones */}
      {kind === 'cloudy' && (
        <g style={tappableStyle} onClick={tap('sun')}>
          <rect x={SUN_X - 150} y={SUN_Y - 45} width="290" height="90" fill="transparent"/>
          <Cloud cx={SUN_X + 26}  cy={SUN_Y + 8}   scale={1.05}/>
          <Cloud cx={SUN_X - 78}  cy={SUN_Y + 22}  scale={1.2}/>
          <Cloud cx={SUN_X - 130} cy={SUN_Y - 20}  scale={1.1}/>
          <Cloud cx={SUN_X + 110} cy={SUN_Y - 18}  scale={1.25}/>
        </g>
      )}
      {kind === 'rainy' && (
        <g style={tappableStyle} onClick={tap('sun')}>
          <rect x={SUN_X - 110} y={SUN_Y - 40} width="220" height="90" fill="transparent"/>
          <Cloud cx={SUN_X} cy={SUN_Y + 4} scale={1.45} dark/>
          <Cloud cx={SUN_X - 78} cy={SUN_Y + 22} scale={0.95} dark/>
          <Cloud cx={SUN_X + 78} cy={SUN_Y + 28} scale={0.9} dark/>
        </g>
      )}

      {/* RAIN */}
      {kind === 'rainy' && <Rain tick={tick}/>}

      {/* NIGHT — moon and stars instead of the sun */}
      {kind === 'night' && (
        <g style={tappableStyle} onClick={tap('sun')}>
          <rect x={SUN_X - 60} y={SUN_Y - 50} width="120" height="100" fill="transparent"/>
          {/* twinkling stars */}
          {[
            { x: SUN_X - 110, y: SUN_Y - 18, r: 1.0, ph: 0 },
            { x: SUN_X - 70,  y: SUN_Y + 36, r: 0.9, ph: 1.3 },
            { x: SUN_X + 80,  y: SUN_Y - 22, r: 1.1, ph: 2.1 },
            { x: SUN_X + 130, y: SUN_Y + 12, r: 0.8, ph: 0.7 },
            { x: SUN_X - 145, y: SUN_Y + 18, r: 0.85, ph: 2.6 },
            { x: SUN_X + 60,  y: SUN_Y + 38, r: 0.7, ph: 1.8 },
          ].map((s, i) => (
            <circle key={i} cx={s.x} cy={s.y} r={s.r}
                    fill="#F4EFD8"
                    opacity={0.5 + 0.4 * Math.abs(Math.sin(tick * 1.3 + s.ph))}/>
          ))}
          {/* moon */}
          <circle cx={SUN_X} cy={SUN_Y} r="34" fill="#1A2238" opacity="0.4"/>
          <circle cx={SUN_X} cy={SUN_Y} r="22" fill="#F0E9D2"/>
          <circle cx={SUN_X + 9} cy={SUN_Y - 3} r="19" fill="#1F2940"/>
          {/* moon craters */}
          <circle cx={SUN_X - 6} cy={SUN_Y + 4} r="1.8" fill="#D8CFB3" opacity="0.55"/>
          <circle cx={SUN_X - 10} cy={SUN_Y - 5} r="1.2" fill="#D8CFB3" opacity="0.45"/>
        </g>
      )}

      {/* HOUSE */}
      <g style={tappableStyle} onClick={tap('home')}>
        <rect x={HOUSE_X - 55} y={HOUSE_Y - 50} width="110" height="120" fill="transparent"/>
        <g transform={`translate(${HOUSE_X - 40}, ${HOUSE_Y - 40})`}>
          <ellipse cx="40" cy="92" rx="50" ry="4" fill="rgba(0,0,0,0.12)"/>
          <rect x="5" y="40" width="70" height="50" fill="#F7F3E8" stroke="#E4E0D4" strokeWidth="1"/>
          <polygon points="-5,42 40,8 85,42 75,42 40,18 5,42" fill="url(#roofGrad)"/>
          <polygon points="8,40 38,17 38,30 12,47" fill="url(#panelGrad)" stroke="#00C06F" strokeWidth="0.6"/>
          <polygon points="42,17 72,40 68,47 42,30" fill="url(#panelGrad)" stroke="#00C06F" strokeWidth="0.6"/>
          {!isOff && <polygon points="8,40 38,17 38,30 12,47" fill="#FFE8A0" opacity={kind === 'cloudy' ? 0.15 : 0.3}/>}
          {!isOff && <polygon points="42,17 72,40 68,47 42,30" fill="#FFE8A0" opacity={kind === 'cloudy' ? 0.15 : 0.3}/>}
          <rect x="14" y="52" width="18" height="18" fill="#B8D4E8"/>
          <line x1="23" y1="52" x2="23" y2="70" stroke="#8FA8BC" strokeWidth="0.5"/>
          <line x1="14" y1="61" x2="32" y2="61" stroke="#8FA8BC" strokeWidth="0.5"/>
          <rect x="45" y="62" width="14" height="28" fill="#2a3a34"/>
          <circle cx="56" cy="77" r="0.8" fill="#00C06F"/>
        </g>
        <NodeLabel x={HOUSE_X} y={HOUSE_Y + 72} text="YOUR HOME" fill={labelColor}/>
      </g>

      {/* BATTERY */}
      <g style={tappableStyle} onClick={tap('battery')}>
        <g transform={`translate(${BATT_X}, ${BATT_Y}) scale(${BATT_SCALE})`}>
          <rect x="-30" y="-22" width="60" height="58" fill="transparent"/>
          <ellipse cx="0" cy="18" rx="24" ry="3" fill="rgba(0,0,0,0.12)"/>
          <rect x="-22" y="-13" width="40" height="26" rx="3.5" fill="#F7F3E8" stroke="#2a3a34" strokeWidth="1.5"/>
          <rect x="18"  y="-6"  width="3"  height="12" rx="1" fill="#2a3a34"/>
          <rect x="-19.5" y="-10" width={(BATTERY_SOC / 100) * 35} height="20" rx="1.5"
                fill={battFill}/>
          {/* + when charging, - when discharging */}
          <text x="0" y="3" textAnchor="middle" fontSize="10" fontWeight="700"
                fill="#fff" fontFamily="Inter, Geist, sans-serif"
                style={{ pointerEvents: 'none' }}>
            {batteryState === 'charging' ? '+' : '−'}
          </text>
        </g>
        <NodeLabel x={BATT_X} y={BATT_Y + 50 * BATT_SCALE} text="BATTERY" fill={labelColor}/>
      </g>

      {/* GRID */}
      <g style={tappableStyle} onClick={tap('grid')}>
        <g transform={`translate(${GRID_X}, ${GRID_Y}) scale(${GRID_SCALE})`}>
          <rect x="-30" y="-30" width="60" height="80" fill="transparent"/>
          <rect x="-2" y="-22" width="4" height="44" fill="#6B7370"/>
          <rect x="-18" y="-26" width="36" height="5" rx="0.5" fill="#6B7370"/>
          <rect x="-18" y="-16" width="36" height="5" rx="0.5" fill="#6B7370"/>
          <line x1="-18" y1="-30" x2="-12" y2="-22" stroke="#6B7370" strokeWidth="2"/>
          <line x1="18"  y1="-30" x2="12"  y2="-22" stroke="#6B7370" strokeWidth="2"/>
        </g>
        <NodeLabel x={GRID_X} y={GRID_Y + 50 * GRID_SCALE} text="GRID" fill={labelColor}/>
      </g>

      {/* COMMUNITY */}
      <g style={tappableStyle} onClick={tap('community')}>
        <g transform={`translate(${COMM_X}, ${COMM_Y}) scale(${COMM_SCALE})`}>
          <rect x="-45" y="-30" width="90" height="80" fill="transparent"/>
          {[
            { x: -26, y: 6,  h: 30 },
            { x: 0,   y: -6, h: 38 },
            { x: 26,  y: 8,  h: 28 },
          ].map((c, i) => (
            <g key={i} transform={`translate(${c.x}, ${c.y})`}>
              <rect x="-11" y="-5" width="22" height={c.h} fill="#F7F3E8" stroke="#E4E0D4" strokeWidth="0.8"/>
              <polygon points="-13,-5 0,-16 13,-5" fill="#2a3a34"/>
              <rect x="-4" y="3" width="6" height="7" fill="#FFC94A"
                    opacity={0.6 + 0.3 * Math.sin(tick * 2.5 + i)}/>
            </g>
          ))}
        </g>
        <NodeLabel x={COMM_X} y={COMM_Y + 50 * COMM_SCALE} text="COMMUNITY" fill={labelColor}/>
      </g>

      {/* FLOWS — the moving lines of energy. Which ones show depends on the weather. */}
      {/* sun to home */}
      {!isOff && (
        <FlowLine
          path={`M ${SUN_X} ${SUN_Y + 25} Q ${SUN_X} ${HOUSE_Y - 50} ${HOUSE_X} ${HOUSE_Y - 32}`}
          tick={tick} color="#FFB83D" speed={0.6} particles={3} dashed/>
      )}

      {/* home to battery (charging) */}
      {!isOff && (
        <FlowLine
          path={`M ${HOUSE_X - 30} ${HOUSE_Y + 30} Q ${HOUSE_X - 100} ${HOUSE_Y + 70} ${BATT_X + 22} ${BATT_Y - 18}`}
          tick={tick} color="#00A862" speed={0.6} particles={3}/>
      )}

      {/* battery to home (discharging) */}
      {isOff && (
        <FlowLine
          path={`M ${BATT_X + 22} ${BATT_Y - 18} Q ${HOUSE_X - 100} ${HOUSE_Y + 70} ${HOUSE_X - 30} ${HOUSE_Y + 30}`}
          tick={tick} color={flowBlue} speed={0.6} particles={3}/>
      )}

      {/* home to community (sharing surplus) */}
      {!isOff && (
        <FlowLine
          path={`M ${HOUSE_X + 30} ${HOUSE_Y + 30} Q ${HOUSE_X + 100} ${HOUSE_Y + 70} ${COMM_X - 30} ${COMM_Y - 10}`}
          tick={tick} color="#00A862" speed={0.6} particles={4}/>
      )}

      {/* grid to community */}
      {kind === 'cloudy' && (
        <FlowLine
          path={`M ${GRID_X + 22} ${GRID_Y - 8} Q ${(GRID_X + COMM_X) / 2} ${GRID_Y - 14} ${COMM_X - 26} ${COMM_Y - 8}`}
          tick={tick} color={GREY_FLOW} speed={0.6} particles={2} dashed/>
      )}

      {/* grid to community */}
      {isOff && (
        <FlowLine
          path={`M ${GRID_X + 22} ${GRID_Y - 8} Q ${(GRID_X + COMM_X) / 2} ${GRID_Y - 14} ${COMM_X - 26} ${COMM_Y - 8}`}
          tick={tick} color={flowBlue} speed={0.6} particles={3}/>
      )}

      {/* small trades between neighbours, which vary with the weather */}
      {kind === 'sunny' && (
        <FlowLine
          path={`M ${COMM_X - 4} ${COMM_Y - 6} Q ${COMM_X - 14} ${COMM_Y - 18} ${COMM_X - 22} ${COMM_Y + 2}`}
          tick={tick} color="#00A862" speed={0.4} particles={2}/>
      )}
      {kind === 'sunny' && (
        <FlowLine
          path={`M ${COMM_X + 4} ${COMM_Y - 6} Q ${COMM_X + 14} ${COMM_Y - 18} ${COMM_X + 22} ${COMM_Y + 4}`}
          tick={tick} color="#00A862" speed={0.4} particles={2}/>
      )}
      {kind === 'cloudy' && (
        <FlowLine
          path={`M ${COMM_X - 4} ${COMM_Y - 6} Q ${COMM_X - 14} ${COMM_Y - 18} ${COMM_X - 22} ${COMM_Y + 2}`}
          tick={tick} color="#5C8E72" speed={0.4} particles={2}/>
      )}
      {isOff && (
        <FlowLine
          path={`M ${COMM_X - 22} ${COMM_Y + 2} Q ${COMM_X} ${COMM_Y - 22} ${COMM_X + 22} ${COMM_Y + 4}`}
          tick={tick} color={flowBlue} speed={0.4} particles={2}/>
      )}
    </svg>
  );
}

function Cloud({ cx, cy, scale = 1, dark = false }) {
  const fill = dark ? '#5B6770' : '#FFFFFF';
  const op   = dark ? 0.92 : 0.94;
  return (
    <g transform={`translate(${cx} ${cy}) scale(${scale})`} opacity={op}>
      <ellipse cx="-14" cy="3"  rx="13" ry="9"  fill={fill}/>
      <ellipse cx="0"   cy="-4" rx="16" ry="11" fill={fill}/>
      <ellipse cx="13"  cy="2"  rx="12" ry="9"  fill={fill}/>
      <ellipse cx="-6"  cy="7"  rx="11" ry="6"  fill={fill}/>
      <ellipse cx="6"   cy="7"  rx="11" ry="6"  fill={fill}/>
    </g>
  );
}

function Rain({ tick }) {
  return (
    <g style={{ pointerEvents: 'none' }}>
      {Array.from({ length: 32 }).map((_, i) => {
        const x = (i * 11 + 6) % 360;
        const phase = (i * 37) % 100 / 100;
        const y = (((tick * 90) + phase * 320) % 320) + 30;
        return (
          <line key={i} x1={x} y1={y} x2={x - 3} y2={y + 9}
                stroke="#7BA0C9" strokeWidth="1.3" strokeOpacity="0.6"
                strokeLinecap="round"/>
        );
      })}
    </g>
  );
}

function NodeLabel({ x, y, text, fill = 'var(--ink-900)' }) {
  return (
    <g>
      <text x={x} y={y} textAnchor="middle"
            fontSize="13" fontFamily="Inter, Geist, sans-serif"
            fill={fill} letterSpacing="0.03em" fontWeight="900">
        {text}
      </text>
    </g>
  );
}

function FlowLine({ path, tick, color, speed = 1, particles = 4, dashed }) {
  const ref = React.useRef();
  const [len, setLen] = React.useState(0);
  React.useEffect(() => {
    if (ref.current) setLen(ref.current.getTotalLength());
  }, [path]);

  const pts = [];
  if (len) {
    for (let i = 0; i < particles; i++) {
      const p = ((tick * speed * 0.22 + i / particles) % 1) * len;
      if (ref.current) {
        const pt = ref.current.getPointAtLength(p);
        pts.push({ x: pt.x, y: pt.y, phase: i / particles });
      }
    }
  }

  return (
    <g style={{ pointerEvents: 'none' }}>
      <path ref={ref} d={path} fill="none" stroke={color} strokeOpacity="0.3"
            strokeWidth="1.5" strokeDasharray={dashed ? "2 4" : "3 4"}/>
      {pts.map((p, i) => (
        <circle key={i} cx={p.x} cy={p.y} r="3.5" fill={color}>
          <animate attributeName="opacity" values="0;1;0"
                   dur={`${2/speed}s`} repeatCount="indefinite"
                   begin={`${p.phase * 2/speed}s`}/>
        </circle>
      ))}
    </g>
  );
}

Object.assign(window, { HouseTab });
