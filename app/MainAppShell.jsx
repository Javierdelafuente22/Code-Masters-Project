// Main app shell for the 5 tabs.
// On mobile it fills the screen; on desktop it sits inside the phone frame.

// Weather lives here, not in HouseTab, so both the Home and Community tabs
// can use the same value.
const SHELL_WEATHER_LAT = 51.48, SHELL_WEATHER_LON = -0.20; // Fulham, SW6
function shellClassifyWeather(code) {
  if (code <= 1) return 'sunny';
  if (code <= 64) return 'cloudy';
  return 'rainy';
}

// Night is 20:00–06:00 UK time. At night we show the night look.
function isUKNight(date) {
  const hr = parseInt(
    new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', hour: 'numeric', hour12: false }).format(date),
    10
  );
  return hr >= 20 || hr < 6;
}

function useWeatherState() {
  const [weather, setWeather] = React.useState(null);
  const [override, setOverride] = React.useState(null);
  const [now, setNow] = React.useState(() => new Date());

  React.useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 60 * 1000);
    return () => clearInterval(id);
  }, []);

  React.useEffect(() => {
    let cancelled = false;
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${SHELL_WEATHER_LAT}&longitude=${SHELL_WEATHER_LON}&current=temperature_2m,weather_code&timezone=Europe%2FLondon`;
    fetch(url)
      .then(r => r.json())
      .then(d => {
        if (cancelled) return;
        if (d && d.current && typeof d.current.weather_code === 'number') {
          setWeather({
            kind: shellClassifyWeather(d.current.weather_code),
            temp: Math.round(d.current.temperature_2m),
          });
        } else {
          setWeather({ kind: 'sunny', temp: 18, error: true });
        }
      })
      .catch(() => { if (!cancelled) setWeather({ kind: 'sunny', temp: 18, error: true }); });
    return () => { cancelled = true; };
  }, []);

  const isNight = isUKNight(now);
  // At night we always use 'night', even if the weather is sunny.
  const liveKind = isNight ? 'night' : (weather?.kind ?? 'sunny');
  // If the chosen override is the same as the live weather, ignore it so the
  // app still counts as live.
  const effectiveOverride = override && override !== liveKind ? override : null;
  const activeKind = effectiveOverride || liveKind;
  const activeTemp = weather?.temp ?? 18;
  const isLoading = !weather;
  const hasError = !!weather?.error;
  const isLive = !effectiveOverride && weather && !hasError;

  return { weather, override: effectiveOverride, setOverride, liveKind, activeKind, activeTemp, isLoading, hasError, isLive };
}

function MainAppShell() {
  const [tab, setTab] = React.useState('home');
  const [communityHighlight, setCommunityHighlight] = React.useState(false);
  const [homeHighlight, setHomeHighlight] = React.useState(true);
  const weatherState = useWeatherState();

  const isMobile =
    window.matchMedia('(max-width: 600px) and (pointer: coarse)').matches ||
    window.matchMedia('(display-mode: standalone)').matches ||
    window.navigator.standalone === true;

  const handleNavigate = (nextTab, fromBanner = false) => {
    setTab(nextTab);
    if (fromBanner && nextTab === 'community') setCommunityHighlight(true);
  };

  const renderTab = () => {
    switch (tab) {
      case 'home':      return <HouseTab onNavigate={handleNavigate} highlight={homeHighlight} onClearHighlight={() => setHomeHighlight(false)} weatherState={weatherState}/>;
      case 'community': return <CommunityTab highlight={communityHighlight} onClearHighlight={() => setCommunityHighlight(false)} weatherState={weatherState}/>;
      case 'dashboard': return <DashboardTab onNavigate={handleNavigate}/>;
      case 'profile':   return <ProfileTab/>;
      default:          return <DashboardTab/>;
    }
  };

  // Keep the Assistant tab mounted so its chat isn't lost when switching tabs;
  // we just hide it instead of removing it.
  const assistantLayer = (style) => (
    <div style={{ position: 'absolute', inset: 0, display: tab === 'assistant' ? 'flex' : 'none', flexDirection: 'column', ...style }}>
      <AssistantTab/>
      <TabBar active={tab} onChange={setTab}/>
    </div>
  );

  if (isMobile) {
    return (
      <div style={{
        position: 'fixed',
        top: 0, left: 0, right: 0, bottom: 0,
        background: 'var(--cream-50, #F4F5F2)',
        overflow: 'hidden',
      }}>
        {assistantLayer({})}
        {tab !== 'assistant' && (
          <div key={tab} style={{ position: 'absolute', inset: 0 }} className="pw-fade-in">
            {renderTab()}
            <TabBar active={tab} onChange={setTab}/>
          </div>
        )}
      </div>
    );
  }

  return (
    <div style={{
      minHeight: '100vh', background: '#E8E3D6',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      padding: '20px', boxSizing: 'border-box',
    }}>
      <IOSDevice width={390} height={844}>
        <div style={{ height: '100%', position: 'relative' }}>
          {assistantLayer({})}
          {tab !== 'assistant' && (
            <div key={tab} style={{ height: '100%', position: 'relative' }} className="pw-fade-in">
              {renderTab()}
              <TabBar active={tab} onChange={setTab}/>
            </div>
          )}
        </div>
      </IOSDevice>
    </div>
  );
}

Object.assign(window, { MainAppShell });
