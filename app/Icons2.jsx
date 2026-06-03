// App icons as small SVG components.
function IconTabHome({ size = 22, filled }) {
  return filled ? (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="currentColor">
      <path d="M11 2.5l8.5 7v9.5a1 1 0 01-1 1h-5v-6h-5v6h-5a1 1 0 01-1-1V9.5L11 2.5z"/>
    </svg>
  ) : (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 9.5L11 2.5l8 7v9.5a1 1 0 01-1 1h-4.5v-6h-5v6H4a1 1 0 01-1-1V9.5z"/>
    </svg>
  );
}

function IconTabCommunity({ size = 22, filled }) {
  return filled ? (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="currentColor">
      <circle cx="7" cy="8" r="3"/>
      <circle cx="15" cy="8" r="3"/>
      <path d="M1.5 18c0-2.6 2.4-4.5 5.5-4.5s5.5 1.9 5.5 4.5H1.5zM9.5 18c0-2.6 2.4-4.5 5.5-4.5s5.5 1.9 5.5 4.5H9.5z"/>
    </svg>
  ) : (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="7" cy="8" r="3"/>
      <circle cx="15" cy="8" r="3"/>
      <path d="M1.5 18c0-2.6 2.4-4.5 5.5-4.5s5.5 1.9 5.5 4.5M9.5 18c0-2.6 2.4-4.5 5.5-4.5s5.5 1.9 5.5 4.5"/>
    </svg>
  );
}

function IconTabDashboard({ size = 26, filled }) {
  // Lightning bolt for the Dashboard tab.
  return filled ? (
    <svg width={size} height={size} viewBox="0 0 26 26" fill="currentColor">
      <path d="M14.5 2L6 14h6l-2 10 8.5-12h-6l2-10z"/>
    </svg>
  ) : (
    <svg width={size} height={size} viewBox="0 0 26 26" fill="none"
         stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14.5 2L6 14h6l-2 10 8.5-12h-6l2-10z"/>
    </svg>
  );
}

function IconTabAssistant({ size = 22, filled }) {
  return filled ? (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="currentColor">
      <path d="M3 5a2 2 0 012-2h12a2 2 0 012 2v9a2 2 0 01-2 2h-6l-4 3.5V16H5a2 2 0 01-2-2V5z"/>
    </svg>
  ) : (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 5a2 2 0 012-2h12a2 2 0 012 2v9a2 2 0 01-2 2h-6l-4 3.5V16H5a2 2 0 01-2-2V5z"/>
    </svg>
  );
}

function IconTabProfile({ size = 22, filled }) {
  return filled ? (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="currentColor">
      <circle cx="11" cy="7.5" r="3.5"/>
      <path d="M3 19c0-3.8 3.6-6.5 8-6.5s8 2.7 8 6.5H3z"/>
    </svg>
  ) : (
    <svg width={size} height={size} viewBox="0 0 22 22" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="7.5" r="3.5"/>
      <path d="M3 19c0-3.8 3.6-6.5 8-6.5s8 2.7 8 6.5"/>
    </svg>
  );
}

function IconMic({ size = 20 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <rect x="7" y="2" width="6" height="10" rx="3"/>
      <path d="M4 9a6 6 0 0012 0M10 15v3M7 18h6"/>
    </svg>
  );
}

function IconSend({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18" fill="currentColor">
      <path d="M2 9l14-7-4 7 4 7L2 9z"/>
    </svg>
  );
}

function IconSun({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="10" cy="10" r="3.5"/>
      <path d="M10 2v2M10 16v2M2 10h2M16 10h2M4.2 4.2l1.4 1.4M14.4 14.4l1.4 1.4M4.2 15.8l1.4-1.4M14.4 5.6l1.4-1.4"/>
    </svg>
  );
}

function IconMoon({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M16 12.5A7 7 0 017.5 4 7 7 0 1016 12.5z"/>
    </svg>
  );
}

function IconBattery({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2.5" y="6" width="13" height="8" rx="1.5"/>
      <rect x="15.5" y="8.5" width="2" height="3" rx="0.5" fill="currentColor" stroke="none"/>
    </svg>
  );
}

function IconGrid({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10 2v6M10 12v6M2 10h6M12 10h6"/>
      <circle cx="10" cy="10" r="2"/>
      <path d="M4 4l3 3M16 4l-3 3M4 16l3-3M16 16l-3-3"/>
    </svg>
  );
}

function IconCloud({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M5.5 14h9a3.5 3.5 0 000-7 5 5 0 00-9.7 1.2A3 3 0 005.5 14z"/>
    </svg>
  );
}

function IconSparkle({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="currentColor">
      <path d="M8 1l1.3 4.7L14 7l-4.7 1.3L8 13l-1.3-4.7L2 7l4.7-1.3L8 1z"/>
    </svg>
  );
}

function IconPlus({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14" fill="none"
         stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <path d="M7 3v8M3 7h8"/>
    </svg>
  );
}

function IconBell({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none"
         stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3.5 11.5h9l-1-1.5V7.5a3.5 3.5 0 10-7 0V10l-1 1.5zM6.5 13a1.5 1.5 0 003 0"/>
    </svg>
  );
}

function IconExternal({ size = 12 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 12 12" fill="none"
         stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round">
      <path d="M5 2H2.5v7.5H10V7M6.5 2H10v3.5M6 6l4-4"/>
    </svg>
  );
}

function IconPause({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14" fill="currentColor">
      <rect x="3" y="2" width="3" height="10" rx="1"/>
      <rect x="8" y="2" width="3" height="10" rx="1"/>
    </svg>
  );
}

function IconLeaf({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" fill="none"
         stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M13 3c-6 0-10 3-10 7 0 1 .3 2 1 3 2-4 5-6 9-7-3 2-5 5-6 9 4 0 7-4 7-9 0-1 0-2-1-3z"/>
    </svg>
  );
}

Object.assign(window, {
  IconTabHome, IconTabCommunity, IconTabDashboard, IconTabAssistant, IconTabProfile,
  IconMic, IconSend, IconSun, IconMoon, IconBattery, IconGrid, IconCloud,
  IconSparkle, IconPlus, IconBell, IconExternal, IconPause, IconLeaf,
});
