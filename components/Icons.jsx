// Reusable icon components.
const PW_STROKE = { stroke: 'currentColor', strokeWidth: 1.5, strokeLinecap: 'round', strokeLinejoin: 'round', fill: 'none' };

function IconCheck({ size = 20 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 20 20"><path {...PW_STROKE} d="M4.5 10.5l3.5 3.5L15.5 6.5"/></svg>
  );
}
function IconChevron({ size = 16, dir = 'right', style }) {
  const rot = { right: 0, left: 180, down: 90, up: -90 }[dir];
  return (
    <svg width={size} height={size} viewBox="0 0 16 16" style={{ transform: `rotate(${rot}deg)`, ...style }}>
      <path {...PW_STROKE} d="M6 3l5 5-5 5"/>
    </svg>
  );
}
function IconInfo({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <circle cx="7" cy="7" r="6" {...PW_STROKE}/>
      <path {...PW_STROKE} d="M7 6.5v3.5"/>
      <circle cx="7" cy="4.25" r="0.5" fill="currentColor"/>
    </svg>
  );
}
function IconPencil({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <path {...PW_STROKE} d="M2 12l1-3 6-6 2 2-6 6-3 1zM8 3l2 2"/>
    </svg>
  );
}
function IconLock({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <rect x="2.5" y="6.5" width="9" height="6" rx="1.5" {...PW_STROKE}/>
      <path {...PW_STROKE} d="M4.5 6.5V4.5a2.5 2.5 0 015 0v2"/>
    </svg>
  );
}
function IconPin({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16">
      <path {...PW_STROKE} d="M8 14s5-4.5 5-8.5a5 5 0 00-10 0C3 9.5 8 14 8 14z"/>
      <circle cx="8" cy="5.5" r="1.75" {...PW_STROKE}/>
    </svg>
  );
}
function IconSolar({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18">
      <path {...PW_STROKE} d="M2 12.5h14l-1.5-7h-11L2 12.5z"/>
      <path {...PW_STROKE} d="M6 5.5v7M10 5.5v7M2.75 9h12.5"/>
    </svg>
  );
}
function IconCar({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18">
      <path {...PW_STROKE} d="M2.5 11.5h13v-3l-2-3.5h-9l-2 3.5v3z"/>
      <circle cx="5.5" cy="12.5" r="1.25" {...PW_STROKE}/>
      <circle cx="12.5" cy="12.5" r="1.25" {...PW_STROKE}/>
      <path {...PW_STROKE} d="M3 8.5h12"/>
    </svg>
  );
}
function IconHome({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18">
      <path {...PW_STROKE} d="M3 8l6-5 6 5v7a1 1 0 01-1 1h-4v-5H8v5H4a1 1 0 01-1-1V8z"/>
    </svg>
  );
}
function IconPeople({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18">
      <circle cx="6" cy="6" r="2.5" {...PW_STROKE}/>
      <circle cx="12" cy="6" r="2.5" {...PW_STROKE}/>
      <path {...PW_STROKE} d="M2 14c0-2.2 1.8-4 4-4s4 1.8 4 4M10 14c0-2.2 1.8-4 4-4s2 1 2 2"/>
    </svg>
  );
}
function IconBriefcase({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18">
      <rect x="2.5" y="5.5" width="13" height="9" rx="1.5" {...PW_STROKE}/>
      <path {...PW_STROKE} d="M6.5 5.5V4a1 1 0 011-1h3a1 1 0 011 1v1.5M2.5 9.5h13"/>
    </svg>
  );
}
function IconShield({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <path {...PW_STROKE} d="M7 1.5l5 2v3.5c0 3-2.1 5.5-5 6-2.9-0.5-5-3-5-6V3.5l5-2z"/>
      <path {...PW_STROKE} d="M5 7l1.5 1.5L9.5 5.5"/>
    </svg>
  );
}
function IconBolt({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16">
      <path {...PW_STROKE} d="M9 1.5L3.5 9h4L7 14.5 12.5 7h-4L9 1.5z"/>
    </svg>
  );
}
function IconArrowRight({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16">
      <path {...PW_STROKE} d="M3 8h10M9 4l4 4-4 4"/>
    </svg>
  );
}
function IconAlert({ size = 18 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 18 18">
      <path {...PW_STROKE} d="M9 2l7.5 13h-15L9 2z"/>
      <path {...PW_STROKE} d="M9 7v4"/>
      <circle cx="9" cy="13" r="0.5" fill="currentColor"/>
    </svg>
  );
}
function IconRefresh({ size = 16 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 16 16">
      <path {...PW_STROKE} d="M3 8a5 5 0 018.5-3.5L14 7M13 8a5 5 0 01-8.5 3.5L2 9"/>
      <path {...PW_STROKE} d="M14 3v4h-4M2 13V9h4"/>
    </svg>
  );
}
function IconDoc({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <path {...PW_STROKE} d="M3 2h5l3 3v7a1 1 0 01-1 1H3a1 1 0 01-1-1V3a1 1 0 011-1z"/>
      <path {...PW_STROKE} d="M8 2v3h3M4.5 8h5M4.5 10.5h3.5"/>
    </svg>
  );
}
function IconDownload({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <path {...PW_STROKE} d="M7 2v7M4 6l3 3 3-3M2.5 11.5h9"/>
    </svg>
  );
}
function IconTrash({ size = 14 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 14 14">
      <path {...PW_STROKE} d="M3 4h8M5.5 4V2.5h3V4M4 4l.5 8a1 1 0 001 1h3a1 1 0 001-1L10 4"/>
    </svg>
  );
}

Object.assign(window, {
  IconCheck, IconChevron, IconInfo, IconPencil, IconLock, IconPin,
  IconSolar, IconCar, IconHome, IconPeople, IconBriefcase, IconShield,
  IconBolt, IconArrowRight, IconAlert, IconRefresh, IconDoc, IconDownload, IconTrash,
});
