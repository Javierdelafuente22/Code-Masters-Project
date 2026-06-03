// Profile tab: shows the user's account details and settings like trading,
// notifications, data export, support, and deleting the account.
function ProfileTab() {
  const [paused, setPaused] = React.useState(false);
  const [digest, setDigest] = React.useState(true);
  const [goodDay, setGoodDay] = React.useState(true);
  const [delConfirm, setDelConfirm] = React.useState(0);
  const [openPanel, setOpenPanel] = React.useState(null);
  const [supportMsg, setSupportMsg] = React.useState('');
  const [supportSent, setSupportSent] = React.useState(false);
  const [csvOpen, setCsvOpen] = React.useState(false);

  // Builds the trading history as a CSV file and saves it to the device.
  const downloadCsv = () => {
    const header = 'Date,Time,Type,kWh,Rate (p/kWh),Counterparty,Grid Equiv Rate (p/kWh),Saving (£),CO2 Saved (kg)';
    const rows = [
      '2026-04-28,10:30,Export,2.4,18.5,Peer #12,5.5,0.31,0.98',
      '2026-04-28,11:00,Export,1.8,18.5,Peer #07,5.5,0.23,0.74',
      '2026-04-28,14:15,Export,3.1,17.2,Peer #23,5.5,0.36,1.27',
      '2026-04-27,09:45,Export,1.2,18.5,Peer #12,5.5,0.16,0.49',
      '2026-04-27,12:00,Export,2.8,17.8,Peer #31,5.5,0.34,1.15',
      '2026-04-27,16:30,Import,1.5,22.4,Peer #09,24.0,0.02,0.00',
      '2026-04-26,10:00,Export,3.6,18.5,Peer #07,5.5,0.47,1.47',
      '2026-04-26,13:30,Export,2.2,17.2,Peer #45,5.5,0.26,0.90',
      '2026-04-25,11:15,Export,1.9,18.5,Peer #12,5.5,0.25,0.78',
      '2026-04-25,14:00,Export,2.7,17.8,Peer #23,5.5,0.33,1.10',
      '2026-04-25,17:45,Import,2.1,21.8,Peer #31,24.0,0.05,0.00',
      '2026-04-24,09:30,Export,3.4,18.5,Peer #09,5.5,0.44,1.39',
      '2026-04-24,12:45,Export,1.6,17.2,Peer #07,5.5,0.19,0.65',
      '2026-04-23,10:15,Export,2.9,18.5,Peer #45,5.5,0.38,1.19',
      '2026-04-23,15:00,Import,1.8,22.4,Peer #12,24.0,0.03,0.00',
      '2026-04-22,11:00,Export,2.5,17.8,Peer #23,5.5,0.31,1.02',
      '2026-04-22,13:30,Export,3.0,18.5,Peer #31,5.5,0.39,1.23',
      '2026-04-21,10:00,Export,1.4,17.2,Peer #09,5.5,0.16,0.57',
      '2026-04-21,14:30,Export,2.6,18.5,Peer #07,5.5,0.34,1.06',
      '2026-04-21,18:00,Import,1.3,21.8,Peer #45,24.0,0.03,0.00',
    ];
    const csv = header + '\n' + rows.join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'ampeer-trading-history.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  // Content for the detail screens opened from the Support section.
  const panels = {
    export: {
      title: 'Export my data',
      icon: <IconDownload size={14}/>,
      content: (
        <div>
          <p style={{ marginTop: 0 }}>
            Under Article 20 of the UK GDPR, you have the right to receive your personal data in a portable format. Your export will include:
          </p>
          <p>
            <strong>Trading history</strong> — every peer trade, including timestamps, kWh amounts, rates, and counterparties (anonymised).
          </p>
          <p>
            <strong>Consumption data</strong> — half-hourly smart meter readings as processed by Ampeer.
          </p>
          <p>
            <strong>Savings records</strong> — weekly and monthly summaries, including grid-comparison calculations.
          </p>
          <p style={{ marginBottom: 16 }}>
            Files are delivered as CSV, ready to open in Excel or import into another service. The export typically takes a few seconds.
          </p>
          <button className="pw-btn" style={{ height: 48, fontSize: 14 }} onClick={() => setCsvOpen(true)}>
            <IconDownload size={14}/>
            <span>Download my data (.csv)</span>
          </button>
        </div>
      ),
    },
    help: {
      title: 'Help centre',
      icon: <IconDoc size={14}/>,
      content: (
        <div>
          <p style={{ marginTop: 0, fontWeight: 600, color: 'var(--ink-900)' }}>
            Frequently asked questions (FAQs)
          </p>
          <p>
            <strong>Can I choose who I trade with?</strong> No. All peers are anonymised — you never see names or addresses. Ampeer matches you with the nearest available peers to minimise grid losses and maximise savings.
          </p>
          <p>
            <strong>What happens during a power cut?</strong> Nothing changes. Supply is managed by your DNO and supplier. Ampeer only handles the commercial layer.
          </p>
          <p>
            <strong>Can the app be adversarily gamed?</strong> The algorithm is not accessible from the app, making manipulation highly unlikely. Moreover, trading automatically stops if you become worse-off versus grid tariffs.
          </p>
          <p>
            <strong>What's coming next?</strong> We aim to allow you to maximise green energy mix, not just price. We also aim to add preferences for trading with specific infrastructure types (schools, community buildings, local businesses), while keeping data private.
          </p>
          <p style={{ marginBottom: 0 }}>
            <strong>How do I delete my account?</strong> Scroll to the bottom of your Profile and tap "Delete my account." Data is erased within 30 days.
          </p>
        </div>
      ),
    },
    contact: {
      title: 'Contact support',
      icon: <IconExternal size={12}/>,
      content: (
        <div>
          <p style={{ marginTop: 0 }}>
            Real humans in the UK. We typically reply within 4 hours during business days (Mon–Fri, 9am–6pm).
          </p>
          <div style={{ marginTop: 8 }}>
            <label className="t-label" style={{ display: 'block', color: 'var(--ink-400)', marginBottom: 8, fontSize: 11 }}>
              Your message
            </label>
            <textarea
              value={supportMsg}
              onChange={e => setSupportMsg(e.target.value)}
              placeholder="Describe your issue or question..."
              rows={4}
              style={{
                width: '100%', padding: '12px 14px',
                borderRadius: 'var(--r-md)',
                border: '1px solid var(--cream-200)',
                fontFamily: 'var(--font-sans)',
                fontSize: 14, color: 'var(--ink-900)',
                resize: 'vertical', outline: 'none',
                boxSizing: 'border-box',
                background: '#fff',
              }}
            />
            <button
              className="pw-btn"
              style={{ height: 48, fontSize: 14, marginTop: 12 }}
              disabled={!supportMsg.trim() || supportSent}
              onClick={() => setSupportSent(true)}
            >
              {supportSent ? (
                <React.Fragment>
                  <IconCheck size={14}/>
                  <span>Message sent</span>
                </React.Fragment>
              ) : (
                <span>Send message</span>
              )}
            </button>
            {supportSent && (
              <p className="pw-fade-in" style={{
                fontSize: 12, color: 'var(--ink-600)', marginTop: 10, lineHeight: 1.5,
              }}>
                We've received your message and will reply to your registered email within 4 hours.
              </p>
            )}
          </div>
        </div>
      ),
    },
  };

  if (csvOpen) return <CsvViewer onBack={() => setCsvOpen(false)} onDownload={downloadCsv} />;

  // Show a detail screen when one of the support items is tapped.
  if (openPanel && panels[openPanel]) {
    const panel = panels[openPanel];
    return (
      <div className="pw-screen">
        <TabHeader eyebrow="Profile" title={panel.title}/>
        <div style={{ padding: '0 24px 120px' }}>
          <button onClick={() => { setOpenPanel(null); setSupportSent(false); setSupportMsg(''); }} style={{
            appearance: 'none', border: 0, background: 'transparent',
            display: 'flex', alignItems: 'center', gap: 6,
            color: 'var(--ink-700)', fontSize: 15, fontWeight: 500,
            fontFamily: 'var(--font-sans)', cursor: 'pointer',
            padding: '4px 0', marginBottom: 16,
          }}>
            <IconChevron dir="left" size={16}/>
            <span>Back to profile</span>
          </button>

          <div style={{
            fontSize: 14, lineHeight: 1.65, color: 'var(--ink-700)',
            fontFamily: 'var(--font-sans)',
          }}>
            {panel.content}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="pw-screen">
      <TabHeader eyebrow="Profile" title="Account & data"/>

      <div style={{ padding: '0 24px 120px' }}>
        {/* User card */}
        <SectionLabel>Your details</SectionLabel>
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          overflow: 'hidden',
          marginBottom: 20,
        }}>
          <div style={{
            background: 'var(--lime-50)',
            padding: '18px 18px 16px',
            color: 'var(--ink-900)',
            display: 'flex', alignItems: 'center', gap: 14,
            borderBottom: '1px solid var(--lime-100)',
          }}>
            <div style={{
              width: 44, height: 44, borderRadius: 999,
              background: 'var(--lime-500)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: 'var(--ink-900)', fontWeight: 600, fontSize: 16,
            }}>SC</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 15, fontWeight: 600, color: 'var(--ink-900)', letterSpacing: '-0.005em' }}>Sarah Chen</div>
              <div style={{ fontSize: 12, color: 'var(--ink-600)' }}>
                SW6 3JD · London · Member since Apr 2026
              </div>
            </div>
            <div style={{
              padding: '4px 8px', borderRadius: 999,
              background: 'var(--ink-900)',
              color: 'var(--lime-400)',
              fontSize: 11, letterSpacing: '0.05em',
              textTransform: 'uppercase',
              fontWeight: 600,
            }}>
              Prosumer
            </div>
          </div>
          <ProfileRow icon={<IconHome size={14}/>} label="Household"    value="2 adults, 1 EV"/>
          <ProfileRow icon={<IconSolar size={14}/>} label="Solar"        value="4.2kW array · Fronius" last/>
        </div>

        {/* Trading status */}
        <SectionLabel>Trading</SectionLabel>
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          overflow: 'hidden',
          marginBottom: 20,
        }}>
          <ConnectionRow
            title="Smart meter"
            subtitle="Octopus Energy · IHD-5B62"
            status="connected"
          />
          <ConnectionRow
            title="Tariff"
            subtitle="Octopus Agile · Variable · £0.24/kWh avg"
            status="info"
            last={!paused}
          />
          <div style={{
            borderTop: '1px solid var(--cream-200)',
            padding: '14px 18px',
            display: 'flex', alignItems: 'center', gap: 12,
          }}>
            <div style={{
              width: 34, height: 34, borderRadius: 10,
              background: paused ? 'var(--cream-100)' : 'var(--lime-50)',
              color: paused ? 'var(--ink-600)' : 'var(--lime-600)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              flexShrink: 0,
            }}>
              <IconPause size={12}/>
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 500, letterSpacing: '-0.005em' }}>
                {paused ? 'Trading paused' : 'Actively trading'}
              </div>
              <div style={{ fontSize: 12, color: 'var(--ink-600)', marginTop: 1 }}>
                {paused ? 'You\'ll use the grid as normal' : 'Your surplus is being sold to peers'}
              </div>
            </div>
            <Toggle on={!paused} onChange={v => setPaused(!v)}/>
          </div>
        </div>

        {/* Notifications */}
        <SectionLabel>Notifications</SectionLabel>
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          overflow: 'hidden',
          marginBottom: 20,
        }}>
          <ToggleRow
            icon="📩"
            title="Weekly digest"
            detail="Sunday, 8am — what you saved last week"
            on={digest} onChange={setDigest}
          />
          <ToggleRow
            icon="☀️"
            title="Good day alerts"
            detail="Push when you crack your own savings record"
            on={goodDay} onChange={setGoodDay}
            last
          />
        </div>

        {/* Support */}
        <SectionLabel>Support</SectionLabel>
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          overflow: 'hidden',
          marginBottom: 20,
        }}>
          <LinkRow icon={<IconDownload size={14}/>} title="Export my data" detail="Download everything as CSV" onClick={() => setOpenPanel('export')}/>
          <LinkRow icon={<IconDoc size={14}/>} title="Help centre" detail="Guides, FAQs, troubleshooting" onClick={() => setOpenPanel('help')}/>
          <LinkRow icon={<IconExternal size={12}/>} title="Contact support" detail="Real humans · reply within 4h" onClick={() => setOpenPanel('contact')} last/>
        </div>

        {/* Danger zone */}
        <SectionLabel danger>Danger zone</SectionLabel>
        <div style={{
          background: 'var(--surface)',
          border: '1px solid ' + (delConfirm > 0 ? '#D4524B' : 'var(--cream-200)'),
          borderRadius: 'var(--r-md)',
          padding: 16, transition: 'border-color .2s',
        }}>
          <div style={{
            fontSize: 14, color: 'var(--ink-900)', fontWeight: 600,
            marginBottom: 4, letterSpacing: '-0.005em',
          }}>
            Delete my account
          </div>
          <div style={{
            fontSize: 12, color: 'var(--ink-600)',
            lineHeight: 1.5, marginBottom: 14, textWrap: 'pretty',
          }}>
            {delConfirm === 0 && 'Trading stops, your data is erased within 30 days, and your community share is reallocated. This cannot be undone.'}
            {delConfirm === 1 && 'Really sure? Trading will end and your account will be deleted. Tap again to confirm.'}
            {delConfirm === 2 && 'Request submitted. Your account will be deleted within 30 days. We\'ve sent an email.'}
          </div>
          {delConfirm < 2 ? (
            <button onClick={() => setDelConfirm(delConfirm + 1)} style={{
              appearance: 'none', border: '1px solid #D4524B',
              background: delConfirm === 1 ? '#D4524B' : 'transparent',
              color: delConfirm === 1 ? '#fff' : '#D4524B',
              padding: '10px 14px', borderRadius: 10,
              fontSize: 13, fontWeight: 600, cursor: 'pointer',
              fontFamily: 'var(--font-sans)',
              letterSpacing: '-0.005em',
              display: 'flex', alignItems: 'center', gap: 8,
            }}>
              <IconTrash size={12}/>
              {delConfirm === 0 ? 'Delete my account' : 'Yes, delete everything'}
            </button>
          ) : (
            <div style={{
              display: 'inline-flex', alignItems: 'center', gap: 6,
              padding: '6px 10px', borderRadius: 8,
              background: 'var(--cream-100)', color: 'var(--ink-700)',
              fontSize: 11, fontWeight: 600, letterSpacing: '0.02em',
              fontFamily: 'var(--font-sans)',
            }}>
              <IconCheck size={12}/>
              REQUEST RECEIVED
            </div>
          )}
        </div>

        <div style={{
          marginTop: 24, fontSize: 10,
          color: 'var(--ink-400)',
          textAlign: 'center',
          fontFamily: 'var(--font-sans)',
          letterSpacing: '0.05em',
        }}>
          AMPEER v2.4.1 · BUILD 2026.04.19
        </div>
      </div>
    </div>
  );
}

function SectionLabel({ children, danger }) {
  return (
    <div className="t-label" style={{
      color: danger ? '#D4524B' : 'var(--ink-500)',
      marginBottom: 8, marginTop: 4,
      padding: '0 4px',
      fontSize: 13,
    }}>
      {children}
    </div>
  );
}

function ProfileRow({ icon, label, value, last }) {
  return (
    <div style={{
      padding: '14px 18px',
      display: 'flex', alignItems: 'center', gap: 12,
      borderTop: '1px solid var(--cream-200)',
    }}>
      <div style={{
        width: 28, height: 28, borderRadius: 8,
        background: 'var(--cream-100)', color: 'var(--ink-600)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        flexShrink: 0,
      }}>
        {icon}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div className="t-label" style={{ color: 'var(--ink-400)', marginBottom: 2 }}>
          {label}
        </div>
        <div style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 500, letterSpacing: '-0.005em' }}>
          {value}
        </div>
      </div>
    </div>
  );
}

function ConnectionRow({ title, subtitle, status, last }) {
  const dot = status === 'connected' ? 'var(--lime-500)' : 'var(--ink-400)';
  return (
    <div style={{
      padding: '14px 18px',
      display: 'flex', alignItems: 'center', gap: 12,
      borderBottom: last ? 0 : '1px solid var(--cream-200)',
    }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 3 }}>
          <span style={{
            width: 6, height: 6, borderRadius: 999, background: dot,
          }}/>
          <span className="t-label" style={{ color: 'var(--ink-400)' }}>{title}</span>
        </div>
        <div style={{ fontSize: 13, color: 'var(--ink-900)', fontWeight: 500, letterSpacing: '-0.005em' }}>
          {subtitle}
        </div>
      </div>
    </div>
  );
}

function ToggleRow({ icon, title, detail, on, onChange, last }) {
  return (
    <div style={{
      padding: '14px 18px',
      display: 'flex', alignItems: 'center', gap: 12,
      borderBottom: last ? 0 : '1px solid var(--cream-200)',
    }}>
      <div style={{
        width: 32, height: 32, borderRadius: 10,
        background: 'var(--cream-100)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 14, flexShrink: 0,
      }}>
        {icon}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 500, letterSpacing: '-0.005em' }}>
          {title}
        </div>
        <div style={{ fontSize: 12, color: 'var(--ink-600)', marginTop: 1 }}>
          {detail}
        </div>
      </div>
      <Toggle on={on} onChange={onChange}/>
    </div>
  );
}

function LinkRow({ icon, title, detail, last, onClick }) {
  return (
    <button onClick={onClick} style={{
      appearance: 'none', border: 0, background: 'transparent',
      width: '100%', padding: '14px 18px',
      display: 'flex', alignItems: 'center', gap: 12,
      textAlign: 'left', cursor: 'pointer',
      borderBottom: last ? 0 : '1px solid var(--cream-200)',
      fontFamily: 'var(--font-sans)',
    }}>
      <div style={{
        width: 32, height: 32, borderRadius: 10,
        background: 'var(--cream-100)', color: 'var(--ink-600)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        flexShrink: 0,
      }}>
        {icon}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 500, letterSpacing: '-0.005em' }}>
          {title}
        </div>
        <div style={{ fontSize: 12, color: 'var(--ink-600)', marginTop: 1 }}>
          {detail}
        </div>
      </div>
      <IconChevron size={14}/>
    </button>
  );
}

function Toggle({ on, onChange }) {
  return (
    <button onClick={() => onChange(!on)} style={{
      appearance: 'none', border: 0, cursor: 'pointer',
      width: 42, height: 24, borderRadius: 999,
      background: on ? 'var(--lime-500)' : 'var(--cream-200)',
      position: 'relative', flexShrink: 0,
      transition: 'background .18s',
    }}>
      <div style={{
        position: 'absolute', top: 2, left: on ? 20 : 2,
        width: 20, height: 20, borderRadius: 999,
        background: '#fff', boxShadow: '0 1px 3px rgba(0,0,0,0.2)',
        transition: 'left .18s',
      }}/>
    </button>
  );
}

// Shows the trading history in a table, with a button to download it.
function CsvViewer({ onBack, onDownload }) {
  const isMobile =
    window.matchMedia('(max-width: 600px) and (pointer: coarse)').matches ||
    window.matchMedia('(display-mode: standalone)').matches ||
    window.navigator.standalone === true;

  const headers = ['Date', 'Time', 'Type', 'kWh', 'Rate (p)', 'Peer', 'Saving (£)', 'CO₂ (kg)'];
  const rows = [
    ['2026-04-28','10:30','Export','2.4','18.5','#12','0.31','0.98'],
    ['2026-04-28','11:00','Export','1.8','18.5','#07','0.23','0.74'],
    ['2026-04-28','14:15','Export','3.1','17.2','#23','0.36','1.27'],
    ['2026-04-27','09:45','Export','1.2','18.5','#12','0.16','0.49'],
    ['2026-04-27','12:00','Export','2.8','17.8','#31','0.34','1.15'],
    ['2026-04-27','16:30','Import','1.5','22.4','#09','0.02','—'],
    ['2026-04-26','10:00','Export','3.6','18.5','#07','0.47','1.47'],
    ['2026-04-26','13:30','Export','2.2','17.2','#45','0.26','0.90'],
    ['2026-04-25','11:15','Export','1.9','18.5','#12','0.25','0.78'],
    ['2026-04-25','14:00','Export','2.7','17.8','#23','0.33','1.10'],
    ['2026-04-25','17:45','Import','2.1','21.8','#31','0.05','—'],
    ['2026-04-24','09:30','Export','3.4','18.5','#09','0.44','1.39'],
    ['2026-04-24','12:45','Export','1.6','17.2','#07','0.19','0.65'],
    ['2026-04-23','10:15','Export','2.9','18.5','#45','0.38','1.19'],
    ['2026-04-23','15:00','Import','1.8','22.4','#12','0.03','—'],
    ['2026-04-22','11:00','Export','2.5','17.8','#23','0.31','1.02'],
    ['2026-04-22','13:30','Export','3.0','18.5','#31','0.39','1.23'],
    ['2026-04-21','10:00','Export','1.4','17.2','#09','0.16','0.57'],
    ['2026-04-21','14:30','Export','2.6','18.5','#07','0.34','1.06'],
    ['2026-04-21','18:00','Import','1.3','21.8','#45','0.03','—'],
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', background: 'var(--cream-50)' }}>
      {/* Header */}
      <div style={{
        padding: `${isMobile ? 26 : 66}px 20px 16px`,
        background: 'var(--cream-50)',
        borderBottom: '1px solid var(--cream-200)',
        display: 'flex', alignItems: 'center', gap: 8,
        flexShrink: 0,
      }}>
        <button onClick={onBack} style={{
          appearance: 'none', border: 0, background: 'transparent',
          display: 'flex', alignItems: 'center', gap: 4,
          color: 'var(--ink-600)', fontSize: 13, cursor: 'pointer', padding: 0,
          fontFamily: 'var(--font-sans)',
        }}>
          <IconChevron size={14} dir="left"/>
          <span>Back</span>
        </button>
        <div style={{
          flex: 1, textAlign: 'center',
          fontSize: 14, fontWeight: 600, color: 'var(--ink-900)',
          letterSpacing: '-0.005em',
        }}>Trading data</div>
        <button onClick={onDownload} style={{
          appearance: 'none', border: 0, background: 'transparent',
          display: 'flex', alignItems: 'center', gap: 4,
          color: 'var(--ink-600)', fontSize: 13, cursor: 'pointer', padding: 0,
          fontFamily: 'var(--font-sans)',
        }}>
          <IconDownload size={16}/>
        </button>
      </div>

      {/* Table */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <table style={{ borderCollapse: 'collapse', fontSize: 12, fontFamily: 'var(--font-sans)', width: '100%' }}>
          <thead>
            <tr style={{ background: 'var(--cream-100)', position: 'sticky', top: 0 }}>
              {headers.map(h => (
                <th key={h} style={{
                  padding: '8px 10px', textAlign: 'left',
                  borderBottom: '1px solid var(--cream-200)',
                  color: 'var(--ink-500)', fontWeight: 500,
                  whiteSpace: 'nowrap', fontSize: 11,
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--cream-200)', background: i % 2 === 0 ? 'var(--surface)' : 'var(--cream-50)' }}>
                {row.map((cell, j) => (
                  <td key={j} style={{
                    padding: '8px 10px', color: 'var(--ink-900)',
                    whiteSpace: 'nowrap',
                    color: cell === 'Import' ? 'var(--ink-500)' : cell === 'Export' ? 'var(--lime-600)' : 'var(--ink-900)',
                  }}>{cell}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

    </div>
  );
}

Object.assign(window, { ProfileTab });
