// Shared loading and error screens.

// Loading screen shown while the app connects.
function LoadingState({ label = 'Connecting securely', sublabel = 'This usually takes a few seconds' }) {
  return (
    <div className="pw-screen pw-fade-in" style={{
      display: 'flex', flexDirection: 'column',
      alignItems: 'center', justifyContent: 'center',
      padding: '80px 32px',
    }}>
      {/* Concentric ring loader */}
      <div style={{ position: 'relative', width: 72, height: 72 }}>
        <div style={{
          position: 'absolute', inset: 0, borderRadius: 999,
          border: '2px solid var(--cream-200)',
        }}/>
        <div style={{
          position: 'absolute', inset: 0, borderRadius: 999,
          border: '2px solid transparent',
          borderTopColor: 'var(--forest-700)',
          borderRightColor: 'var(--forest-700)',
          animation: 'spin 1.1s cubic-bezier(.5,.1,.5,.9) infinite',
        }}/>
        <div style={{
          position: 'absolute', inset: 18,
          borderRadius: 999, background: 'var(--lime-50)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          color: 'var(--ink-900)',
        }}>
          <IconBolt size={18}/>
        </div>
      </div>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>

      <div className="t-display" style={{
        marginTop: 28, fontSize: 22, color: 'var(--ink-900)', textAlign: 'center',
      }}>
        {label}
      </div>
      <div className="t-body" style={{
        marginTop: 6, fontSize: 13, color: 'var(--ink-600)', textAlign: 'center',
      }}>
        {sublabel}
      </div>

      {/* Step list showing progress */}
      <div style={{
        marginTop: 36, width: '100%', maxWidth: 260,
        display: 'flex', flexDirection: 'column', gap: 10,
      }}>
        {[
          { label: 'Reading meter credentials', state: 'done' },
          { label: 'Verifying with your provider', state: 'active' },
          { label: 'Fetching 30 days of data',  state: 'queued' },
        ].map((r, i) => (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: 10,
            fontSize: 12, color: r.state === 'queued' ? 'var(--ink-400)' : 'var(--ink-700)',
          }}>
            <div style={{
              width: 14, height: 14, borderRadius: 999,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              background: r.state === 'done' ? 'var(--forest-700)' :
                          r.state === 'active' ? 'transparent' : 'var(--cream-200)',
              border: r.state === 'active' ? '1.5px solid var(--forest-700)' : 0,
              color: '#fff',
            }}>
              {r.state === 'done' && <IconCheck size={10}/>}
              {r.state === 'active' && (
                <div style={{
                  width: 5, height: 5, borderRadius: 999, background: 'var(--forest-700)',
                  animation: 'pwPulse 1.1s ease-in-out infinite',
                }}/>
              )}
            </div>
            <span>{r.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// Error screen shown when the meter connection fails.
function ErrorState({ onRetry, onSkip }) {
  return (
    <PwScreen step={0}>
      <div style={{
        marginTop: 60, display: 'flex', flexDirection: 'column', alignItems: 'center',
        textAlign: 'center',
      }}>
        <div style={{
          width: 72, height: 72, borderRadius: 999,
          background: '#FBECE6',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          color: 'var(--error)',
        }}>
          <IconAlert size={30}/>
        </div>

        <h2 className="t-display" style={{
          fontSize: 28, lineHeight: 1.1, margin: '24px 0 10px',
          color: 'var(--ink-900)', maxWidth: 300,
        }}>
          We couldn't reach your meter.
        </h2>
        <p className="t-body" style={{
          fontSize: 14, lineHeight: 1.5, color: 'var(--ink-600)',
          margin: 0, maxWidth: 300,
        }}>
          Your provider's gateway didn't respond in time. This usually clears up in a few minutes.
        </p>

        {/* Error details box */}
        <div style={{
          marginTop: 24, width: '100%',
          padding: '12px 14px',
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          textAlign: 'left',
          display: 'grid', gridTemplateColumns: '1fr auto', gap: '6px 14px',
          fontSize: 12,
        }}>
          <span style={{ color: 'var(--ink-600)' }}>Error</span>
          <span className="t-mono" style={{ color: 'var(--ink-900)' }}>GATEWAY_504_TIMEOUT</span>
          <span style={{ color: 'var(--ink-600)' }}>Retry in</span>
          <span className="t-mono" style={{ color: 'var(--forest-700)' }}>~30s</span>
        </div>

        <div style={{ marginTop: 28, width: '100%' }}>
          <PwButton onClick={onRetry} icon={<IconRefresh size={14}/>}>
            Try again
          </PwButton>
        </div>
        <button onClick={onSkip} style={{
          marginTop: 14, appearance: 'none', background: 'transparent', border: 0,
          fontSize: 14, color: 'var(--ink-600)', cursor: 'pointer',
          fontFamily: 'var(--font-sans)',
        }}>
          Set up manually instead
        </button>
      </div>
    </PwScreen>
  );
}

Object.assign(window, { LoadingState, ErrorState });
