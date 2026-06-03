// Onboarding step 3: show the estimated yearly savings.
function Screen3_Savings({ state, onNext, onBack }) {
  const [showCalc, setShowCalc] = React.useState(false);
  const [lo, hi] = [100, 300];
  const [loAnim, setLoAnim] = React.useState(0);
  const [hiAnim, setHiAnim] = React.useState(0);

  // Animate the numbers counting up from 0 to the final amounts.
  React.useEffect(() => {
    const start = performance.now();
    const dur = 900;
    const tick = (t) => {
      const p = Math.min(1, (t - start) / dur);
      const e = 1 - Math.pow(1 - p, 3);
      setLoAnim(Math.round(lo * e));
      setHiAnim(Math.round(hi * e));
      if (p < 1) requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, []);

  return (
    <PwScreen step={2} onBack={onBack}>
      <PwPageTitle
        eyebrow="Step 2 of 4 — Your numbers"
        title="You could save..."
        subtitle={`For homes like yours in ${state.area || 'London'} with solar panels and batteries. Estimated range reflects weather, price and consumption variability.`}
        size={32}
      />
      <div>

        {/* Big savings range */}
        <div style={{ margin: '8px 0 6px' }}>
          <div style={{
            display: 'flex', alignItems: 'baseline', gap: 14,
            flexWrap: 'nowrap',
          }}>
            <span className="t-num" style={{
              fontSize: 56, color: 'var(--ink-900)',
              letterSpacing: '-0.045em', lineHeight: 0.95, fontWeight: 600,
            }}>
              <span style={{ fontSize: 28, color: 'var(--ink-400)', marginRight: 2, fontWeight: 500 }}>£</span>
              {loAnim}
            </span>
            <span style={{
              fontFamily: 'var(--font-sans)', fontSize: 16, color: 'var(--ink-400)',
              fontWeight: 500, letterSpacing: '0.02em',
            }}>to</span>
            <span className="t-num" style={{
              fontSize: 56, color: 'var(--ink-900)',
              letterSpacing: '-0.045em', lineHeight: 0.95, fontWeight: 600,
            }}>
              <span style={{ fontSize: 28, color: 'var(--ink-400)', marginRight: 2, fontWeight: 500 }}>£</span>
              {hiAnim}
            </span>
          </div>
          <div className="t-label" style={{ color: 'var(--ink-500)', marginTop: 10 }}>
            per year
          </div>
        </div>

        {/* Bar chart comparing standard tariff vs Ampeer */}
        <div style={{ marginTop: 36 }}>
          <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
            marginBottom: 10,
          }}>
            <span className="t-label" style={{ color: 'var(--ink-500)' }}>Comparison</span>
            <span style={{ fontSize: 11, color: 'var(--ink-400)', fontFamily: 'var(--font-sans)', fontWeight: 500 }}>£/year</span>
          </div>

          {[
            { label: 'Standard grid tariff with your setup', value: 800, color: 'var(--cream-200)', highlight: false },
            { label: 'With Ampeer',    value: 606, color: 'var(--lime-500)', highlight: true, savings: 'save £194' },
          ].map((row, i) => {
            const max = 800;
            const pct = (row.value / max) * 100;
            return (
              <div key={i} style={{
                display: 'grid', gridTemplateColumns: '1fr auto',
                alignItems: 'center', gap: 12, marginBottom: 10,
              }}>
                {/* Bar */}
                <div style={{
                  position: 'relative', height: 44,
                  background: 'var(--surface)',
                  border: '1px solid var(--cream-200)',
                  borderRadius: 'var(--r-md)',
                  overflow: 'hidden',
                }}>
                  <div style={{
                    position: 'absolute', top: 0, bottom: 0, left: 0,
                    width: `${pct}%`,
                    background: row.color,
                    animation: 'pwGrow 1s cubic-bezier(.22,.8,.32,1) both',
                  }}/>
                  <div style={{
                    position: 'absolute', inset: 0,
                    display: 'flex', alignItems: 'center', gap: 8,
                    padding: '0 14px',
                  }}>
                    <span style={{
                      fontSize: 14, fontWeight: 500,
                      color: row.highlight ? 'var(--forest-900)' : 'var(--ink-900)',
                      whiteSpace: 'nowrap',
                    }}>{row.label}</span>
                    {row.savings && (
                      <span style={{
                        fontFamily: 'var(--font-sans)', fontSize: 11, fontWeight: 600,
                        padding: '2px 8px',
                        background: 'var(--forest-900)',
                        color: 'var(--lime-400)',
                        borderRadius: 999,
                        whiteSpace: 'nowrap',
                        letterSpacing: '0.02em',
                      }}>{row.savings}</span>
                    )}
                  </div>
                </div>
                {/* Show the amount next to the bar */}
                <span className="t-num" style={{
                  fontSize: 20, color: 'var(--ink-900)',
                  whiteSpace: 'nowrap', minWidth: 60, textAlign: 'right',
                }}>
                  £{row.value.toLocaleString()}
                </span>
              </div>
            );
          })}

          <style>{`@keyframes pwGrow { from { width: 0; } }`}</style>
        </div>

        {/* Tap to see how the savings were worked out */}
        <button onClick={() => setShowCalc(v => !v)} style={{
          marginTop: 22,
          appearance: 'none', background: 'transparent', border: 0, padding: 0,
          display: 'inline-flex', alignItems: 'center', gap: 6,
          color: 'var(--ink-900)', fontSize: 14, fontFamily: 'var(--font-sans)',
          fontWeight: 500, cursor: 'pointer',
        }}>
          <span>How we calculated this</span>
          <div style={{
            transform: `rotate(${showCalc ? 90 : 0}deg)`,
            transition: 'transform .22s ease',
          }}>
            <IconChevron size={14}/>
          </div>
        </button>
        {showCalc && (
          <div className="pw-fade-in" style={{
            marginTop: 12, padding: '14px 16px',
            background: 'var(--surface)',
            border: '1px solid var(--cream-200)',
            borderRadius: 'var(--r-md)',
            fontSize: 13, lineHeight: 1.55, color: 'var(--ink-600)',
          }}>
            <div style={{
              display: 'grid', gridTemplateColumns: '1fr auto', gap: '8px 16px',
              fontSize: 13,
            }}>
              <span style={{ color: 'var(--ink-600)' }}>Average household bill</span>
              <span className="t-num" style={{ color: 'var(--ink-900)' }}>£1400 / year</span>
              <span style={{ color: 'var(--ink-600)' }}>Solar panel and battery savings </span>
              <span className="t-num" style={{ color: 'var(--ink-900)', fontWeight: 600 }}>£600 / year</span>
              <span style={{ color: 'var(--ink-600)' }}>Ampeer's additional savings</span>
              <span className="t-num" style={{ color: 'var(--lime-600)', fontWeight: 600 }}>24%</span>
            </div>
            <div style={{
              marginTop: 10, paddingTop: 10,
              borderTop: '1px solid var(--cream-200)',
              fontSize: 12, color: 'var(--ink-400)',
            }}>
              Data provided by British Gas and Ampeer's simulations.
            </div>
          </div>
        )}
      </div>

      {/* Main button to continue */}
      <div style={{ marginTop: 28 }}>
        <PwButton onClick={onNext} icon={<IconArrowRight size={16}/>}>
          Set up your profile
        </PwButton>
      </div>
    </PwScreen>
  );
}

function HeroRow({ prefix, value, big }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'baseline', gap: 12,
      padding: big ? '2px 0' : '2px 0 8px',
      borderBottom: big ? 0 : '1px solid var(--cream-200)',
      marginBottom: big ? 0 : 8,
    }}>
      <span className="t-label" style={{
        color: 'var(--ink-500)', width: 40, flexShrink: 0,
      }}>{prefix}</span>
      <span className="t-num" style={{
        fontSize: big ? 72 : 40,
        color: big ? 'var(--ink-900)' : 'var(--ink-600)',
        letterSpacing: '-0.045em', lineHeight: 0.95,
        fontWeight: 600,
      }}>
        <span style={{
          fontSize: big ? 32 : 20, color: 'var(--ink-400)',
          marginRight: 4, fontWeight: 500,
        }}>£</span>
        {value}
      </span>
    </div>
  );
}

Object.assign(window, { Screen3_Savings });
