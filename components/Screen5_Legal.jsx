// Onboarding step 5: show the terms and ask the user to agree.
function Screen5_Legal({ onNext, onBack }) {
  const [agreed, setAgreed] = React.useState(false);
  const [openItem, setOpenItem] = React.useState(null);

  const items = [
    {
      label: 'Terms of Service',
      desc: 'How Ampeer operates and what you agree to.',
      icon: <IconDoc size={14}/>,
      content: (
        <div>
          <p style={{ marginTop: 0 }}>
            <strong>What Ampeer does.</strong> Ampeer matches households with surplus solar electricity with nearby neighbours who want cheaper and cleaner energy. We are a technology platform — not your energy supplier. Your supplier stays the same, and all energy flows through the national grid as normal.
          </p>
          <p>
            <strong>Automated trading.</strong> Once you opt in, Ampeer automatically matches your surplus or consumption with local peers. You can pause or stop at any time — no effort, no exit fees, no minimum term.
          </p>
          <p>
            <strong>Pricing.</strong> Trades settle at rates between the grid export rate and the standard import rate — sellers earn more, buyers pay less. Ampeer takes a small fee per trade only from your retailer.
          </p>
          <p>
            <strong>Regulatory basis.</strong> Ampeer operates under Ofgem's framework for peer-to-peer trading. Smart meter data is accessed via your supplier's authorised API under the Smart Energy Code (SEC). We comply with the Electricity Act 1989 and Energy Act 2023.
          </p>
          <p style={{ marginBottom: 0 }}>
            <strong>Changes to these terms.</strong> We will notify you at least 30 days before any material change. You can reject changes by closing your account at any time.
          </p>
        </div>
      ),
    },
    {
      label: 'Your data',
      desc: "What we collect, why, and where it's stored.",
      icon: <IconShield size={14}/>,
      content: (
        <div>
          <p style={{ marginTop: 0 }}>
            <strong>What we access.</strong> Your name, email, postcode, smart meter MPAN, half-hourly consumption and generation readings, and basic household profile. We do not collect financial details — billing is handled by your supplier.
          </p>
          <p>
            <strong>Why.</strong> To match you with peers, calculate savings, and settle trades. Each piece of data has a specific purpose — we do not collect anything speculatively.
          </p>
          <p>
            <strong>Lawful basis.</strong> We process data under UK GDPR Article 6(1)(b) (contract performance) for core trading, and Article 6(1)(a) (consent) for other features like community benchmarking.
          </p>
          <p>
            <strong>Storage.</strong> All data is stored in the UK, and is not transfered abroad. We never sell or share your data for marketing.
          </p>
          <p>
            <strong>Retention.</strong> Trading data is kept for 6 years after your last transaction (per HMRC requirements). If you delete your account, personal (non-trading) details are removed within 30 days.
          </p>
          <p style={{ marginBottom: 0 }}>
            <strong>Your rights.</strong> Under the UK GDPR you can access, rectify, restrict, port, or object to processing of your data. You can also complain to the ICO at <span style={{ fontWeight: 500 }}>ico.org.uk</span>.
          </p>
        </div>
      ),
    },
    {
      label: 'Right to erasure',
      desc: 'How to delete everything at any time, with no questions asked.',
      icon: <IconTrash size={14}/>,
      content: (
        <div>
          <p style={{ marginTop: 0 }}>
            <strong>Your right.</strong> Under Article 17 of the UK GDPR, you have the right to request the deletion of all personal data we hold about you. We honour this without requiring a reason.
          </p>
          <p>
            <strong>How to request it.</strong> Go to Profile → Your Data (GDPR) → Delete my account, or email <span style={{ fontWeight: 500 }}>privacy@ampeer.co.uk</span>. We will confirm receipt within 3-5 business days and complete the deletion within 30 calendar days, as required by UK GDPR Article 12(3).
          </p>
          <p>
            <strong>What gets deleted.</strong> Your name, email, postcode, meter details, household profile, and all personally identifiable details.
          </p>
          <p>
            <strong>What we may retain.</strong> We are legally required to keep certain anonymised, non-identifiable records for regulatory and tax compliance (e.g. aggregated settlement data required by Ofgem and HMRC).
          </p>
          <p style={{ marginBottom: 0 }}>
            <strong>Data portability.</strong> Before deleting your account, you can export your full trading history in a standard machine-readable format (CSV) from Profile → Your Data → Export. This is your right under Article 20 of the UK GDPR.
          </p>
        </div>
      ),
    },
  ];

  // If the user opened one of the items, show its full text instead.
  if (openItem !== null) {
    const item = items[openItem];
    return (
      <PwScreen>
        <div style={{
          position: 'sticky', top: 0, zIndex: 3,
          background: 'var(--cream-50)',
          padding: '0 0 12px',
        }}>
          <button onClick={() => setOpenItem(null)} style={{
            appearance: 'none', border: 0, background: 'transparent',
            display: 'flex', alignItems: 'center', gap: 6,
            color: 'var(--ink-700)', fontSize: 15, fontWeight: 500,
            fontFamily: 'var(--font-sans)', cursor: 'pointer',
            padding: '4px 0',
          }}>
            <IconChevron dir="left" size={16}/>
            <span>Back to consent</span>
          </button>
        </div>

        <div style={{ marginTop: 8 }}>
          <div style={{
            display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16,
          }}>
            <div style={{
              width: 32, height: 32, borderRadius: 8,
              background: 'var(--lime-50)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: 'var(--ink-900)',
            }}>{item.icon}</div>
            <h2 className="t-title" style={{
              fontSize: 22, margin: 0, color: 'var(--ink-900)',
              fontWeight: 600, letterSpacing: '-0.02em',
            }}>{item.label}</h2>
          </div>

          <div style={{
            fontSize: 14, lineHeight: 1.65, color: 'var(--ink-700)',
            fontFamily: 'var(--font-sans)',
          }}>
            {item.content}
          </div>
        </div>
      </PwScreen>
    );
  }

  return (
    <PwScreen step={4} onBack={onBack}>
      <PwPageTitle
        eyebrow="Step 4 of 4 — Consent"
        title="Terms, in plain English."
        subtitle="The short version of how your data is handled is below."
        size={32}
      />

      {/* Short reminder of why this saves money */}
      <div style={{ marginTop: 4 }}>
        <PwReassurance title="Why Ampeer saves you money.">
          Trading with peers means you buy for less and sell for more versus a standard grid tariff.
        </PwReassurance>
      </div>

      {/* Tappable list of legal sections */}
      <div className="pw-card" style={{ marginTop: 20, overflow: 'hidden' }}>
        {items.map((it, i) => (
          <button key={i} onClick={() => setOpenItem(i)} style={{
            appearance: 'none', border: 0, background: 'transparent',
            width: '100%', textAlign: 'left', cursor: 'pointer',
            padding: '14px 18px',
            display: 'flex', alignItems: 'flex-start', gap: 12,
            borderTop: i === 0 ? 0 : '1px solid var(--cream-200)',
          }}>
            <div style={{
              width: 28, height: 28, borderRadius: 8,
              background: 'var(--lime-50)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: 'var(--ink-900)', flexShrink: 0, marginTop: 2,
            }}>{it.icon}</div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: 15, color: 'var(--ink-900)', fontWeight: 500 }}>
                {it.label}
              </div>
              <div style={{ fontSize: 13, color: 'var(--ink-600)', marginTop: 2, lineHeight: 1.4 }}>
                {it.desc}
              </div>
            </div>
            <div style={{ color: 'var(--ink-400)', marginTop: 4, flexShrink: 0 }}>
              <IconChevron size={14}/>
            </div>
          </button>
        ))}
      </div>

      {/* Checkbox to agree */}
      <button
        type="button"
        onClick={() => setAgreed(a => !a)}
        style={{
          appearance: 'none', width: '100%', textAlign: 'left', cursor: 'pointer',
          marginTop: 20, display: 'flex', gap: 12, alignItems: 'flex-start',
          padding: '14px 16px',
          background: agreed ? 'var(--lime-50)' : 'var(--surface)',
          border: `1px solid ${agreed ? 'var(--lime-500)' : 'var(--cream-200)'}`,
          borderRadius: 'var(--r-md)',
          transition: 'background .18s, border-color .18s',
          fontFamily: 'var(--font-sans)',
        }}>
        <div style={{
          width: 22, height: 22, borderRadius: 6,
          border: `1.5px solid ${agreed ? 'var(--ink-900)' : 'var(--ink-300)'}`,
          background: agreed ? 'var(--ink-900)' : 'transparent',
          flexShrink: 0, marginTop: 1,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          transition: 'all .16s',
        }}>
          {agreed && <span style={{ color: 'var(--lime-400)', display: 'flex' }}><IconCheck size={14}/></span>}
        </div>
        <span style={{ fontSize: 14, lineHeight: 1.5, color: 'var(--ink-700)' }}>
          I agree to Ampeer's Terms and Privacy policy, and consent to automated trading.
        </span>
      </button>

      <div style={{ marginTop: 20 }}>
        <PwButton onClick={onNext} disabled={!agreed} icon={<IconArrowRight size={16}/>}>
          Accept & continue
        </PwButton>
      </div>
    </PwScreen>
  );
}

Object.assign(window, { Screen5_Legal });
