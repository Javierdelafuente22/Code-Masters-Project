// Runs the onboarding screens in order, then opens the main app.

function OnboardingFlow({ onComplete }) {
  const [step, setStep] = React.useState(0);
  const [state, setState] = React.useState({});

  const isMobile =
    window.matchMedia('(max-width: 600px) and (pointer: coarse)').matches ||
    window.matchMedia('(display-mode: standalone)').matches ||
    window.navigator.standalone === true;

  const goNext = () => {
    if (step >= 5) {
      onComplete && onComplete();
    } else {
      setStep(s => s + 1);
    }
  };
  const goBack = () => setStep(s => Math.max(0, s - 1));

  const screens = [
    <Screen1_Postcode key="s1" state={state} setState={setState} onNext={goNext}/>,
    <Screen2_Connected key="s2" provider="your energy provider" onNext={goNext} onBack={goBack}/>,
    <Screen3_Savings   key="s3" state={state} onNext={goNext} onBack={goBack}/>,
    <Screen4_Profile   key="s4" state={state} setState={setState} onNext={goNext} onBack={goBack}/>,
    <Screen5_Legal     key="s5" onNext={goNext} onBack={goBack}/>,
    <Screen6_AllSet    key="s6" onNext={goNext} onBack={goBack}/>,
  ];

  const content = (
    <div key={step} style={{ height: '100%' }}>
      {screens[step]}
    </div>
  );

  // On a phone, fill the whole screen. On desktop, show it inside a phone frame.
  if (isMobile) {
    return (
      <div style={{
        position: 'fixed',
        top: 0, left: 0, right: 0, bottom: 0,
        background: 'var(--cream-50, #F4F5F2)',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
      }}>
        <div style={{ flex: 1, overflow: 'hidden' }}>
          {content}
        </div>
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
        {content}
      </IOSDevice>
    </div>
  );
}

Object.assign(window, { OnboardingFlow });
