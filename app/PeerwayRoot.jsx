// Shows onboarding first, then the main app.
// Every page refresh starts again at onboarding.

function PeerwayRoot() {
  const [phase, setPhase] = React.useState(() => {
    // Clear the old flag so a refresh always begins with onboarding.
    try { sessionStorage.removeItem('pw_onboarded'); } catch (e) {}
    return 'onboarding';
  });

  const handleOnboardingComplete = () => {
    try { sessionStorage.setItem('pw_onboarded', '1'); } catch (e) {}
    setPhase('app');
  };

  if (phase === 'onboarding') {
    return <OnboardingFlow onComplete={handleOnboardingComplete}/>;
  }
  return <MainAppShell/>;
}

Object.assign(window, { PeerwayRoot });
