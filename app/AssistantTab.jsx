const nowBST = () => new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Europe/London', hour: 'numeric', minute: '2-digit', hour12: true,
}).format(new Date()).replace(' ', '').toLowerCase();

// Address of the local chat server. When it isn't running (for example on
// GitHub Pages) the app uses the built-in replies further down instead.
const CHAT_API_BASE = 'http://localhost:5000';

// A fetch that gives up after a set time so a slow server can't freeze the app.
const fetchWithTimeout = (url, opts = {}, ms = 1500) => {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), ms);
  return fetch(url, { ...opts, signal: ctrl.signal })
    .finally(() => clearTimeout(id));
};

function SmartModeButton({ onClick }) {
  const [hov, setHov] = React.useState(false);
  return (
    <button
      onClick={onClick}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        appearance: 'none', cursor: 'pointer',
        background: 'var(--ink-900)',
        border: '1.5px solid var(--ink-900)',
        padding: '7px 13px', borderRadius: 999,
        display: 'flex', alignItems: 'center', gap: 5,
        fontSize: 12, color: '#fff', fontWeight: 600,
        letterSpacing: '-0.01em', fontFamily: 'var(--font-sans)',
        transition: 'transform .15s, box-shadow .15s',
        transform: hov ? 'translateY(-2px)' : 'none',
        boxShadow: hov ? '0 6px 18px rgba(0,0,0,0.18)' : '0 1px 3px rgba(0,0,0,0.10)',
        animation: 'pwSmartAttention 1.6s ease-out 0.8s 1 both',
      }}
    >
      <IconSparkle size={12} />
      Turn on smart mode
    </button>
  );
}

// Assistant tab: the in-app chatbot that helps the user plan and trade their energy.
function AssistantTab() {
  // the chat history shown in the window, starting with two welcome messages
  const [messages, setMessages] = React.useState(() => {
    const t = nowBST();
    return [
      {
        role: 'ai', type: 'greeting',
        text: "Hi Sarah — I'm your trading agent. I keep an eye on prices and trade your surplus to earn you a bit extra. Tell me about your week and I'll plan around it.",
        ts: t,
      },
      {
        role: 'ai', type: 'smart-promo',
        text: "Or activate smart mode to connect to your calendar and location so I can trade automatically.",
        ts: t,
      },
    ];
  });
  const [input, setInput] = React.useState('');
  const [view, setView] = React.useState('chat'); // 'chat' | 'intelligence'
  const [calEnabled, setCalEnabled] = React.useState(false);
  const [locationEnabled, setLocationEnabled] = React.useState(false);
  const [listening, setListening] = React.useState(false);
  const [apiMode, setApiMode] = React.useState(false);
  const recognitionRef = React.useRef(null);
  const scrollRef = React.useRef();
  const inputRef = React.useRef();

  // Check once if the chat server is running. If it is, use it; if not,
  // stick with the built-in replies.
  React.useEffect(() => {
    let cancelled = false;
    fetchWithTimeout(`${CHAT_API_BASE}/health`, {}, 1500)
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j && j.ok) setApiMode(true); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, []);

  // Voice input: tap the mic, speak, and what you say is typed into the box.
  const speechSupported = typeof window !== 'undefined' &&
    (window.SpeechRecognition || window.webkitSpeechRecognition);

  const startListening = () => {
    if (!speechSupported) {
      alert("Voice input isn't supported in this browser. Try Safari or Chrome.");
      return;
    }
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    const rec = new SR();
    rec.lang = 'en-GB';
    rec.interimResults = true;
    rec.continuous = true;

    let finalText = '';
    rec.onresult = (e) => {
      let interim = '';
      for (let i = e.resultIndex; i < e.results.length; i++) {
        const r = e.results[i];
        if (r.isFinal) finalText += r[0].transcript + ' ';
        else interim += r[0].transcript;
      }
      setInput((finalText + interim).trim());
    };
    rec.onerror = () => setListening(false);
    rec.onend = () => {
      // The browser may stop on its own after a quiet moment, so start it
      // again if the user hasn't tapped to stop.
      if (recognitionRef.current && recognitionRef.current._keepAlive) {
        try { rec.start(); } catch (e) {}
      }
    };

    rec._keepAlive = true;
    recognitionRef.current = rec;
    setListening(true);
    rec.start();
  };

  const stopListening = () => {
    if (recognitionRef.current) {
      recognitionRef.current._keepAlive = false;
      try { recognitionRef.current.stop(); } catch (e) {}
    }
    setListening(false);
  };

  React.useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  // Grow the text box to fit whatever has been typed or spoken into it.
  React.useEffect(() => {
    if (!inputRef.current) return;
    inputRef.current.style.height = 'auto';
    inputRef.current.style.height = inputRef.current.scrollHeight + 'px';
  }, [input]);


  const prompts = [
  "Going on holiday this weekend",
  "Charge my EV by 7am"];


  // Add the user's message to the chat and work out a reply.
  const runPrompt = (text) => {
    setMessages((m) => [...m, { role: 'user', text, ts: 'now' }]);
    setInput('');
    if (inputRef.current) { inputRef.current.style.height = '36px'; }

    if (apiMode) {
      // Ask the server for a reply. If anything goes wrong, fall back to the
      // built-in replies so the user always gets an answer.
      fetchWithTimeout(`${CHAT_API_BASE}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text }),
      }, 15000)
        .then(r => r.ok ? r.json() : Promise.reject('http'))
        .then(j => {
          if (!j || !j.ok || !j.result) throw 'shape';
          const r = j.result;
          if (r.category === 'Error' || !Array.isArray(r.plan) || r.plan.length === 0) {
            setMessages((m) => [...m, {
              role: 'ai', type: 'message', ts: 'now',
              text: r.mission_check || "Sorry, I couldn't understand that."
            }]);
            return;
          }
          setMessages((m) => [...m, {
            role: 'ai', type: 'plan', ts: 'now',
            summary: r.mission_check,
            plan: r.plan,
            confirm: true,
            _payload: r,
          }]);
        })
        .catch(() => {
          setMessages((m) => [...m, aiRespond(text)]);
        });
      return;
    }

    // Built-in replies, used when the server isn't available.
    setTimeout(() => {
      const response = aiRespond(text);
      setMessages((m) => [...m, response]);
    }, 700);
  };

  // Built-in replies. Looks at the message and returns a fitting answer.
  const aiRespond = (text) => {
    const t = text.toLowerCase();

    // A plain greeting on its own, like "hi" or "hello"
    if (/^(hi|hello|hey|hiii|hellooo|hii|helloo|hiya|howdy|yo|sup|hola|good (morning|afternoon|evening|day)|what'?s up|whatsup)( there| ampeer)?[\s.,!?]*$/i.test(t)) {
      return {
        role: 'ai', type: 'message', ts: 'now',
        text: "Hi Sarah! I'm here to help with your energy. Want me to plan around a holiday, your work schedule, or EV charging? Just tell me what's coming up."
      };
    }

    // Going away or on holiday
    if (/\b(holiday|holidays|vacation|away|trip|travel|travelling|traveling|out of town|paris|spain|abroad|weekend away|going away|not home|not around|leaving town|leaving home|be away|be out|gone for|gone all|few days off|days off|week off|time off|flying|flight|airport|staying at|visiting|won't be home|won't be in|not in today|not in tomorrow|not back|back on|return on|gone until|empty house|house.?sit|nobody home|no one home|mum'?s|mom'?s|parents|friend'?s|bnb|airbnb|hotel|hostel|camping|festival|wedding|break|getaway|ski|beach|city break)\b/.test(t)) {
      return {
        role: 'ai', type: 'plan', ts: 'now',
        summary: "Got it — sounds like you'll be away. Here's my plan:",
        plan: [
        { label: 'Sell solar surplus', detail: 'Est. +£6.80' },
        { label: 'Pause EV charging', detail: 'No sessions scheduled' },
        { label: 'Hold battery at 20%', detail: 'Ready for your return' },
        { label: 'Alert if grid outage', detail: 'Via SMS' }],

        confirm: true
      };
    }

    // Work day, either at home or in the office
    if (/\b(work from home|working from home|wfh|home office|home all day|in the office|at the office|going to work|commute|commuting|workday|work schedule|office today|office tomorrow|in office|heading to work|heading in|going in|at work|working today|working tomorrow|9 to 5|nine to five|hybrid|remote|on site|on-?site|staying home|home today|home tomorrow|day off|not working|working late|early start|late start|back from work|finishing early|leaving work|leave work|start at|finish at|shifts?|night shift|morning shift)\b/.test(t)) {
      const office = /\b(office|going to work|commute|commuting|in office|heading to work|heading in|going in|at work|on site|on-?site|9 to 5|nine to five)\b/.test(t);
      if (office) {
        return {
          role: 'ai', type: 'plan', ts: 'now',
          summary: "Okay — out at the office. I'll trade while the house is empty:",
          plan: [
          { label: 'Sell midday solar to peers', detail: 'Est. +£3.40' },
          { label: 'Pause heating, keep fridge', detail: 'Sched. 9am–5pm' },
          { label: 'Pre-warm 30 min before return', detail: 'Comfort on arrival' },
          { label: 'Charge battery from cheap grid', detail: 'For evening peak' }],

          confirm: true
        };
      }
      return {
        role: 'ai', type: 'plan', ts: 'now',
        summary: "Okay — home all day. I'll optimise for your comfort and wallet:",
        plan: [
        { label: 'Power the house from solar', detail: '9am–4pm' },
        { label: 'Sell only true surplus', detail: 'Est. +£1.20' },
        { label: 'Keep battery at 80%+', detail: 'For evening peak' }],

        confirm: true
      };
    }

    // Charging the EV
    if (/\b(ev|electric vehicle|car|tesla|model [3sxy]|charge|charging|charger|plug in|plug it in|plugged in|top up|top it up|juice|range|miles|battery.?low|need.?charge|full.?charge|charge.?full|ready.?by|ready.?for|drive|driving|road trip|long drive|motorway|need the car|using the car|taking the car)\b/.test(t)) {
      // Pick out a time like "by 7am" if one was given, otherwise use 7am
      const timeMatch = t.match(/by\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/);
      const targetTime = timeMatch ? timeMatch[0].replace('by ', '') : '7am';
      return {
        role: 'ai', type: 'plan', ts: 'now',
        summary: `EV fully charged by ${targetTime} — here's how:`,
        plan: [
        { label: 'Start charging at 2am', detail: 'Cheapest green window' },
        { label: 'Use battery + cheap grid', detail: 'Est. £2.40 total' },
        { label: 'Target: 80% charge', detail: `Ready ~15 min before ${targetTime}` }],

        confirm: true
      };
    }

    // Asking what the app does or how it works
    if (/\b(what (does|is) (the |this )?(app|ampeer|it)( do)?|what'?s (the |this )?(app|ampeer|it)( do)?|what (is|'?s) (it|this|the app) for|how (does|do) (it|this|the app|ampeer) work|how (does|do) ampeer work|tell me (about|more about) (the |this )?(app|ampeer|it)|explain (the |this |it )?(app|ampeer)?|describe (the |this )?(app|ampeer|it)|how does peer.to.peer|how does p2p)\b/.test(t)) {
      return {
        role: 'ai', type: 'message', ts: 'now',
        text:
          "Ampeer helps you trade your spare solar energy with neighbours instead of selling it back to the grid for a low price. When your panels produce more than you use, Ampeer automatically sells the extra to nearby homes — better for you, cheaper for them. When the sun isn't out, you buy from neighbours at below-grid rates. Everything is automated and optimised, no effort to trade is needed from you."
      };
    }

    // Asking why the app is worth using
    if (/\b(why (is|are|would|should) (this|it|i|ampeer|the app|using ampeer)|why use (this|the app|ampeer)|how (do|does|will) (it|this|ampeer) (help|benefit) (me|us|society|the planet|the environment|the community)|how (do|will) i benefit|what'?s in it for me|what'?s the benefit|benefits? (of|for|to) (using )?(this|it|the app|ampeer|me)|is (this|it|ampeer) worth it|good for (me|society|the planet|the environment|the community)|help (the )?(environment|planet|community)|why does it benefit)\b/.test(t)) {
      return {
        role: 'ai', type: 'message', ts: 'now',
        text:
          "Three big wins.\n\n" +
          "First, your wallet. Most homes save £200–£600 a year — you sell surplus for more than the grid would pay, and buy from neighbours for less than it would charge.\n\n" +
          "Second, your community. The electricity from your roof powers a school, shop, or family on your street instead of travelling miles down high-voltage lines.\n\n" +
          "Third, your planet. Peer-to-peer trading makes solar pay off properly, which encourages more roofs to go green. Lots of small local producers is exactly what decarbonises the grid."
      };
    }

    // About energy, but not one of the cases above
    if (/\b(energy|electricity|power|solar|battery|grid|tariff|surplus|kwh|kw|sell|buy|trade|trading|peer|community|bill|saving|savings|price|cheap|peak|off-?peak)\b/.test(t)) {
      return {
        role: 'ai', type: 'message', ts: 'now',
        text: "I can help most with three things right now: holidays/time away, your work schedule, and EV charging. Try one of those and I'll plan around your energy."
      };
    }

    // Anything not about energy
    return {
      role: 'ai', type: 'message', ts: 'now',
      text: "I can only help with energy-related tasks — like managing your solar, planning around holidays or work, or scheduling EV charging. Ask me about any of those!"
    };
  };

  // Runs when the user taps Yes or Not yet on a suggested plan.
  const handleConfirm = (idx, ok) => {
    // Remember the plan being answered before the chat updates.
    const target = messages[idx];

    setMessages((m) => {
      const next = [...m];
      next[idx] = { ...next[idx], confirm: false, resolved: ok ? 'yes' : 'no' };
      if (ok) {
        next.push({
          role: 'ai', type: 'done',
          text: "Done — I'll take care of it. I'll check in if anything changes.",
          ts: 'now'
        });
      } else {
        next.push({
          role: 'ai', type: 'text',
          text: "Okay, holding off. Let me know if I can help you with anything else.",
          ts: 'now'
        });
      }
      return next;
    });

    if (ok && apiMode && target && target._payload) {
      // Tell the server the plan was accepted. We don't wait for a result.
      fetchWithTimeout(`${CHAT_API_BASE}/apply`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ payload: target._payload }),
      }, 5000).catch(() => {});
    }
  };

  // Turns on smart mode and posts a confirmation message in the chat.
  const handleEnable = ({ cal, location }) => {
    let what, benefit;
    if (cal && location) {
      what = 'calendar and location';
      benefit = "plan your trading automatically and track weather more accurately";
    } else if (cal) {
      what = 'calendar';
      benefit = "plan your trading automatically";
    } else {
      what = 'location';
      benefit = "track weather more accurately for smarter trades";
    }
    setView('chat');
    setTimeout(() => {
      setMessages(m => [...m, {
        role: 'ai', type: 'done',
        text: `Smart mode on. I've connected your ${what} and will ${benefit} — no input needed from you.`,
        ts: nowBST(),
      }]);
    }, 300);
  };

  if (view === 'intelligence') {
    return <IntelligenceScreen onBack={() => setView('chat')} onEnable={handleEnable}
      cal={calEnabled} onCalChange={setCalEnabled}
      location={locationEnabled} onLocationChange={setLocationEnabled} />;
  }

  return (
    <div className="pw-screen" style={{ display: 'flex', flexDirection: 'column' }}>
      <TabHeader
        eyebrow="Assistant"
        title="Your agent"
        />
      

      {/* Chat feed */}
      <div ref={scrollRef} className="pw-noscrollbar" style={{
        flex: 1, overflowY: 'auto', padding: '0 16px 12px',
        display: 'flex', flexDirection: 'column', gap: 10
      }}>
        {messages.map((m, i) =>
        <ChatBubble key={i} msg={m} idx={i} onConfirm={handleConfirm} onSmartMode={() => setView('intelligence')} />
        )}
      </div>

      {/* Quick suggestion buttons */}
      <div style={{
        padding: '8px 16px 0',
        display: 'flex', gap: 8, flexWrap: 'wrap'
      }}>
        {prompts.map((p) => <PromptChip key={p} label={p} onClick={() => runPrompt(p)} />)}
      </div>

      {/* Input dock */}
      <div style={{
        padding: '12px 16px 95px',
        background: 'linear-gradient(180deg, transparent, var(--cream-50) 40%)'
      }}>
        <form onSubmit={(e) => { e.preventDefault(); if (input.trim()) runPrompt(input.trim()); }}
        style={{
          display: 'flex', alignItems: 'flex-end', gap: 8,
          padding: 6, background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 20,
          boxShadow: 'var(--shadow-sm)'
        }}>
          <textarea
            ref={inputRef}
            value={input}
            rows={1}
            onChange={(e) => {
              setInput(e.target.value);
              e.target.style.height = 'auto';
              e.target.style.height = e.target.scrollHeight + 'px';
            }}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if (input.trim()) runPrompt(input.trim());
              }
            }}
            placeholder={listening ? "Listening…" : "Ask or tell me anything…"}
            style={{
              flex: 1, border: 0, background: 'transparent', outline: 'none',
              padding: '8px 10px', fontSize: 14,
              color: 'var(--ink-900)', fontFamily: 'var(--font-sans)',
              resize: 'none', overflow: 'hidden',
              lineHeight: '20px',
              boxSizing: 'border-box',
              minHeight: 36, maxHeight: 140,
            }} />
          
          {input.trim() && !listening ?
          <button type="submit" style={{
            appearance: 'none', border: 0, cursor: 'pointer',
            width: 38, height: 38, borderRadius: 999,
            background: 'var(--ink-900)', color: 'var(--lime-400)',
            display: 'flex', alignItems: 'center', justifyContent: 'center'
          }}>
              <IconSend size={16} />
            </button> :

          <button type="button"
            onClick={() => listening ? stopListening() : startListening()}
            aria-label={listening ? 'Stop listening' : 'Start voice input'}
            style={{
              appearance: 'none', border: 0, cursor: 'pointer',
              width: 38, height: 38, borderRadius: 999,
              background: listening ? 'var(--ink-900)' : 'var(--lime-500)',
              color: listening ? 'var(--lime-400)' : 'var(--ink-900)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              boxShadow: listening
                ? '0 0 0 4px rgba(0,168,98,0.25)'
                : '0 2px 8px rgba(0,168,98,0.3)',
              transition: 'all .18s ease',
              animation: listening ? 'pwMicPulse 1.2s ease-in-out infinite' : 'none'
            }}>
              <IconMic size={18} />
            </button>
          }
          <style>{`
            @keyframes pwMicPulse {
              0%, 100% { box-shadow: 0 0 0 0 rgba(0,168,98,0.45); }
              50%      { box-shadow: 0 0 0 8px rgba(0,168,98,0.0); }
            }
            @keyframes pwSmartAttention {
              0%   { box-shadow: none; }
              35%  { box-shadow: 0 0 0 4px rgba(0,0,0,0.10); }
              100% { box-shadow: none; }
            }
          `}</style>
        </form>
      </div>
    </div>);

}

function PromptChip({ label, onClick }) {
  const [hov, setHov] = React.useState(false);
  return (
    <button onClick={onClick}
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{
        appearance: 'none', cursor: 'pointer', flexShrink: 0,
        border: '1px solid var(--cream-200)',
        background: hov ? 'var(--ink-900)' : 'var(--surface)',
        color: hov ? '#fff' : 'var(--ink-700)',
        padding: '8px 12px', borderRadius: 999,
        fontSize: 12, fontWeight: 500, letterSpacing: '-0.005em',
        transition: 'background .15s, color .15s',
        fontFamily: 'var(--font-sans)',
      }}
    >{label}</button>
  );
}

function ChatBubble({ msg, idx, onConfirm, onSmartMode }) {
  if (msg.role === 'user') {
    return (
      <div style={{ display: 'flex', justifyContent: 'flex-end', paddingLeft: 40, minWidth: 0 }}>
        <div style={{
          background: 'var(--ink-900)', color: '#fff',
          padding: '10px 14px', borderRadius: '18px 18px 4px 18px',
          fontSize: 14, lineHeight: 1.4,
          maxWidth: '85%', letterSpacing: '-0.005em',
          whiteSpace: 'pre-wrap', overflowWrap: 'anywhere',
        }}>
          {msg.text}
        </div>
      </div>);

  }

  // AI
  return (
    <div style={{ display: 'flex', gap: 8, paddingRight: 40 }}>
      <div style={{
        width: 28, height: 28, borderRadius: 999,
        background: 'var(--lime-500)', color: 'var(--ink-900)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        flexShrink: 0, marginTop: 2
      }}>
        <IconSparkle size={12} />
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        {msg.type === 'plan' ?
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: '4px 18px 18px 18px',
          padding: '12px 14px'
        }}>
            <div style={{
            fontSize: 14, color: 'var(--ink-900)', lineHeight: 1.4,
            marginBottom: 10, fontWeight: 500,
            letterSpacing: '-0.005em', textWrap: 'pretty'
          }}>
              {msg.summary}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: 12 }}>
              {msg.plan.map((step, i) =>
            <div key={i} style={{
              display: 'flex', alignItems: 'flex-start', gap: 8,
              padding: '8px 10px',
              background: 'var(--cream-50)',
              borderRadius: 8
            }}>
                  <div style={{
                width: 18, height: 18, borderRadius: 999,
                background: 'var(--ink-900)', color: '#fff',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 10, fontWeight: 600,
                flexShrink: 0, fontFamily: 'var(--font-sans)'
              }}>
                    {i + 1}
                  </div>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 13, color: 'var(--ink-900)', fontWeight: 500, letterSpacing: '-0.005em' }}>
                      {step.label}
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--ink-600)', marginTop: 1 }}>
                      {step.detail}
                    </div>
                  </div>
                </div>
            )}
            </div>
            {msg.confirm ?
          <div style={{ display: 'flex', gap: 8 }}>
                <button onClick={() => onConfirm(idx, true)} style={{
              flex: 1, appearance: 'none', border: 0, cursor: 'pointer',
              height: 38, borderRadius: 10,
              background: 'var(--ink-900)', color: '#fff',
              fontSize: 13, fontWeight: 600,
              fontFamily: 'var(--font-sans)',
              letterSpacing: '-0.005em'
            }}>
                  Yes, do it
                </button>
                <button onClick={() => onConfirm(idx, false)} style={{
              flex: 1, appearance: 'none',
              border: '1px solid var(--cream-200)',
              background: 'var(--surface)',
              cursor: 'pointer', height: 38, borderRadius: 10,
              color: 'var(--ink-700)',
              fontSize: 13, fontWeight: 500,
              fontFamily: 'var(--font-sans)',
              letterSpacing: '-0.005em'
            }}>
                  Not yet
                </button>
              </div> :
          msg.resolved === 'yes' ?
          <div style={{
            padding: '6px 10px', borderRadius: 8,
            background: 'var(--lime-50)', color: 'var(--lime-600)',
            fontSize: 11, fontWeight: 600, letterSpacing: '0.02em',
            fontFamily: 'var(--font-sans)', textTransform: 'uppercase',
            display: 'inline-flex', alignItems: 'center', gap: 6
          }}>
                <IconCheck size={12} /> CONFIRMED
              </div> :
          null}
          </div> :

        msg.type === 'smart-promo' ? (
          <div style={{
            background: 'var(--surface)',
            border: '1px solid var(--cream-200)',
            borderRadius: '4px 18px 18px 18px',
            padding: '10px 14px',
          }}>
            <div style={{
              fontSize: 14, color: 'var(--ink-900)', lineHeight: 1.4,
              letterSpacing: '-0.005em', textWrap: 'pretty', marginBottom: 10,
            }}>
              {msg.text}
            </div>
            <SmartModeButton onClick={onSmartMode} />
          </div>
        ) : (
          <div style={{
            background: msg.type === 'done' ? 'var(--lime-50)' : 'var(--surface)',
            border: '1px solid ' + (msg.type === 'done' ? 'var(--lime-100)' : 'var(--cream-200)'),
            borderRadius: '4px 18px 18px 18px',
            padding: '10px 14px',
            fontSize: 14, color: 'var(--ink-900)', lineHeight: 1.45,
            letterSpacing: '-0.005em', textWrap: 'pretty',
            whiteSpace: 'pre-wrap',
          }}>
            {msg.text}
          </div>
        )
        }
        <div style={{
          fontSize: 10, color: 'var(--ink-400)', marginTop: 4,
          fontFamily: 'var(--font-mono)', letterSpacing: '0.02em'
        }}>
          {msg.ts}
        </div>
      </div>
    </div>);

}

// The smart mode screen, explaining what gets connected and asking permission.
function IntelligenceScreen({ onBack, onEnable, cal, onCalChange, location, onLocationChange }) {
  const anyOn = cal || location;

  return (
    <div className="pw-screen">
      <div style={{ marginBottom: 58 }}/>
      <div style={{
        padding: '16px 24px 0',
        background: 'var(--cream-50)',
        position: 'sticky', top: 0, zIndex: 2
      }}>
        <button onClick={onBack} style={{
          appearance: 'none', border: 0, background: 'transparent',
          padding: 0, color: 'var(--ink-600)',
          display: 'flex', alignItems: 'center', gap: 4,
          fontSize: 13, cursor: 'pointer', marginBottom: 28
        }}>
          <IconChevron size={14} dir="left" />
          <span>Back to assistant</span>
        </button>

        <h1 className="t-title" style={{
          fontSize: 28, lineHeight: 1.08, margin: '0 0 12px',
          color: 'var(--ink-900)', fontWeight: 600, letterSpacing: '-0.025em'
        }}>
          Upgrade intelligence.
        </h1>
      </div>

      <div style={{ padding: '0 24px 120px' }}>
        <p style={{
          fontSize: 14, color: 'var(--ink-700)', lineHeight: 1.55,
          marginBottom: 22, textWrap: 'pretty'
        }}>Connect to your apps so I can plan ahead — selling more energy when you're away and charging your EV before you need it.

        </p>

        {/* Permission toggles */}
        <div style={{
          background: 'var(--surface)',
          border: '1px solid var(--cream-200)',
          borderRadius: 'var(--r-md)',
          marginBottom: 20, overflow: 'hidden'
        }}>
          <IntelToggle
            title="Calendar"
            detail="I'll only scan for meeting events. I'll never read your meeting notes."
            on={cal} onChange={onCalChange} />

          <div style={{ borderTop: '1px solid var(--cream-200)' }} />
          <IntelToggle
            title="Location"
            detail="I'll only use your location for more accurate weather tracking."
            on={location} onChange={onLocationChange} />

        </div>

        {/* What we won't do */}
        <div style={{
          background: 'var(--ink-900)', color: '#fff',
          borderRadius: 'var(--r-md)',
          padding: 18, marginBottom: 20
        }}>
          <div style={{
            display: 'flex', alignItems: 'center', gap: 8, marginBottom: 14
          }}>
            <div style={{
              width: 28, height: 28, borderRadius: 999,
              background: 'rgba(0,192,111,0.20)', color: 'var(--lime-400)',
              display: 'flex', alignItems: 'center', justifyContent: 'center'
            }}>
              <IconShield size={14} />
            </div>
            <div style={{ fontSize: 14, fontWeight: 600 }}>What we won't do</div>
          </div>
          <ul style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 10 }}>
            {[
            'Store anything beyond travel events',
            'Use your location for anything beyond weather',
            'Share your schedule with other users',
            'Sell or share your data with advertisers'].
            map((t) =>
            <li key={t} style={{
              display: 'flex', alignItems: 'flex-start', gap: 10,
              fontSize: 13, color: 'rgba(255,255,255,0.85)', lineHeight: 1.45
            }}>
                <span style={{
                width: 16, height: 16, borderRadius: 999,
                background: 'rgba(255,255,255,0.08)',
                display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                flexShrink: 0, marginTop: 1, color: 'rgba(255,255,255,0.5)'
              }}>
                  <svg width="8" height="8" viewBox="0 0 8 8" fill="currentColor"><path d="M1 1l6 6M7 1l-6 6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>
                </span>
                {t}
              </li>
            )}
          </ul>
        </div>

        <button disabled={!anyOn} onClick={() => anyOn && onEnable({ cal, location })}
          className="pw-btn pw-btn-primary" style={{
          width: '100%', height: 52,
          opacity: anyOn ? 1 : 0.4,
          cursor: anyOn ? 'pointer' : 'not-allowed'
        }}>Enable smarter intelligence
        </button>
        <div style={{
          fontSize: 11, color: 'var(--ink-400)', textAlign: 'center',
          marginTop: 10, letterSpacing: '-0.005em'
        }}>
          Revocable anytime from here or Profile → Pause Trading
        </div>
      </div>
    </div>);

}

function IntelToggle({ title, detail, on, onChange }) {
  return (
    <button onClick={() => onChange(!on)} style={{
      appearance: 'none', border: 0, background: 'transparent',
      width: '100%', padding: '14px 16px', cursor: 'pointer',
      display: 'flex', alignItems: 'center', gap: 12, textAlign: 'left'
    }}>
      <div style={{ flex: 1 }}>
        <div style={{ fontSize: 14, color: 'var(--ink-900)', fontWeight: 600, letterSpacing: '-0.005em' }}>
          {title}
        </div>
        <div style={{ fontSize: 12, color: 'var(--ink-600)', marginTop: 2, lineHeight: 1.4 }}>
          {detail}
        </div>
      </div>
      <div style={{
        width: 42, height: 24, borderRadius: 999,
        background: on ? 'var(--lime-500)' : 'var(--cream-200)',
        position: 'relative', flexShrink: 0,
        transition: 'background .18s'
      }}>
        <div style={{
          position: 'absolute', top: 2, left: on ? 20 : 2,
          width: 20, height: 20, borderRadius: 999,
          background: '#fff', boxShadow: '0 1px 3px rgba(0,0,0,0.2)',
          transition: 'left .18s'
        }} />
      </div>
    </button>);

}

Object.assign(window, { AssistantTab });