import React from "react";

export function RecordingControl({ stream }: { stream: MediaStream | null }) {
  const recorder = React.useRef<MediaRecorder | null>(null);
  const chunks = React.useRef<Blob[]>([]);
  const [recording, setRecording] = React.useState(false);

  function start() {
    if (!stream || typeof MediaRecorder === "undefined") return;
    const mime = ["video/webm;codecs=vp9,opus","video/webm;codecs=vp8,opus","video/webm"].find(MediaRecorder.isTypeSupported) ?? "";
    chunks.current = [];
    const r = new MediaRecorder(stream, mime ? { mimeType: mime } : undefined);
    r.ondataavailable = e => { if (e.data.size) chunks.current.push(e.data); };
    r.onstop = () => {
      const blob = new Blob(chunks.current, { type: r.mimeType || "video/webm" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url; a.download = `yazykon-${new Date().toISOString().replace(/[:.]/g,"-")}.webm`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      chunks.current = [];
    };
    r.start(1000);
    recorder.current = r;
    setRecording(true);
  }

  function stop() { recorder.current?.stop(); recorder.current = null; setRecording(false); }

  return <button type="button" onClick={recording ? stop : start} disabled={!stream}>
    {recording ? "⏹️ Остановить запись" : "⏺️ Запись"}
  </button>;
}

export function CaptionsControl({ stream }: { stream: MediaStream | null }) {
  const recognition = React.useRef<any>(null);
  const enabledRef = React.useRef(false);
  const [enabled, setEnabled] = React.useState(false);
  const [caption, setCaption] = React.useState("");

  function toggle() {
    const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
    if (!SpeechRecognition) { setCaption("Распознавание речи не поддерживается этим браузером"); return; }
    if (enabledRef.current) { recognition.current?.stop(); recognition.current = null; enabledRef.current = false; setEnabled(false); setCaption(""); return; }
    const r = new SpeechRecognition();
    r.lang = "ru-RU"; r.continuous = true; r.interimResults = true;
    r.onresult = (event: any) => {
      let text = "";
      for (let i = event.resultIndex; i < event.results.length; i++) text += event.results[i][0].transcript;
      setCaption(text.trim());
    };
    r.onerror = () => { enabledRef.current = false; setEnabled(false); setCaption("Ошибка распознавания речи"); };
    r.onend = () => { if (enabledRef.current) { try { r.start(); } catch {} } };
    recognition.current = r; enabledRef.current = true; r.start(); setEnabled(true);
  }

  return <span className="meeting-feature">
    <button type="button" onClick={toggle}>{enabled ? "💬 Субтитры: вкл" : "💬 Субтитры"}</button>
    {caption && <span className="caption-overlay" aria-live="polite">{caption}</span>}
  </span>;
}

export function MeetingFeatureControls({ stream }: { stream: MediaStream | null }) {
  return <><RecordingControl stream={stream} /><CaptionsControl stream={stream} /></>;
}
