import React from "react";

interface RoomProps { roomId: string; }
type SignalMessage =
  | { type: "joined"; roomId: string; peerId: string; data?: { peers: string[]; tracks?: number } }
  | { type: "peer-joined"; peerId: string }
  | { type: "peer-left"; peerId: string }
  | { type: "track-published"; peerId: string; data?: { trackId?: string } }
  | { type: "track-removed"; peerId: string; data?: { trackId?: string } }
  | { type: "offer"; data: RTCSessionDescriptionInit }
  | { type: "answer"; data: RTCSessionDescriptionInit }
  | { type: "ice"; data: RTCIceCandidateInit }
  | { type: "error"; error?: string; data?: { code?: string } };

function mediaUrl() {
  const configured = import.meta.env.VITE_MEDIA_URL;
  if (configured) {
    const url = new URL(configured, window.location.origin);
    const basePath = url.pathname.replace(/\/$/, "");
    return `${url.protocol === "https:" ? "wss:" : "ws:"}//${url.host}${basePath}/ws`;
  }
  return `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.hostname}:4000/ws`;
}

export function Room({ roomId }: RoomProps) {
  const localVideo = React.useRef<HTMLVideoElement>(null);
  const socket = React.useRef<WebSocket | null>(null);
  const peer = React.useRef<RTCPeerConnection | null>(null);
  const localStream = React.useRef<MediaStream | null>(null);
  const pendingIce = React.useRef<RTCIceCandidateInit[]>([]);
  const pendingLocalIce = React.useRef<RTCIceCandidateInit[]>([]);
  const remoteStreams = React.useRef(new Map<string, MediaStream>());
  const remoteOwners = React.useRef(new Map<string, string>());
  const remoteTrackStreams = React.useRef(new Map<string, string>());
  const [remotes, setRemotes] = React.useState<Array<{ id: string; stream: MediaStream }>>([]);
  const [status, setStatus] = React.useState("Запуск камеры…");
  const [connected, setConnected] = React.useState(false);
  const [mic, setMic] = React.useState(true);
  const [camera, setCamera] = React.useState(true);
  const [participants, setParticipants] = React.useState<string[]>([]);
  const [sharing, setSharing] = React.useState(false);
  const screenTrack = React.useRef<MediaStreamTrack | null>(null);

  React.useEffect(() => {
    let stopped = false;
    async function start() {
      try {
        const stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        if (stopped) { stream.getTracks().forEach(t => t.stop()); return; }
        localStream.current = stream;
        if (localVideo.current) localVideo.current.srcObject = stream;

        const iceServers: RTCIceServer[] = [];
        const stunUrl = import.meta.env.VITE_STUN_URL || "stun:stun.l.google.com:19302";
        iceServers.push({ urls: stunUrl });
        const turnUrl = import.meta.env.VITE_TURN_URL;
        if (turnUrl) {
          try {
            const response = await fetch("/api/turn-credentials");
            if (response.ok) {
              const turn = await response.json() as { enabled?: boolean; urls?: string; username?: string; credential?: string };
              if (turn.enabled && turn.username && turn.credential) {
                iceServers.push({ urls: turn.urls || turnUrl, username: turn.username, credential: turn.credential });
              }
            }
          } catch (error) {
            console.warn("TURN credentials unavailable", error);
          }
        }
        const pc = new RTCPeerConnection({ iceServers });
        peer.current = pc;
        for (const track of stream.getTracks()) pc.addTrack(track, stream);

        pc.ontrack = (event) => {
          const stream = event.streams[0] ?? new MediaStream([event.track]);
          const id = stream.id || event.track.id;
          remoteStreams.current.set(id, stream);
          setRemotes(Array.from(remoteStreams.current, ([streamId, remote]) => ({ id: streamId, stream: remote })));
        };
        pc.onconnectionstatechange = () => {
          setConnected(pc.connectionState === "connected");
          if (pc.connectionState === "connected") setStatus("Соединение установлено");
          else if (pc.connectionState === "failed") setStatus("WebRTC: соединение не установлено");
          else setStatus(`WebRTC: ${pc.connectionState}`);
        };

        const ws = new WebSocket(mediaUrl());
        socket.current = ws;
        ws.onopen = async () => {
          setStatus("Подключено к языкOn SFU");
          ws.send(JSON.stringify({ type: "join", roomId }));
          for (const candidate of pendingLocalIce.current) {
            ws.send(JSON.stringify({ type: "ice", roomId, data: candidate }));
          }
          pendingLocalIce.current = [];
          try {
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            ws.send(JSON.stringify({ type: "offer", roomId, data: pc.localDescription }));
          } catch (error) {
            console.error("initial WebRTC offer failed", error);
            setStatus("Не удалось начать WebRTC-соединение");
          }
        };
        pc.onicecandidate = event => {
          if (!event.candidate) return;
          const candidate = event.candidate.toJSON();
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: "ice", roomId, data: candidate }));
          } else {
            pendingLocalIce.current.push(candidate);
          }
        };
        ws.onmessage = async event => {
          const message = JSON.parse(event.data) as SignalMessage;
          if (message.type === "joined") {
            const data = message.data;
            setParticipants([message.peerId, ...(data?.peers ?? [])]);
            setStatus("Комната подключена");
            return;
          }
          if (message.type === "peer-joined") {
            setParticipants(current => current.includes(message.peerId) ? current : [...current, message.peerId]);
            setStatus("Новый участник подключился");
            return;
          }
          if (message.type === "peer-left") {
            remoteStreams.current.delete(message.peerId);
            setRemotes(Array.from(remoteStreams.current, ([id, stream]) => ({ id, stream })));
            setParticipants(current => current.filter(id => id !== message.peerId));
            setStatus("Участник вышел");
            return;
          }
          if (message.type === "answer") {
            await pc.setRemoteDescription(message.data);
            for (const candidate of pendingIce.current) await pc.addIceCandidate(candidate);
            pendingIce.current = [];
            return;
          }
          if (message.type === "offer") {
            await pc.setRemoteDescription(message.data);
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            ws.send(JSON.stringify({ type: "answer", roomId, data: pc.localDescription }));
            return;
          }
          if (message.type === "ice") {
            if (!pc.remoteDescription) pendingIce.current.push(message.data);
            else await pc.addIceCandidate(message.data);
            return;
          }
          if (message.type === "error") setStatus(`Ошибка SFU: ${message.error ?? message.data?.code ?? "UNKNOWN"}`);
        };
        ws.onerror = () => setStatus("Ошибка соединения с SFU");
        ws.onclose = () => { if (!stopped) setStatus("SFU отключён"); };
      } catch (error) { console.error(error); setStatus("Нет доступа к камере или микрофону"); }
    }
    void start();
    return () => {
      stopped = true;
      socket.current?.close();
      peer.current?.close();
      localStream.current?.getTracks().forEach(t => t.stop());
      screenTrack.current?.stop();
      screenTrack.current = null;
      remoteStreams.current.clear();
      remoteOwners.current.clear();
      remoteTrackStreams.current.clear();
    };
  }, [roomId]);

  function toggleMic() { const next = !mic; localStream.current?.getAudioTracks().forEach(t => t.enabled = next); setMic(next); }
  function toggleCamera() { const next = !camera; localStream.current?.getVideoTracks().forEach(t => t.enabled = next); setCamera(next); }

  async function toggleScreenShare() {
    const pc = peer.current;
    const stream = localStream.current;
    if (!pc || !stream) return;
    if (sharing) {
      const cameraTrack = stream.getVideoTracks().find(track => track !== screenTrack.current);
      const sender = pc.getSenders().find(item => item.track === screenTrack.current);
      if (sender && cameraTrack) await sender.replaceTrack(cameraTrack);
      screenTrack.current?.stop();
      screenTrack.current = null;
      setSharing(false);
      return;
    }
    try {
      const display = await navigator.mediaDevices.getDisplayMedia({ video: true });
      const track = display.getVideoTracks()[0];
      const sender = pc.getSenders().find(item => item.track?.kind === "video");
      if (!sender) { track.stop(); return; }
      await sender.replaceTrack(track);
      screenTrack.current = track;
      setSharing(true);
      track.onended = () => {
        const cameraTrack = stream.getVideoTracks()[0];
        void sender.replaceTrack(cameraTrack).then(() => {
          screenTrack.current = null;
          setSharing(false);
        });
      };
    } catch (error) {
      console.warn("screen sharing cancelled", error);
    }
  }

  return <main className="meeting">
    <header className="meeting-header"><div className="logo">язык<span>On</span></div><div className="room-code">Комната: {roomId}</div><a href="/">Выйти</a></header>
    <section className="video-grid">
      <div className="video-tile local"><video ref={localVideo} autoPlay muted playsInline /><span>Вы</span></div>
      {remotes.map(remote => <RemoteVideo key={remote.id} id={remote.id} stream={remote.stream} />)}
      {!remotes.length && <div className="video-tile remote"><span>Ожидание участников</span></div>}
    </section>
    <div className="meeting-status">{status} {connected ? "• online" : ""}</div>
    <aside className="participants">
      <strong>Участники ({participants.length})</strong>
      {participants.map(id => <div key={id} className="participant">{id === participants[0] ? "Вы" : "Участник"} <code>{id.slice(0, 8)}</code></div>)}
    </aside>
    <nav className="controls"><button onClick={toggleMic}>{mic ? "🎙️ Микрофон" : "🔇 Микрофон"}</button><button onClick={toggleCamera}>{camera ? "📷 Камера" : "🚫 Камера"}</button><button onClick={toggleScreenShare}>{sharing ? "🛑 Остановить экран" : "🖥️ Экран"}</button><a className="leave" href="/">Завершить</a></nav>
  </main>;
}

function RemoteVideo({ id, stream }: { id: string; stream: MediaStream }) {
  const ref = React.useRef<HTMLVideoElement>(null);
  React.useEffect(() => { if (ref.current) ref.current.srcObject = stream; }, [stream]);
  return <div className="video-tile remote"><video ref={ref} autoPlay playsInline /><span>Участник {id.slice(0, 8)}</span></div>;
}
