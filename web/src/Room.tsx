import React from "react";

interface RoomProps {
  roomId: string;
}

type SignalMessage =
  | { type: "joined"; roomId: string; peerId: string; data?: { peers: string[] } }
  | { type: "peer-joined"; peerId: string }
  | { type: "peer-left"; peerId: string }
  | { type: "offer"; data: RTCSessionDescriptionInit }
  | { type: "answer"; data: RTCSessionDescriptionInit }
  | { type: "ice"; data: RTCIceCandidateInit }
  | { type: "error"; error?: string; data?: { code?: string } };

function mediaUrl() {
  const configured = import.meta.env.VITE_MEDIA_URL;
  if (configured) {
    const url = new URL(configured);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    return `${url.origin}/ws`;
  }

  return `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.hostname}:4000/ws`;
}

export function Room({ roomId }: RoomProps) {
  const localVideo = React.useRef<HTMLVideoElement>(null);
  const remoteVideo = React.useRef<HTMLVideoElement>(null);
  const socket = React.useRef<WebSocket | null>(null);
  const peer = React.useRef<RTCPeerConnection | null>(null);
  const localStream = React.useRef<MediaStream | null>(null);
  const pendingIce = React.useRef<RTCIceCandidateInit[]>([]);

  const [status, setStatus] = React.useState("Запуск камеры…");
  const [connected, setConnected] = React.useState(false);
  const [mic, setMic] = React.useState(true);
  const [camera, setCamera] = React.useState(true);

  React.useEffect(() => {
    let stopped = false;

    async function start() {
      try {
        const stream = await navigator.mediaDevices.getUserMedia({
          video: true,
          audio: true
        });

        if (stopped) {
          stream.getTracks().forEach((track) => track.stop());
          return;
        }

        localStream.current = stream;
        if (localVideo.current) localVideo.current.srcObject = stream;

        const pc = new RTCPeerConnection({
          iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
        });
        peer.current = pc;

        for (const track of stream.getTracks()) {
          pc.addTrack(track, stream);
        }

        pc.ontrack = (event) => {
          const [remoteStream] = event.streams;
          if (remoteStream && remoteVideo.current) {
            remoteVideo.current.srcObject = remoteStream;
          }
        };

        pc.onconnectionstatechange = () => {
          if (pc.connectionState === "connected") {
            setConnected(true);
            setStatus("Соединение установлено");
          } else if (pc.connectionState === "failed") {
            setConnected(false);
            setStatus("WebRTC: соединение не установлено");
          } else {
            setStatus(`WebRTC: ${pc.connectionState}`);
          }
        };

        const ws = new WebSocket(mediaUrl());
        socket.current = ws;

        ws.onopen = async () => {
          setStatus("Подключено к языкуOn SFU");
          ws.send(JSON.stringify({ type: "join", roomId }));

          const offer = await pc.createOffer();
          await pc.setLocalDescription(offer);

          ws.send(JSON.stringify({
            type: "offer",
            roomId,
            data: pc.localDescription
          }));
        };

        pc.onicecandidate = (event) => {
          if (event.candidate && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
              type: "ice",
              roomId,
              data: event.candidate.toJSON()
            }));
          }
        };

        ws.onmessage = async (event) => {
          const message = JSON.parse(event.data) as SignalMessage;

          if (message.type === "joined") {
            setStatus("Комната подключена");
            return;
          }

          if (message.type === "peer-joined") {
            setStatus("Новый участник подключился");
            return;
          }

          if (message.type === "peer-left") {
            setStatus("Участник вышел");
            return;
          }

          if (message.type === "answer") {
            await pc.setRemoteDescription(message.data);
            for (const candidate of pendingIce.current) {
              await pc.addIceCandidate(candidate);
            }
            pendingIce.current = [];
            return;
          }

          if (message.type === "offer") {
            await pc.setRemoteDescription(message.data);
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);

            ws.send(JSON.stringify({
              type: "answer",
              roomId,
              data: pc.localDescription
            }));
            return;
          }

          if (message.type === "ice") {
            if (!pc.remoteDescription) {
              pendingIce.current.push(message.data);
            } else {
              await pc.addIceCandidate(message.data);
            }
            return;
          }

          if (message.type === "error") {
            setStatus(`Ошибка SFU: ${message.error ?? message.data?.code ?? "UNKNOWN"}`);
          }
        };

        ws.onerror = () => setStatus("Ошибка соединения с SFU");
        ws.onclose = () => {
          if (!stopped) setStatus("SFU отключён");
        };
      } catch (error) {
        console.error(error);
        setStatus("Нет доступа к камере или микрофону");
      }
    }

    void start();

    return () => {
      stopped = true;
      socket.current?.close();
      peer.current?.close();
      localStream.current?.getTracks().forEach((track) => track.stop());
    };
  }, [roomId]);

  function toggleMic() {
    const next = !mic;
    localStream.current?.getAudioTracks().forEach((track) => {
      track.enabled = next;
    });
    setMic(next);
  }

  function toggleCamera() {
    const next = !camera;
    localStream.current?.getVideoTracks().forEach((track) => {
      track.enabled = next;
    });
    setCamera(next);
  }

  return (
    <main className="meeting">
      <header className="meeting-header">
        <div className="logo">язык<span>On</span></div>
        <div className="room-code">Комната: {roomId}</div>
        <a href="/">Выйти</a>
      </header>

      <section className="video-grid">
        <div className="video-tile local">
          <video ref={localVideo} autoPlay muted playsInline />
          <span>Вы</span>
        </div>
        <div className="video-tile remote">
          <video ref={remoteVideo} autoPlay playsInline />
          <span>{connected ? "Участники" : "Ожидание участников"}</span>
        </div>
      </section>

      <div className="meeting-status">{status}</div>

      <nav className="controls">
        <button onClick={toggleMic}>{mic ? "🎙️ Микрофон" : "🔇 Микрофон"}</button>
        <button onClick={toggleCamera}>{camera ? "📷 Камера" : "🚫 Камера"}</button>
        <a className="leave" href="/">Завершить</a>
      </nav>
    </main>
  );
}
