import React from "react";

interface RoomProps {
  roomId: string;
}

type SignalMessage =
  | { type: "joined"; roomId: string; peerId: string; peers: string[] }
  | { type: "peer-joined"; peerId: string }
  | { type: "peer-left"; peerId: string }
  | { type: "offer"; peerId: string; data: RTCSessionDescriptionInit }
  | { type: "answer"; peerId: string; data: RTCSessionDescriptionInit }
  | { type: "ice"; peerId: string; data: RTCIceCandidateInit }
  | { type: "error"; error: string };

const RTC_CONFIG: RTCConfiguration = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }]
};

function getWebSocketUrl() {
  const api = import.meta.env.VITE_API_URL;
  if (api) {
    const url = new URL(api);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    return `${url.origin}/ws`;
  }
  return `${window.location.protocol === "https:" ? "wss:" : "ws:"}//${window.location.host}/ws`;
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

  const send = React.useCallback((message: object) => {
    if (socket.current?.readyState === WebSocket.OPEN) {
      socket.current.send(JSON.stringify(message));
    }
  }, []);

  const createPeer = React.useCallback((targetPeerId: string) => {
    if (peer.current) return peer.current;

    const pc = new RTCPeerConnection(RTC_CONFIG);

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        send({
          type: "ice",
          roomId,
          peerId: targetPeerId,
          data: event.candidate.toJSON()
        });
      }
    };

    pc.ontrack = (event) => {
      const [stream] = event.streams;
      if (stream && remoteVideo.current) {
        remoteVideo.current.srcObject = stream;
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

    if (localStream.current) {
      for (const track of localStream.current.getTracks()) {
        pc.addTrack(track, localStream.current);
      }
    }

    peer.current = pc;
    return pc;
  }, [roomId, send]);

  const makeOffer = React.useCallback(async (targetPeerId: string) => {
    const pc = createPeer(targetPeerId);
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    send({
      type: "offer",
      roomId,
      peerId: targetPeerId,
      data: pc.localDescription
    });
  }, [createPeer, roomId, send]);

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

        const ws = new WebSocket(getWebSocketUrl());
        socket.current = ws;

        ws.onopen = () => {
          setStatus("Сигнальный сервер подключён");
          send({ type: "join", roomId });
        };

        ws.onmessage = async (event) => {
          const message = JSON.parse(event.data) as SignalMessage;

          if (message.type === "joined") {
            if (message.peers.length === 0) {
              setStatus("Вы в комнате. Ожидание участника…");
            } else {
              await makeOffer(message.peers[0]);
            }
            return;
          }

          if (message.type === "peer-joined") {
            if (!peer.current) await makeOffer(message.peerId);
            return;
          }

          if (message.type === "offer") {
            const pc = createPeer(message.peerId);
            await pc.setRemoteDescription(message.data);

            for (const candidate of pendingIce.current) {
              await pc.addIceCandidate(candidate);
            }
            pendingIce.current = [];

            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);

            send({
              type: "answer",
              roomId,
              peerId: message.peerId,
              data: pc.localDescription
            });
            return;
          }

          if (message.type === "answer") {
            const pc = peer.current;
            if (!pc) return;
            await pc.setRemoteDescription(message.data);

            for (const candidate of pendingIce.current) {
              await pc.addIceCandidate(candidate);
            }
            pendingIce.current = [];
            return;
          }

          if (message.type === "ice") {
            const pc = peer.current;
            if (!pc || !pc.remoteDescription) {
              pendingIce.current.push(message.data);
            } else {
              await pc.addIceCandidate(message.data);
            }
            return;
          }

          if (message.type === "peer-left") {
            peer.current?.close();
            peer.current = null;
            pendingIce.current = [];
            if (remoteVideo.current) remoteVideo.current.srcObject = null;
            setConnected(false);
            setStatus("Участник вышел. Ожидание…");
            return;
          }

          if (message.type === "error") {
            setStatus(`Ошибка: ${message.error}`);
          }
        };

        ws.onerror = () => setStatus("Ошибка WebSocket");
        ws.onclose = () => {
          if (!stopped) setStatus("Сигнальный сервер отключён");
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
  }, [makeOffer, roomId, send]);

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
          <span>{connected ? "Участник" : "Ожидание участника"}</span>
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
