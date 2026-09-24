import React from "react";
import { accessRoom, getRoom, getRoomMembers, getRoomMessages, sendRoomMessage, setRoomMemberRole, type RoomMember } from "./api";

interface RoomProps { roomId: string; }
type Participant = { peerId: string; userId?: string; role?: string };
type ChatMessage = { id?: string; userId?: string; username?: string; peerId?: string; text: string; timestamp: number };
type SignalMessage =
  | { type: "joined"; roomId: string; peerId: string; data?: { peers: string[]; peerMeta?: Record<string, { userId?: string; role?: string }>; tracks?: number; hostId?: string; locked?: boolean; lobby?: boolean } }
  | { type: "peer-joined"; peerId: string; data?: { userId?: string; role?: string } }
  | { type: "peer-left"; peerId: string }
  | { type: "role-updated"; data?: { userId?: string; role?: string } }
  | { type: "track-published"; peerId: string; data?: { trackId?: string } }
  | { type: "track-removed"; peerId: string; data?: { trackId?: string } }
  | { type: "chat"; peerId: string; data?: { id?: string; userId?: string; username?: string; text?: string; timestamp?: number } }
  | { type: "removed"; data?: { reason?: string } }
  | { type: "muted"; data?: { by?: string } }
  | { type: "host-changed"; peerId: string }
  | { type: "offer"; data: RTCSessionDescriptionInit }
  | { type: "answer"; data: RTCSessionDescriptionInit }
  | { type: "ice"; data: RTCIceCandidateInit }
  | { type: "error"; error?: string; data?: { code?: string } }
  | { type: "lobby-waiting" }
  | { type: "lobby-join"; peerId: string }
  | { type: "lobby-denied" }
  | { type: "lobby-on" }
  | { type: "lobby-off" };

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
  const [status, setStatus] = React.useState("Проверяем доступ к комнате…");
  const [connected, setConnected] = React.useState(false);
  const [mic, setMic] = React.useState(true);
  const [camera, setCamera] = React.useState(true);
  const [participants, setParticipants] = React.useState<Participant[]>([]);
  const [selfId, setSelfId] = React.useState("");
  const [roomLocked, setRoomLocked] = React.useState(false);
  const [lobby, setLobby] = React.useState(false);
  const [waiting, setWaiting] = React.useState(false);
  const [waitingPeers, setWaitingPeers] = React.useState<string[]>([]);
  const [hostId, setHostId] = React.useState("");
  const [sharing, setSharing] = React.useState(false);
  const screenTrack = React.useRef<MediaStreamTrack | null>(null);
  const [chat, setChat] = React.useState<ChatMessage[]>([]);
  const [chatText, setChatText] = React.useState("");
  const [members, setMembers] = React.useState<RoomMember[]>([]);
  const selfRole = participants.find(participant => participant.peerId === selfId)?.role;
  const canModerate = hostId === selfId || selfRole === "cohost";
  const canAssignRoles = hostId === selfId;

  React.useEffect(() => {
    let stopped = false;
    async function start() {
      try {
        const room = await getRoom(roomId);
        try { setChat(await getRoomMessages(roomId)); } catch (error) { console.warn("chat history unavailable", error); }
        let accessToken = sessionStorage.getItem("yazykon-access-" + roomId);
        if (room.requiresPassword && !accessToken) {
          const password = window.prompt("Введите пароль комнаты");
          if (password === null) { setStatus("Вход отменён"); return; }
          const access = await accessRoom(roomId, password);
          accessToken = access.accessToken;
        } else if (!room.requiresPassword && !accessToken) {
          const access = await accessRoom(roomId);
          accessToken = access.accessToken;
        }
        if (accessToken) sessionStorage.setItem("yazykon-access-" + roomId, accessToken);
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
          if (event.track.kind === "video") {
            event.track.onended = () => {
              const current = remoteStreams.current.get(id);
              if (current) {
                current.removeTrack(event.track);
                if (current.getTracks().length === 0) remoteStreams.current.delete(id);
                setRemotes(Array.from(remoteStreams.current, ([streamId, remote]) => ({ id: streamId, stream: remote })));
              }
            };
          }
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
          ws.send(JSON.stringify({ type: "join", roomId, data: { accessToken } }));
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
            setSelfId(message.peerId);
            setParticipants([ { peerId: message.peerId, ...data?.peerMeta?.[message.peerId] }, ...(data?.peers ?? []).map(peerId => ({ peerId, ...data?.peerMeta?.[peerId] })) ]);
            setHostId(data?.hostId ?? message.peerId);
            setRoomLocked(Boolean(data?.locked));
            setLobby(Boolean(data?.lobby));
            setWaiting(false);
            setWaitingPeers(current => current.filter(id => id !== message.peerId));
            setStatus("Комната подключена");
            return;
          }
          if (message.type === "lobby-waiting") {
            setWaiting(true);
            setStatus("Вы в зале ожидания — ведущий должен разрешить вход");
            return;
          }
          if (message.type === "lobby-join") {
            setWaitingPeers(current => current.includes(message.peerId) ? current : [...current, message.peerId]);
            setStatus("Новый участник ожидает входа");
            return;
          }
          if (message.type === "lobby-denied") {
            setStatus("Ведущий отклонил вход в комнату");
            socket.current?.close();
            peer.current?.close();
            return;
          }
          if (message.type === "lobby-on") { setLobby(true); return; }
          if (message.type === "lobby-off") { setLobby(false); setWaitingPeers([]); return; }
          if (message.type === "role-updated") { const data = message.data ?? {}; setParticipants(current => current.map(p => p.userId === data.userId ? { ...p, role: data.role } : p)); setMembers(current => current.map(m => m.id === data.userId ? { ...m, role: data.role as RoomMember["role"] } : m)); return; }

    if (message.type === "peer-joined") {
            setParticipants(current => current.some(p => p.peerId === message.peerId) ? current : [...current, { peerId: message.peerId, userId: message.data?.userId, role: message.data?.role }]);
            setStatus("Новый участник подключился");
            return;
          }
          if (message.type === "peer-left") {
            for (const [trackId, owner] of remoteOwners.current) {
              if (owner === message.peerId) {
                const streamId = remoteTrackStreams.current.get(trackId);
                if (streamId) {
                  const remote = remoteStreams.current.get(streamId);
                  if (remote) remote.getTracks().filter(track => track.id === trackId).forEach(track => remote.removeTrack(track));
                  if (remote && remote.getTracks().length === 0) remoteStreams.current.delete(streamId);
                }
                remoteOwners.current.delete(trackId);
                remoteTrackStreams.current.delete(trackId);
              }
            }
            setRemotes(Array.from(remoteStreams.current, ([id, stream]) => ({ id, stream })));
            setParticipants(current => current.filter(p => p.peerId !== message.peerId));
            setStatus("Участник вышел");
            return;
          }
          if (message.type === "room-locked") {
            setRoomLocked(true);
            setStatus("Комната закрыта для новых участников");
            return;
          }
          if (message.type === "room-unlocked") {
            setRoomLocked(false);
            setStatus("Комната снова открыта");
            return;
          }
          if (message.type === "host-changed") {
            setWaitingPeers(current => current.filter(id => id !== message.peerId));
            setHostId(message.peerId);
            setStatus("Роль ведущего передана");
            return;
          }
          if (message.type === "muted") {
            localStream.current?.getAudioTracks().forEach(track => track.enabled = false);
            setMic(false);
            setStatus("Ведущий выключил ваш микрофон");
            return;
          }
          if (message.type === "removed") {
            setStatus("Вы были удалены ведущим");
            socket.current?.close();
            peer.current?.close();
            return;
          }
          if (message.type === "chat") {
            const data = message.data ?? {};
            const text = data.text;
            if (text) setChat(current => current.some(item => item.id === data.id) ? current : [...current.slice(-99), { id: data.id, userId: data.userId, username: data.username, peerId: message.peerId, text, timestamp: data.timestamp ?? Date.now() }]);
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

  async function refreshMembers() { try { setMembers(await getRoomMembers(roomId)); } catch { setMembers([]); } }

  async function changeMemberRole(userId: string, role: "cohost" | "member") { try { await setRoomMemberRole(roomId, userId, role); await refreshMembers(); setStatus("Роль участника изменена"); } catch { setStatus("Не удалось изменить роль"); } }

  function muteParticipant(peerId: string) {
    const ws = socket.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || !canModerate) return;
    ws.send(JSON.stringify({ type: "moderate", roomId, data: { action: "mute", peerId } }));
  }

  function removeParticipant(peerId: string) {
    const ws = socket.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || !canModerate) return;
    ws.send(JSON.stringify({ type: "moderate", roomId, data: { action: "remove", peerId } }));
  }

  function moderate(action: string, peerId?: string) {
    const ws = socket.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || !canModerate) return;
    ws.send(JSON.stringify({ type: "moderate", roomId, data: { action, ...(peerId ? { peerId } : {}) } }));
  }

  function toggleLobby() { moderate(lobby ? "lobby-off" : "lobby-on"); }

  React.useEffect(() => { if (hostId === selfId) void refreshMembers(); }, [hostId, selfId, roomId]);

  function approveWaiting(peerId: string) { setWaitingPeers(current => current.filter(id => id !== peerId)); moderate("approve", peerId); }
  function denyWaiting(peerId: string) { setWaitingPeers(current => current.filter(id => id !== peerId)); moderate("deny", peerId); }

  function toggleRoomLock() {
    const ws = socket.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || !canModerate) return;
    ws.send(JSON.stringify({ type: "moderate", roomId, data: { action: roomLocked ? "unlock" : "lock" } }));
  }

  async function sendChat(event: React.FormEvent) {
    event.preventDefault();
    const text = chatText.trim();
    if (!text) return;
    if (Array.from(text).length > 2000) {
      setStatus("Сообщение слишком длинное (максимум 2000 символов)");
      return;
    }
    try {
      const message = await sendRoomMessage(roomId, text);
      setChat(current => current.some(item => item.id === message.id) ? current : [...current.slice(-99), message]);
      setChatText("");
    } catch (error) {
      setStatus("Не удалось отправить сообщение");
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
      {canModerate && <div className="participant-controls"><button type="button" onClick={toggleLobby}>{lobby ? "🚪 Выключить Lobby" : "🚪 Включить Lobby"}</button>{roomLocked ? null : null}</div>}
      {canModerate && waitingPeers.length > 0 && <div className="lobby-waiting"><strong>Ожидают входа ({waitingPeers.length})</strong>{waitingPeers.map(id => <div key={id} className="participant"><code>{id.slice(0, 8)}</code><button type="button" onClick={() => approveWaiting(id)}>Разрешить вход</button><button type="button" onClick={() => denyWaiting(id)}>Отклонить</button></div>)}</div>}
      {participants.map(participant => { const id = participant.peerId; const member = members.find(item => item.id === participant.userId); return <div key={id} className="participant">{id === selfId ? "Вы" : member?.username ?? "Участник"} <code>{id.slice(0, 8)}</code>{member?.role === "host" || id === hostId ? " • ведущий" : member?.role === "cohost" ? " • со-ведущий" : ""}{hostId === selfId && id !== selfId ? <><button type="button" onClick={() => muteParticipant(id)}>Микрофон</button><button type="button" onClick={() => removeParticipant(id)}>Удалить</button>{member?.role === "cohost" ? <button type="button" onClick={() => changeMemberRole(member.id, "member")}>Снять со-ведущего</button> : <button type="button" onClick={() => changeMemberRole(member?.id ?? "", "cohost")} disabled={!member}>Сделать со-ведущим</button>}</> : null}</div>; })}
      {canModerate && <button type="button" onClick={() => toggleRoomLock()}>{roomLocked ? "🔓 Открыть комнату" : "🔒 Заблокировать комнату"}</button>}
    </aside>
    <section className="chat">
      <strong>Чат</strong>
      <div className="chat-messages">
        {!chat.length && <span className="chat-empty">Сообщений пока нет</span>}
        {chat.map((message, index) => <div key={message.timestamp + "-" + index} className="chat-message"><code>{message.userId && message.userId === participants.find(item => item.peerId === selfId)?.userId ? "Вы" : message.username ?? (message.peerId ? message.peerId.slice(0, 8) : "Участник")}</code><span>{message.text}</span></div>)}
      </div>
      <form onSubmit={sendChat}>
        <input value={chatText} onChange={event => setChatText(event.target.value)} maxLength={2000} placeholder="Написать сообщение…" />
        <button type="submit">Отправить</button>
      </form>
    </section>
    <nav className="controls"><button onClick={toggleMic}>{mic ? "🎙️ Микрофон" : "🔇 Микрофон"}</button><button onClick={toggleCamera}>{camera ? "📷 Камера" : "🚫 Камера"}</button><button onClick={toggleScreenShare}>{sharing ? "🛑 Остановить экран" : "🖥️ Экран"}</button><a className="leave" href="/">Завершить</a></nav>
  </main>;
}

function RemoteVideo({ id, stream }: { id: string; stream: MediaStream }) {
  const ref = React.useRef<HTMLVideoElement>(null);
  React.useEffect(() => { if (ref.current) ref.current.srcObject = stream; }, [stream]);
  return <div className="video-tile remote"><video ref={ref} autoPlay playsInline /><span>Участник {id.slice(0, 8)}</span></div>;
}
