import React from "react";
import { MeetingFeatureControls } from "./MeetingFeatures";
import { participantFromMeta, removeParticipant, upsertParticipant, type Participant } from "./participantModel";
import { appendChatMessage, mergeChatMessages, type ChatMessage } from "./chatModel";
import { accessRoom, getRoom, getRoomMembers, getRoomMessages, sendRoomMessage, setRoomMemberRole, getBreakoutRooms, createBreakoutRoom, assignBreakoutParticipant, removeBreakoutParticipant, deleteBreakoutRoom, joinBreakoutRoom, refreshRoomAccessToken, type BreakoutRoom, type RoomMember } from "./api";

interface RoomProps { roomId: string; }
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

function mediaUrls() {
  const configured = import.meta.env.VITE_MEDIA_URLS || import.meta.env.VITE_MEDIA_URL;
  const values = configured ? String(configured).split(",").map(value => value.trim()).filter(Boolean) : [];
  if (!values.length) values.push(`${window.location.protocol === "https:" ? "ws:" : "ws:"}//${window.location.hostname}:4000`);
  return values.map(value => {
    const url = new URL(value, window.location.origin);
    const basePath = url.pathname.replace(/\/$/, "");
    const wsProtocol = url.protocol === "https:" || url.protocol === "wss:" ? "wss:" : "ws:";
    return `${wsProtocol}//${url.host}${basePath}/ws`;
  });
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
  const [reconnectNonce, setReconnectNonce] = React.useState(0);
  const [sfuRoomId, setSfuRoomId] = React.useState(roomId);
  const [sfuAccessToken, setSfuAccessToken] = React.useState<string | null>(null);
  const [breakouts, setBreakouts] = React.useState<BreakoutRoom[]>([]);
  const [breakoutName, setBreakoutName] = React.useState("");
  const [breakoutBusy, setBreakoutBusy] = React.useState(false);
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
  const screenSource = React.useRef<MediaStream | null>(null);
  const screenTrack = React.useRef<MediaStreamTrack | null>(null);
  const iceRestarting = React.useRef(false);
  const iceRestartTimer = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const pendingIceRestart = React.useRef(false);
  const makingOffer = React.useRef(false);
  const ignoreOffer = React.useRef(false);
  const lastRemoteDescription = React.useRef<string | null>(null);
  const reconnectTimer = React.useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectAttempt = React.useRef(0);
  const reconnecting = React.useRef(false);
  const redirecting = React.useRef(false);
  const connectionGeneration = React.useRef(0);
  const selfIdRef = React.useRef("");
  const identityRoomRef = React.useRef<string | null>(null);
  const mediaEndpointRef = React.useRef(0);
  const [chat, setChat] = React.useState<ChatMessage[]>([]);
  const [chatText, setChatText] = React.useState("");
  const [chatHasMore, setChatHasMore] = React.useState(false);
  const [chatBefore, setChatBefore] = React.useState<string | null>(null);
  const [chatLoading, setChatLoading] = React.useState(false);
  const [members, setMembers] = React.useState<RoomMember[]>([]);
  const selfRole = participants.find(participant => participant.peerId === selfId)?.role;
  const canModerate = hostId === selfId || selfRole === "cohost";
  const canAssignRoles = hostId === selfId;

  React.useEffect(() => {
    let stopped = false;
    const generation = ++connectionGeneration.current;
    async function start() {
      try {
        if (identityRoomRef.current !== sfuRoomId) {
          identityRoomRef.current = sfuRoomId;
          selfIdRef.current = "";
          setSelfId("");
        }
        const room = await getRoom(roomId);
        try { const page = await getRoomMessages(roomId); setChat(current => mergeChatMessages(current, page.messages)); setChatHasMore(page.hasMore); setChatBefore(page.nextBefore); } catch (error) { console.warn("chat history unavailable", error); }
        let accessToken = sfuAccessToken ?? sessionStorage.getItem("yazykon-access-" + sfuRoomId);
        if (sfuRoomId === roomId && accessToken) {
          try {
            const refreshed = await refreshRoomAccessToken(roomId);
            accessToken = refreshed.accessToken;
            setSfuAccessToken(null);
          } catch {
            sessionStorage.removeItem("yazykon-access-" + roomId);
            accessToken = null;
          }
        }
        if (sfuRoomId !== roomId && !accessToken) {
          setStatus("Нет токена breakout-комнаты");
          return;
        }
        if (sfuRoomId === roomId && room.requiresPassword && !accessToken) {
          const password = window.prompt("Введите пароль комнаты");
          if (password === null) { setStatus("Вход отменён"); return; }
          const access = await accessRoom(roomId, password);
          accessToken = access.accessToken;
        } else if (sfuRoomId === roomId && !room.requiresPassword && !accessToken) {
          const access = await accessRoom(roomId);
          accessToken = access.accessToken;
        }
        if (accessToken) sessionStorage.setItem("yazykon-access-" + sfuRoomId, accessToken);
        let stream = localStream.current;
        if (!stream) {
          stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
          if (stopped) { stream.getTracks().forEach(t => t.stop()); return; }
          localStream.current = stream;
        }
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
        if (screenTrack.current) {
          const videoSender = pc.getSenders().find(sender => sender.track?.kind === "video");
          if (videoSender) await videoSender.replaceTrack(screenTrack.current);
        }

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
        pc.oniceconnectionstatechange = () => {
          if (stopped || generation !== connectionGeneration.current || peer.current !== pc) return;
          if (pc.iceConnectionState === "connected" || pc.iceConnectionState === "completed") {
            iceRestarting.current = false;
            pendingIceRestart.current = false;
            if (iceRestartTimer.current) { clearTimeout(iceRestartTimer.current); iceRestartTimer.current = null; }
            return;
          }
          if (pc.iceConnectionState === "failed" || pc.iceConnectionState === "disconnected") {
            if (pendingIceRestart.current || iceRestarting.current) return;
            pendingIceRestart.current = true;
            if (iceRestartTimer.current) clearTimeout(iceRestartTimer.current);
            iceRestartTimer.current = setTimeout(() => {
              iceRestartTimer.current = null;
              pendingIceRestart.current = false;
              if (stopped || generation !== connectionGeneration.current || peer.current !== pc) return;
              if (socket.current?.readyState !== WebSocket.OPEN) return;
              if (pc.signalingState !== "stable" || makingOffer.current) return;
              if (pc.signalingState !== "stable" || makingOffer.current) return;
              iceRestarting.current = true;
              makingOffer.current = true;
              void pc.createOffer({ iceRestart: true }).then(async offer => {
                await pc.setLocalDescription(offer);
                const ws = socket.current;
                if (ws?.readyState === WebSocket.OPEN && generation === connectionGeneration.current) {
                  ws.send(JSON.stringify({ type: "offer", roomId: sfuRoomId, data: pc.localDescription }));
                } else {
                  iceRestarting.current = false;
                  makingOffer.current = false;
                }
              }).catch(error => {
                makingOffer.current = false;
                iceRestarting.current = false;
                console.warn("ICE restart failed", error);
                if (!stopped && generation === connectionGeneration.current) setStatus("WebRTC: ICE не удалось восстановить");
              });
            }, 500);
          }
        };
        pc.onconnectionstatechange = () => {
          setConnected(pc.connectionState === "connected");
          if (pc.connectionState === "connected") {
            iceRestarting.current = false;
            setStatus("Соединение установлено");
          } else if (pc.connectionState === "failed" || pc.connectionState === "disconnected") {
            setStatus("WebRTC: восстанавливаем соединение…");
            if (!pendingIceRestart.current && !iceRestarting.current && socket.current?.readyState === WebSocket.OPEN) {
              if (pc.signalingState !== "stable" || makingOffer.current) return;
              iceRestarting.current = true;
              makingOffer.current = true;
              void pc.createOffer({ iceRestart: true }).then(async offer => {
                await pc.setLocalDescription(offer);
                const ws = socket.current;
                if (ws?.readyState === WebSocket.OPEN) {
                  ws.send(JSON.stringify({ type: "offer", roomId: sfuRoomId, data: pc.localDescription }));
                } else {
                  iceRestarting.current = false;
                  makingOffer.current = false;
                }
              }).catch(error => {
                iceRestarting.current = false;
                makingOffer.current = false;
                console.warn("ICE restart failed", error);
                setStatus("WebRTC: соединение не установлено");
              });
            }
          } else {
            setStatus(`WebRTC: ${pc.connectionState}`);
          }
        };

        if (stopped || generation !== connectionGeneration.current) { pc.close(); return; }
        const endpoints = mediaUrls();
        const endpointIndex = mediaEndpointRef.current % endpoints.length;
        const ws = new WebSocket(endpoints[endpointIndex]);
        socket.current = ws;
        ws.onopen = async () => {
          if (stopped || generation !== connectionGeneration.current) { ws.close(); return; }
          reconnectAttempt.current = 0;
          reconnecting.current = false;
          setStatus(`Подключено к языкOn SFU ${endpointIndex + 1}/${endpoints.length}`);
          ws.send(JSON.stringify({ type: "join", roomId: sfuRoomId, peerId: selfIdRef.current || undefined, data: { accessToken, reconnect: Boolean(selfIdRef.current) } }));
          for (const candidate of pendingLocalIce.current) {
            ws.send(JSON.stringify({ type: "ice", roomId: sfuRoomId, data: candidate }));
          }
          pendingLocalIce.current = [];
          try {
            if (pc.signalingState !== "stable" || makingOffer.current) return;
            makingOffer.current = true;
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            ws.send(JSON.stringify({ type: "offer", roomId: sfuRoomId, data: pc.localDescription }));
            makingOffer.current = false;
          } catch (error) {
            console.error("initial WebRTC offer failed", error);
            setStatus("Не удалось начать WebRTC-соединение");
          }
        };
        pc.onicecandidate = event => {
          if (!event.candidate) return;
          const candidate = event.candidate.toJSON();
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: "ice", roomId: sfuRoomId, data: candidate }));
          } else {
            pendingLocalIce.current.push(candidate);
          }
        };
        ws.onmessage = async event => {
          if (stopped || generation !== connectionGeneration.current || socket.current !== ws) return;
          const message = JSON.parse(event.data) as SignalMessage;
          if (message.type === "joined") {
            const data = message.data;
            selfIdRef.current = message.peerId;
            setSelfId(message.peerId);
            setParticipants([participantFromMeta(message.peerId, data?.peerMeta?.[message.peerId]), ...(data?.peers ?? []).map(peerId => participantFromMeta(peerId, data?.peerMeta?.[peerId]))]);
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
            setParticipants(current => upsertParticipant(current, participantFromMeta(message.peerId, message.data)));
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
            setParticipants(current => removeParticipant(current, message.peerId));
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
            if (text) setChat(current => appendChatMessage(current, { id: data.id, userId: data.userId, username: data.username, peerId: message.peerId, text, timestamp: data.timestamp ?? Date.now() }));
            return;
          }
          if (message.type === "answer") {
            if (pc.signalingState !== "have-local-offer") return;
            try {
              await pc.setRemoteDescription(message.data);
            } catch (error) {
              console.warn("failed to apply remote answer", error);
              iceRestarting.current = false;
              setStatus("WebRTC: ответ сигнализации устарел, переподключаемся…");
              ws.close();
              return;
            }
            makingOffer.current = false;
            for (const candidate of pendingIce.current) await pc.addIceCandidate(candidate);
            pendingIce.current = [];
            return;
          }
          if (message.type === "offer") {
            const descriptionKey = JSON.stringify(message.data);
            if (lastRemoteDescription.current === descriptionKey) return;
            if (pc.signalingState !== "stable") {
              ignoreOffer.current = true;
              return;
            }
            ignoreOffer.current = false;
            try {
              await pc.setRemoteDescription(message.data);
              lastRemoteDescription.current = descriptionKey;
            } catch (error) {
              console.warn("failed to apply remote offer", error);
              setStatus("WebRTC: предложение сигнализации отклонено");
              return;
            }
            if (pc.signalingState !== "have-remote-offer") return;
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            ws.send(JSON.stringify({ type: "answer", roomId: sfuRoomId, data: pc.localDescription }));
            return;
          }
          if (message.type === "ice") {
            if (!pc.remoteDescription) pendingIce.current.push(message.data);
            else {
              try { await pc.addIceCandidate(message.data); }
              catch (error) { console.warn("failed to apply ICE candidate", error); }
            }
            return;
          }
          if (message.type === "error") {
            if (message.data?.code === "SFU_ROOM_OWNER" && message.data?.endpoint) {
              const ownerEndpoint = String(message.data.endpoint);
              const ownerIndex = endpoints.findIndex(value => value === ownerEndpoint || value.replace(/\/ws$/, "") === ownerEndpoint.replace(/\/ws$/, ""));
              if (ownerIndex >= 0) {
                mediaEndpointRef.current = ownerIndex;
                redirecting.current = true;
                setStatus(`Комната обслуживается SFU ${ownerIndex + 1}/${endpoints.length} — переключаемся…`);
                ws.close();
                return;
              }
            }
            setStatus(`Ошибка SFU: ${message.error ?? message.data?.code ?? "UNKNOWN"}`);
          }
        };
        ws.onerror = () => { if (!stopped && generation === connectionGeneration.current && socket.current === ws) setStatus("Ошибка соединения с SFU"); };
        ws.onclose = () => {
          if (stopped || generation !== connectionGeneration.current || socket.current !== ws) return;
          setConnected(false);
          if (redirecting.current) {
            redirecting.current = false;
            const redirectDelay = 100;
            if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
            reconnectTimer.current = setTimeout(() => {
              if (stopped || reconnecting.current) return;
              reconnecting.current = true;
              setReconnectNonce(value => value + 1);
            }, redirectDelay);
            return;
          }
          const attempt = Math.min(reconnectAttempt.current++, 6);
          mediaEndpointRef.current = (endpointIndex + 1) % endpoints.length;
          const delay = Math.min(30000, 1000 * 2 ** attempt);
          setStatus(`SFU ${endpointIndex + 1}/${endpoints.length} отключён — переключение на SFU ${(mediaEndpointRef.current % endpoints.length) + 1} через ${Math.ceil(delay / 1000)} с…`);
          if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
          reconnectTimer.current = setTimeout(() => {
            if (stopped || reconnecting.current) return;
            reconnecting.current = true;
            setReconnectNonce(value => value + 1);
          }, delay);
        };
      } catch (error) { console.error(error); setStatus("Нет доступа к камере или микрофону"); }
    }
    void start();
    return () => {
      stopped = true;
      if (connectionGeneration.current === generation) connectionGeneration.current++;
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      if (iceRestartTimer.current) clearTimeout(iceRestartTimer.current);
      iceRestartTimer.current = null;
      pendingIceRestart.current = false;
      iceRestarting.current = false;
      socket.current?.close();
      peer.current?.close();
      pendingIce.current = [];
      pendingLocalIce.current = [];
      remoteStreams.current.clear();
      remoteOwners.current.clear();
      remoteTrackStreams.current.clear();
      setRemotes([]);
    };
  }, [sfuRoomId, sfuAccessToken, reconnectNonce, roomId]);

  React.useEffect(() => () => {
    if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    socket.current?.close();
    peer.current?.close();
    localStream.current?.getTracks().forEach(t => t.stop());
    screenTrack.current?.stop();
    screenSource.current?.getTracks().forEach(track => track.stop());
    screenTrack.current = null;
    screenSource.current = null;
  }, []);

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
      screenSource.current = null;
      setSharing(false);
      return;
    }
    try {
      const display = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
      const track = display.getVideoTracks()[0];
      const sender = pc.getSenders().find(item => item.track?.kind === "video");
      if (!sender) { track.stop(); return; }
      await sender.replaceTrack(track);
      screenTrack.current = track;
      screenSource.current = display;
      setSharing(true);
      track.onended = () => {
        const cameraTrack = stream.getVideoTracks().find(item => item !== track);
        void (cameraTrack ? sender.replaceTrack(cameraTrack) : Promise.resolve()).then(() => {
          screenTrack.current = null;
          screenSource.current = null;
          setSharing(false);
        });
      };
    } catch (error) {
      console.warn("screen sharing cancelled", error);
    }
  }

  async function refreshBreakouts() {
    try { setBreakouts(await getBreakoutRooms(roomId)); } catch { setBreakouts([]); }
  }

  async function createBreakout() {
    const name = breakoutName.trim();
    if (!name || breakoutBusy) return;
    setBreakoutBusy(true);
    try { await createBreakoutRoom(roomId, name); setBreakoutName(""); await refreshBreakouts(); setStatus("Breakout-комната создана"); }
    catch { setStatus("Не удалось создать breakout-комнату"); }
    finally { setBreakoutBusy(false); }
  }

  async function assignParticipant(breakoutId: string, userId: string) {
    if (breakoutBusy) return;
    setBreakoutBusy(true);
    try { await assignBreakoutParticipant(roomId, breakoutId, userId); await refreshBreakouts(); setStatus("Участник назначен в breakout-комнату"); }
    catch { setStatus("Не удалось назначить участника"); }
    finally { setBreakoutBusy(false); }
  }

  async function unassignParticipant(breakoutId: string, userId: string) {
    if (breakoutBusy) return;
    setBreakoutBusy(true);
    try { await removeBreakoutParticipant(roomId, breakoutId, userId); await refreshBreakouts(); setStatus("Участник возвращён из breakout-назначения"); }
    catch { setStatus("Не удалось снять назначение"); }
    finally { setBreakoutBusy(false); }
  }

  async function deleteBreakout(breakoutId: string) {
    if (breakoutBusy) return;
    if (!window.confirm("Удалить breakout-комнату? Участники смогут вернуться в основную комнату.")) return;
    setBreakoutBusy(true);
    try { await deleteBreakoutRoom(roomId, breakoutId); await refreshBreakouts(); setStatus("Breakout-комната удалена"); }
    catch { setStatus("Не удалось удалить breakout-комнату"); }
    finally { setBreakoutBusy(false); }
  }

  async function enterBreakout(breakoutId: string) {
    if (breakoutBusy) return;
    setBreakoutBusy(true);
    try { const access = await joinBreakoutRoom(roomId, breakoutId); setSfuAccessToken(access.accessToken); setSfuRoomId(access.breakoutId); setStatus("Переходим в breakout-комнату…"); }
    catch { setStatus("Не удалось войти в breakout-комнату"); }
    finally { setBreakoutBusy(false); }
  }

  function returnToMainRoom() {
    const token = sessionStorage.getItem("yazykon-access-" + roomId);
    if (!token) { setStatus("Не найден токен основной комнаты"); return; }
    setSfuAccessToken(token); setSfuRoomId(roomId); setStatus("Возвращаемся в основную комнату…");
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

  React.useEffect(() => {
    if (sfuRoomId !== roomId) return;
    void refreshBreakouts();
    const timer = window.setInterval(() => { void refreshBreakouts(); }, 3000);
    return () => window.clearInterval(timer);
  }, [sfuRoomId, roomId]);

  const selfUserId = participants.find(participant => participant.peerId === selfId)?.userId;
  const assignedBreakout = selfUserId ? breakouts.find(breakout => breakout.participants.includes(selfUserId)) : undefined;

  function approveWaiting(peerId: string) { setWaitingPeers(current => current.filter(id => id !== peerId)); moderate("approve", peerId); }
  function denyWaiting(peerId: string) { setWaitingPeers(current => current.filter(id => id !== peerId)); moderate("deny", peerId); }

  function toggleRoomLock() {
    const ws = socket.current;
    if (!ws || ws.readyState !== WebSocket.OPEN || !canModerate) return;
    ws.send(JSON.stringify({ type: "moderate", roomId, data: { action: roomLocked ? "unlock" : "lock" } }));
  }

  async function loadOlderChat() {
    if (chatLoading || !chatHasMore || !chatBefore) return;
    setChatLoading(true);
    try {
      const page = await getRoomMessages(roomId, chatBefore);
      setChat(current => mergeChatMessages(current, page.messages, 500));
      setChatHasMore(page.hasMore);
      setChatBefore(page.nextBefore);
    } catch {
      setStatus("Не удалось загрузить старые сообщения");
    } finally {
      setChatLoading(false);
    }
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
      setChat(current => appendChatMessage(current, message));
      setChatText("");
    } catch (error) {
      setStatus("Не удалось отправить сообщение");
    }
  }

  return <main className="meeting">
    <header className="meeting-header"><div className="logo">язык<span>On</span></div><div className="room-code">Комната: {sfuRoomId}{sfuRoomId !== roomId ? " • breakout" : ""}</div><a href="/">Выйти</a></header>
    <section className="video-grid">
      <div className="video-tile local"><video ref={localVideo} autoPlay muted playsInline /><span>Вы</span></div>
      {remotes.map(remote => <RemoteVideo key={remote.id} id={remote.id} stream={remote.stream} />)}
      {!remotes.length && <div className="video-tile remote"><span>Ожидание участников</span></div>}
    </section>
    <div className="meeting-status">{status} {connected ? "• online" : ""}</div>
    {sfuRoomId === roomId && assignedBreakout && <aside className="breakout-assigned">
      <strong>Вам назначена breakout-комната: {assignedBreakout.name}</strong>
      <button type="button" onClick={() => void enterBreakout(assignedBreakout.id)} disabled={breakoutBusy}>Перейти</button>
    </aside>}
    {hostId === selfId && sfuRoomId === roomId && <aside className="breakouts">
      <strong>Breakout-комнаты</strong>
      <form onSubmit={event => { event.preventDefault(); void createBreakout(); }} className="participant-controls">
        <input value={breakoutName} onChange={event => setBreakoutName(event.target.value)} maxLength={80} placeholder="Название комнаты" />
        <button type="submit" disabled={breakoutBusy || !breakoutName.trim()}>Создать</button>
      </form>
      {breakouts.length === 0 && <span className="chat-empty">Комнаты ещё не созданы</span>}
      {breakouts.map(breakout => <div key={breakout.id} className="participant">
        <span>{breakout.name} ({breakout.participants.length})</span>
        <button type="button" onClick={() => void enterBreakout(breakout.id)} disabled={breakoutBusy}>Войти</button><button type="button" onClick={() => void deleteBreakout(breakout.id)} disabled={breakoutBusy}>Удалить</button>
        {participants.filter(p => p.userId && p.peerId !== selfId).map(p => {
          const assigned = Boolean(p.userId && breakout.participants.includes(p.userId));
          const member = members.find(m => m.id === p.userId);
          return <span key={p.peerId}>
            {assigned
              ? <button type="button" onClick={() => p.userId && void unassignParticipant(breakout.id, p.userId)} disabled={breakoutBusy}>✓ {member?.username ?? p.peerId.slice(0, 8)}</button>
              : <button type="button" onClick={() => p.userId && void assignParticipant(breakout.id, p.userId)} disabled={breakoutBusy || !p.userId}>+ {member?.username ?? p.peerId.slice(0, 8)}</button>}
          </span>;
        })}
      </div>)}
    </aside>}
    {sfuRoomId !== roomId && <aside className="breakout-active">
      <strong>Вы в breakout-комнате</strong>
      <button type="button" onClick={returnToMainRoom} disabled={breakoutBusy}>↩ Вернуться в основную</button>
    </aside>}
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
        {chatHasMore && <button type="button" onClick={() => void loadOlderChat()} disabled={chatLoading}>{chatLoading ? "Загрузка…" : "Загрузить старые сообщения"}</button>}
        {!chat.length && <span className="chat-empty">Сообщений пока нет</span>}
        {chat.map((message, index) => <div key={message.timestamp + "-" + index} className="chat-message"><code>{message.userId && message.userId === participants.find(item => item.peerId === selfId)?.userId ? "Вы" : message.username ?? (message.peerId ? message.peerId.slice(0, 8) : "Участник")}</code><span>{message.text}</span></div>)}
      </div>
      <form onSubmit={sendChat}>
        <input value={chatText} onChange={event => setChatText(event.target.value)} maxLength={2000} placeholder="Написать сообщение…" />
        <button type="submit">Отправить</button>
      </form>
    </section>
    <nav className="controls"><MeetingFeatureControls stream={localStream.current} /><button onClick={toggleMic}>{mic ? "🎙️ Микрофон" : "🔇 Микрофон"}</button><button onClick={toggleCamera}>{camera ? "📷 Камера" : "🚫 Камера"}</button><button onClick={toggleScreenShare}>{sharing ? "🛑 Остановить экран" : "🖥️ Экран"}</button><a className="leave" href="/">Завершить</a></nav>
  </main>;
}

function RemoteVideo({ id, stream }: { id: string; stream: MediaStream }) {
  const ref = React.useRef<HTMLVideoElement>(null);
  React.useEffect(() => { if (ref.current) ref.current.srcObject = stream; }, [stream]);
  return <div className="video-tile remote"><video ref={ref} autoPlay playsInline /><span>Участник {id.slice(0, 8)}</span></div>;
}
