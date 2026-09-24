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
  const selfIdRef = React.useRef("");
  const connectionGeneration = React.useRef(0);
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
              iceRestarting.current = true;
              if (pc.signalingState !== "stable" || makingOffer.current) return;
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
        const ws = new WebSocket(mediaUrl());
        socket.current = ws;
        ws.onopen = async () => {
          if (stopped || generation !== connectionGeneration.current) { ws.close(); return; }
          reconnectAttempt.current = 0;
          reconnecting.current = false;
          setStatus("Восстанавливаем медиасессию…");
          setStatus("Подключено к языкOn SFU");
          ws.send(JSON.stringify({ type: "join", roomId: sfuRoomId, peerId: selfIdRef.current || undefined, data: { accessToken, reconnect: Boolean(selfIdRef.current) } }));
          for (const candidate of pendingLocalIce.current) {