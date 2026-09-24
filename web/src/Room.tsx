import { JitsiMeeting } from "@jitsi/react-sdk";

interface RoomProps {
  roomId: string;
}

export function Room({ roomId }: RoomProps) {
  return (
    <main className="meeting">
      <header className="meeting-header">
        <div className="logo">язык<span>On</span></div>
        <div className="room-code">Комната: {roomId}</div>
        <a href="/">Выйти</a>
      </header>

      <section className="meeting-frame">
        <JitsiMeeting
          domain={import.meta.env.VITE_JITSI_DOMAIN || "meet.jit.si"}
          roomName={`yazykOn-${roomId}`}
          lang="ru"
          userInfo={{ displayName: "Участник" }}
          configOverwrite={{
            prejoinPageEnabled: true,
            disableDeepLinking: true
          }}
          interfaceConfigOverwrite={{
            SHOW_JITSI_WATERMARK: false
          }}
          getIFrameRef={(iframe) => {
            iframe.style.height = "100%";
            iframe.style.width = "100%";
            iframe.style.border = "0";
          }}
        />
      </section>
    </main>
  );
}
