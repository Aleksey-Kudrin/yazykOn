import React from "react";
import { createRoot } from "react-dom/client";
import { createRoom } from "./api";
import "./styles.css";

function App() {
  const [status, setStatus] = React.useState("Проверяем сервер…");
  const [room, setRoom] = React.useState<{ id: string; name: string } | null>(null);
  const [name, setName] = React.useState("Моя конференция");
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    fetch((import.meta.env.VITE_API_URL ?? "http://localhost:3000") + "/api/health")
      .then((response) => response.json())
      .then((data) => setStatus(data.ok ? "Сервер работает" : "Ошибка сервера"))
      .catch(() => setStatus("Сервер недоступен"));
  }, []);

  async function handleCreateRoom() {
    setLoading(true);
    try {
      const created = await createRoom(name);
      setRoom(created);
    } catch {
      setStatus("Не удалось создать конференцию");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="app">
      <section className="card">
        <div className="logo">язык<span>On</span></div>
        <h1>Видеоконференции</h1>
        <p>Создайте комнату и пригласите участников по ссылке.</p>

        <div className="status">
          <span className={status === "Сервер работает" ? "dot online" : "dot"} />
          {status}
        </div>

        <input
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="Название конференции"
          maxLength={100}
        />

        <button disabled={loading} onClick={handleCreateRoom}>
          {loading ? "Создание…" : "Создать конференцию"}
        </button>

        {room && (
          <div className="room">
            <strong>{room.name}</strong>
            <div>Код комнаты: <code>{room.id}</code></div>
            <a href={`/room/${room.id}`}>Открыть комнату →</a>
          </div>
        )}
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
