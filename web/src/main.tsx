import React from "react";
import { createRoot } from "react-dom/client";
import { Room } from "./Room";
import { createRoom, getMe, login, register } from "./api";
import "./styles.css";

function Home() {
  const [status, setStatus] = React.useState("Проверяем сервер…");
  const [room, setRoom] = React.useState<{ id: string; name: string } | null>(null);
  const [name, setName] = React.useState("Моя конференция");
  const [loading, setLoading] = React.useState(false);
  const [password, setPassword] = React.useState("");
  const [username, setUsername] = React.useState("");
  const [authPassword, setAuthPassword] = React.useState("");
  const [user, setUser] = React.useState<{ id: string; username: string } | null>(null);
  const [authMode, setAuthMode] = React.useState<"login" | "register">("login");
  const [authError, setAuthError] = React.useState("");

  React.useEffect(() => { getMe().then(setUser).catch(() => setUser(null)); }, []);

  React.useEffect(() => {
    fetch((import.meta.env.VITE_API_URL ?? "http://localhost:3000") + "/api/health")
      .then((response) => response.json())
      .then((data) => setStatus(data.ok ? "Сервер работает" : "Ошибка сервера"))
      .catch(() => setStatus("Сервер недоступен"));
  }, []);

  async function handleAuth() {
    setAuthError("");
    try { const next = authMode === "login" ? await login(username, authPassword) : await register(username, authPassword); setUser(next); setAuthPassword(""); }
    catch (error) { setAuthError(error instanceof Error ? error.message : "Ошибка авторизации"); }
  }

  async function handleCreateRoom() {
    if (!user) { setAuthError("Сначала войдите в аккаунт"); return; }
    setLoading(true);
    try {
      const created = await createRoom(name, password);
      setRoom(created);
      window.history.pushState({}, "", `/room/${created.id}`);
      window.dispatchEvent(new PopStateEvent("popstate"));
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

        <input
          type="password"
          value={password}
          onChange={(event) => setPassword(event.target.value)}
          placeholder="Пароль комнаты (необязательно)"
          maxLength={128}
        />

        <button disabled={loading} onClick={handleCreateRoom}>
          {loading ? "Создание…" : "Создать конференцию"}
        </button>

        {room && (
          <div className="room">
            <strong>{room.name}</strong>
            <div>Код комнаты: <code>{room.id}</code>{room.requiresPassword ? " • 🔒 пароль защищает вход" : ""}</div>
          </div>
        )}
      </section>
    </main>
  );
}

function App() {
  const [path, setPath] = React.useState(window.location.pathname);

  React.useEffect(() => {
    const onPopState = () => setPath(window.location.pathname);
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  const match = path.match(/^\/room\/([A-Z0-9]+)$/i);
  return match ? <Room roomId={match[1].toUpperCase()} /> : <Home />;
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
