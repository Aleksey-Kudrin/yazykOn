import React from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

function App() {
  const [status, setStatus] = React.useState("Проверяем сервер…");

  React.useEffect(() => {
    fetch("http://localhost:3000/api/health")
      .then((response) => response.json())
      .then((data) => setStatus(data.ok ? "Сервер работает" : "Ошибка сервера"))
      .catch(() => setStatus("Сервер недоступен"));
  }, []);

  return (
    <main className="app">
      <section className="card">
        <div className="logo">язык<span>On</span></div>
        <h1>Видеоконференции нового поколения</h1>
        <p>Первый рабочий каркас языкOn.</p>
        <div className="status">
          <span className={status === "Сервер работает" ? "dot online" : "dot"} />
          {status}
        </div>
        <button onClick={() => window.location.reload()}>Проверить снова</button>
      </section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
