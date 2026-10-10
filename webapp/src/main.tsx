import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { api } from "./api";
import App from "./App";
import "./index.css";
import { mockApi } from "./mocks/api";
import { offerHomeScreen, telegram } from "./telegram";

const tg = telegram();
tg?.ready();
tg?.expand();
offerHomeScreen(tg);

// npm run dev — режим mock: страница с тестовыми данными без бота
const client = import.meta.env.MODE === "mock" ? mockApi : api;
const root = document.getElementById("root");
if (root) {
  createRoot(root).render(
    <StrictMode>
      <App api={client} />
    </StrictMode>,
  );
}
