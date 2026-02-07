(() => {
    // ----------------------------
    // DOM guard: index.html only
    // ----------------------------
    const chat = document.getElementById("chat");
    const form = document.getElementById("form");
    const input = document.getElementById("input");
    const send = document.getElementById("send");
    const newChatBtn = document.getElementById("newChatBtn");
    const convList = document.getElementById("convList");
    const toast = document.getElementById("toast");
    const modelBtn = document.getElementById("modelBtn");
    const modelBtnText = document.getElementById("modelBtnText");
    const modelMenu = document.getElementById("modelMenu");

    if (
        !chat || !form || !input || !send ||
        !newChatBtn || !convList || !toast ||
        !modelBtn || !modelBtnText || !modelMenu
    ) {
        return;
    }

    let userId = null;
    let currentModel = null;
    let activeThreadId = null;

    const MODEL_INFO = {
        seisan: { label: "生産モデル 1.04", desc: "現場の知識を、最短で引き出す。/ 現場会議議事録 / 能率管理表 / 品質過去トラ / 停止時間データ" },
        hozen: { label: "保全モデル 1.04", desc: "巧の知識をヒントに。 / 現場会議議事録 / TMSS予防保全・突発事後・調査解析" },
        ems: { label: "環境/EMSモデル 1.02", desc: "環境EMS / 潤滑油使用量" },
        genka: { label: "原価・経営モデル 1.01", desc: "伝発注意事項" },
        jinji: { label: "人事制度モデル 1.03", desc: "コロナ発生時の対応_第33版 / 60歳以降の再雇用制度 退職手続きマニュアル / 期間従業員運用マニュアル" },
        iatf: { label: "IATFモデル 1.04", desc: "IATF文書 / 現場会議議事録 / 品質過去トラ / 歯車基礎テキスト" },
        security: { label: "情報セキュリティーモデル 1.02", desc: "鋳鍛造部 端末管理・セキュリティー管理 ルール / '25_社給スマートフォン更新手順" },
    };

    const modelLabel = (k) => (MODEL_INFO[k]?.label || k || "モデル");

    function showToast(text) {
        toast.textContent = text;
        toast.classList.add("show");
        clearTimeout(showToast._t);
        showToast._t = setTimeout(() => toast.classList.remove("show"), 1800);
    }

    function fmtDateTime(iso) {
        if (!iso) return "";
        if (iso.includes("T")) {
            const [d, t] = iso.split("T");
            return `${d.replaceAll("-", "/")} ${t}`;
        }
        return iso;
    }

    // localStorage thread per user
    const activeThreadKey = () => `activeThread:${userId || "anon"}`;
    const setActiveThread = (tid) => {
        activeThreadId = (tid || "").trim() || null;
        localStorage.setItem(activeThreadKey(), activeThreadId || "");
    };
    const loadActiveThread = () => (localStorage.getItem(activeThreadKey()) || "").trim() || null;

    // ★追加：送信前に必ず thread_id を確定させる（meta/done取りこぼし保険）
    function newThreadId() {
        if (window.crypto?.randomUUID) return crypto.randomUUID().replaceAll("-", "");
        return (Date.now().toString(16) + Math.random().toString(16).slice(2) + Math.random().toString(16).slice(2)).slice(0, 32);
    }

    // ----------------------------
    // API fetch (401 -> login)
    // ----------------------------
    async function apiFetch(url, opts) {
        const res = await fetch(url, opts);

        // APIは401で返す設計（app.py側）
        if (res.status === 401) {
            location.href = "/login";
            throw new Error("unauthorized");
        }

        // 念のためHTMLが返ってきたらログインへ
        const ct = (res.headers.get("content-type") || "").toLowerCase();
        if (ct.includes("text/html")) {
            location.href = "/login";
            throw new Error("not json");
        }
        return res;
    }

    // ----------------------------
    // dropdown
    // ----------------------------
    function closeModelMenu() { modelMenu.hidden = true; modelBtn.classList.remove("open"); }
    function openModelMenu() { modelMenu.hidden = false; modelBtn.classList.add("open"); }
    function toggleModelMenu() { modelMenu.hidden ? openModelMenu() : closeModelMenu(); }

    modelBtn.addEventListener("click", (e) => { e.stopPropagation(); toggleModelMenu(); });
    document.addEventListener("click", () => closeModelMenu());

    function buildModelMenu(keys) {
        modelMenu.innerHTML = "";
        for (const key of keys) {
            const item = document.createElement("button");
            item.type = "button";
            item.className = "dropdown-item";

            const top = document.createElement("div");
            top.className = "dropdown-item-title";
            top.textContent = MODEL_INFO[key]?.label || key;

            const desc = document.createElement("div");
            desc.className = "dropdown-item-desc";
            desc.textContent = MODEL_INFO[key]?.desc || "";

            item.appendChild(top);
            item.appendChild(desc);

            item.addEventListener("click", async () => {
                closeModelMenu();
                await setModel(key);
            });

            modelMenu.appendChild(item);
        }
    }

    // ----------------------------
    // chat UI
    // ----------------------------
    function addMsg({ role, text, modelKey, timeISO, showModelTag, showTime }) {
        const row = document.createElement("div");
        row.className = `msg ${role}`;

        const bubble = document.createElement("div");
        bubble.className = "bubble";

        if (showModelTag) {
            const tag = document.createElement("span");
            tag.className = "model-tag";
            tag.textContent = modelLabel(modelKey);
            bubble.appendChild(tag);
        }

        const body = document.createElement("div");
        body.className = "bubble-text";
        body.textContent = text;
        bubble.appendChild(body);

        if (showTime && timeISO) {
            const ts = document.createElement("div");
            ts.className = "msg-time";
            ts.textContent = fmtDateTime(timeISO);
            bubble.appendChild(ts);
        }

        row.appendChild(bubble);
        chat.appendChild(row);
        chat.scrollTop = chat.scrollHeight;
        return { body, bubble, row };
    }

    function renderEmptyChat() {
        chat.innerHTML = "";
        addMsg({
            role: "bot",
            text: "ぼくはChuっとGPTです💋学習データの中からなら何でも回答します。左のモデルの中から最適なモデルを選択して質問してくださいね！",
            modelKey: currentModel,
            timeISO: "",
            showModelTag: true,
            showTime: false,
        });
    }

    // ----------------------------
    // data load
    // ----------------------------
    async function loadModels() {
        const res = await apiFetch("/api/models");
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || "models error");

        userId = data.user_id;
        currentModel = data.current;

        buildModelMenu((data.models || []).map(x => x.key));
        modelBtnText.textContent = modelLabel(currentModel);
        showToast(`現在：${modelLabel(currentModel)}`);
    }

    async function setModel(next) {
        const res = await apiFetch("/api/model", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ model: next })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "model set error");

        currentModel = next;
        modelBtnText.textContent = modelLabel(currentModel);
        showToast(`現在：${modelLabel(currentModel)}`);

        // threadは維持（同一チャット継続）
        await loadThreads();
        await loadHistory();
    }

    async function loadThreads() {
        const res = await apiFetch("/api/threads?limit=100");
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "threads error");
        renderThreadList(data.items || []);
    }

    async function loadHistory() {
        if (!activeThreadId) {
            renderEmptyChat();
            return;
        }
        const url = new URL("/api/history", location.origin);
        url.searchParams.set("thread_id", activeThreadId);

        const res = await apiFetch(url.toString());
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "history error");

        const items = data.items || [];
        chat.innerHTML = "";

        for (const m of items) {
            const role = m.role === "user" ? "user" : "bot";
            addMsg({
                role,
                text: m.content,
                modelKey: m.model_key,
                timeISO: m.created_at,
                showModelTag: role === "bot",
                showTime: role === "bot", // 要件：回答のみ右下に時刻
            });
        }

        if (items.length === 0) {
            renderEmptyChat();
        }
    }

    // ----------------------------
    // sidebar list
    // ----------------------------
    function closeAnyMenu() {
        const m = document.querySelector(".menu-pop");
        if (m) m.remove();
    }

    function openMenuFor(anchorEl, it) {
        closeAnyMenu();
        const pop = document.createElement("div");
        pop.className = "menu-pop";

        const btnRename = document.createElement("button");
        btnRename.type = "button";
        btnRename.textContent = "名前を変更する";

        const btnDelete = document.createElement("button");
        btnDelete.type = "button";
        btnDelete.textContent = "削除する";
        btnDelete.className = "danger";

        btnRename.addEventListener("click", async () => {
            closeAnyMenu();
            const next = prompt("新しい名前", it.name || it.preview || "");
            if (!next) return;

            await apiFetch("/api/threads/rename", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ thread_id: it.thread_id, name: next })
            });

            await loadThreads();
        });

        btnDelete.addEventListener("click", async () => {
            closeAnyMenu();
            if (!confirm("このチャットを削除しますか？（履歴も削除されます）")) return;

            await apiFetch("/api/threads/delete", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ thread_id: it.thread_id })
            });

            if (activeThreadId === it.thread_id) setActiveThread(null);
            await loadThreads();
            await loadHistory();
        });

        pop.appendChild(btnRename);
        pop.appendChild(btnDelete);
        document.body.appendChild(pop);

        const rect = anchorEl.getBoundingClientRect();
        pop.style.left = `${Math.min(rect.right - 180, window.innerWidth - 190)}px`;
        pop.style.top = `${rect.top + 8}px`;

        const onDoc = (ev) => {
            if (!pop.contains(ev.target)) {
                closeAnyMenu();
                document.removeEventListener("click", onDoc, true);
            }
        };
        setTimeout(() => document.addEventListener("click", onDoc, true), 0);
    }

    function renderThreadList(items) {
        convList.innerHTML = "";
        if (!items || items.length === 0) {
            const empty = document.createElement("div");
            empty.className = "conv-empty";
            empty.textContent = "まだチャットがありません";
            convList.appendChild(empty);
            return;
        }

        for (const it of items) {
            const row = document.createElement("div");
            row.className = "conv-item";
            if (activeThreadId && it.thread_id === activeThreadId) row.classList.add("active");

            const left = document.createElement("div");
            left.className = "conv-left";

            const preview = document.createElement("div");
            preview.className = "conv-preview";
            preview.textContent = (it.name || "").trim() || (it.preview || "").trim() || "（プレビューなし）";

            const meta = document.createElement("div");
            meta.className = "conv-meta";
            meta.textContent = fmtDateTime(it.updated_at || "");

            left.appendChild(preview);
            left.appendChild(meta);

            const more = document.createElement("button");
            more.className = "conv-more";
            more.type = "button";
            more.textContent = "・・・";

            left.addEventListener("click", async () => {
                setActiveThread(it.thread_id);
                await loadHistory();
                await loadThreads();
            });

            more.addEventListener("click", (e) => {
                e.stopPropagation();
                openMenuFor(row, it);
            });

            row.appendChild(left);
            row.appendChild(more);
            convList.appendChild(row);
        }
    }

    // ----------------------------
    // New Chat: hard reset
    // ----------------------------
    // ★安定化：新しいチャット押下時点で thread_id を作って確定
    newChatBtn.addEventListener("click", async () => {
        setActiveThread(newThreadId());
        renderEmptyChat();
        showToast("新しいチャットを開始しました");
        await loadThreads();
        input.focus();
    });

    // ----------------------------
    // Streaming (SSE)
    // ----------------------------
    function stringifyErrPayload(ev) {
        if (!ev) return "stream error";
        if (typeof ev.message === "string" && ev.message.trim()) return ev.message;
        try { return JSON.stringify(ev); } catch { return "stream error"; }
    }

    function splitSseBlocks(buffer) {
        const parts = buffer.split(/\r?\n\r?\n/);
        return { blocks: parts.slice(0, -1), rest: parts[parts.length - 1] || "" };
    }

    function parseSseBlock(block) {
        const lines = block.split(/\r?\n/);
        const evLine = lines.find(l => l.startsWith("event:"));
        const dataLines = lines.filter(l => l.startsWith("data:"));
        if (dataLines.length === 0) return null;

        const eventName = evLine ? evLine.replace("event:", "").trim() : "message";
        const dataStr = dataLines.map(l => l.replace("data:", "").trim()).join("\n");

        let ev;
        try { ev = JSON.parse(dataStr); } catch { return null; }
        return { eventName, ev };
    }

    async function streamChat(message) {
        // ★最重要：送信前に thread_id を必ず確定（meta/done取りこぼしでも崩れない）
        if (!activeThreadId) {
            setActiveThread(newThreadId());
        }

        // まずユーザ発言を表示
        addMsg({ role: "user", text: message, modelKey: "", timeISO: "", showModelTag: false, showTime: false });

        // “考え中”を先に出す（ここを書き換えていく）
        const { body: botBody, bubble: botBubble } = addMsg({
            role: "bot",
            text: "ちゅっと考え中・・・",
            modelKey: currentModel,
            timeISO: "",
            showModelTag: true,
            showTime: false
        });

        const res = await apiFetch("/api/chat/stream", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message, thread_id: activeThreadId })
        });

        if (!res.ok) {
            const t = await res.text();
            throw new Error(t);
        }

        let full = "";
        let cleared = false;
        let gotDone = false;

        const reader = res.body.getReader();
        const decoder = new TextDecoder("utf-8");
        let buf = "";

        while (true) {
            const { value, done } = await reader.read();
            if (done) break;

            buf += decoder.decode(value, { stream: true });
            const { blocks, rest } = splitSseBlocks(buf);
            buf = rest;

            for (const block of blocks) {
                const parsed = parseSseBlock(block);
                if (!parsed) continue;

                const { eventName, ev } = parsed;

                // metaが来たら thread_id を上書きして同期（サーバ採番でも整合）
                if (eventName === "meta") {
                    if (ev.thread_id) setActiveThread(ev.thread_id);
                    continue;
                }

                if (eventName === "delta") {
                    if (!cleared) {
                        botBody.textContent = "";
                        full = "";
                        cleared = true;
                    }
                    full += (ev.text || "");
                    botBody.textContent = full;

                } else if (eventName === "replace") {
                    full = ev.text || "";
                    botBody.textContent = full;

                } else if (eventName === "done") {
                    gotDone = true;
                    if (ev.thread_id) setActiveThread(ev.thread_id);
                    await loadThreads();
                    await loadHistory();
                    return;

                } else if (eventName === "error") {
                    throw new Error(stringifyErrPayload(ev));
                }
            }
        }

        // streamがdone無しで閉じた時の保険：リセットせず復元を試みる
        if (!gotDone) {
            await loadThreads();
            if (activeThreadId) {
                await loadHistory();
            } else {
                botBubble.classList.add("warn");
            }
        }
    }

    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        const message = input.value.trim();
        if (!message) return;

        input.value = "";
        send.disabled = true;
        input.disabled = true;

        try {
            await streamChat(message);
        } catch (err) {
            addMsg({
                role: "bot",
                text: "エラー: " + (err?.message || String(err)),
                modelKey: currentModel,
                timeISO: new Date().toISOString().slice(0, 19),
                showModelTag: true,
                showTime: true
            });
        } finally {
            send.disabled = false;
            input.disabled = false;
            input.focus();
        }
    });

    // ----------------------------
    // init
    // ----------------------------
    (async () => {
        try {
            await loadModels();
            activeThreadId = loadActiveThread();
            await loadThreads();
            await loadHistory();
            if (!activeThreadId) renderEmptyChat();
            input.focus();
        } catch (e) {
            chat.innerHTML = "";
            addMsg({
                role: "bot",
                text: "初期化エラー: " + (e?.message || String(e)),
                modelKey: currentModel,
                timeISO: "",
                showModelTag: true,
                showTime: false
            });
        }
    })();
})();
