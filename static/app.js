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

    if (!chat || !form || !input || !send || !newChatBtn || !convList || !toast || !modelBtn || !modelBtnText || !modelMenu) {
        return;
    }

    let userId = null;
    let currentModel = null;
    let activeThreadId = null;

    // thinking GIF（static/thinking.gif を配置）
    const THINKING_GIF_SRC = "/static/thinking.gif";

    const MODEL_INFO = {
        seisan: { label: "生産モデル 1.17", desc: "現場の知識を、最短で引き出す。/ 現場会議議事録 / 能率管理表 / 品質過去トラ / 停止時間データ / 日報データ / 不良品データ / 変化点データ" },
        hozen: { label: "保全モデル 1.14", desc: "巧の知識をヒントに。 / 現場会議議事録 / TMSS予防保全・突発事後・調査解析" },
        sefety: { label: "安全・健康モデル 1.12", desc: "トミ鍛安全内規 / 鋳鍛設備課内規" },
        ems: { label: "環境/EMSモデル 1.13", desc: "環境EMS / 潤滑油使用量 / CN会議" },
        genka: { label: "原価・経営モデル 1.13", desc: "伝発注意事項 / 特調運用マニュアル / 経営会議資料" },
        jinji: { label: "人事制度モデル 1.03", desc: "コロナ発生時の対応_第33版 / 60歳以降の再雇用制度 退職手続きマニュアル / 期間従業員運用マニュアル" },
        iatf: { label: "IATFモデル 1.15", desc: "IATF文書 / 現場会議議事録 / 品質過去トラ / 歯車基礎テキスト / 各種IATF基礎知識" },
        security: { label: "情報セキュリティーモデル 1.02", desc: "鋳鍛造部 端末管理・セキュリティー管理 ルール / '25_社給スマートフォン更新手順" },
    };

    const modelLabel = (k) => (MODEL_INFO[k]?.label || k || "モデル");
    const modelDesc = (k) => (MODEL_INFO[k]?.desc || "");

    // ----------------------------
    // Toast
    // ----------------------------
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

    function nowStampForFile() {
        const s = new Date().toISOString().slice(0, 19);
        return s.replaceAll("-", "").replace("T", "_").replaceAll(":", "");
    }

    function sanitizeFilePart(s) {
        return String(s || "")
            .replace(/[\\\/:\*\?"<>\|]/g, "_")
            .replace(/[\u0000-\u001f]/g, "")
            .trim();
    }

    function threadDisplayName(it) {
        return (it?.name || "").trim() || (it?.preview || "").trim() || "chat";
    }

    // ----------------------------
    // Scroll: keep bottom visible
    // ----------------------------
    let stickToBottom = true;

    function scrollToBottom(force = false) {
        if (force || stickToBottom) {
            chat.scrollTop = chat.scrollHeight;
        }
    }

    chat.addEventListener("scroll", () => {
        const nearBottom = (chat.scrollHeight - (chat.scrollTop + chat.clientHeight)) < 40;
        stickToBottom = nearBottom;
    });

    // ----------------------------
    // Optional UI: model-status bar (auto create)
    // ----------------------------
    let currentModelPill = document.getElementById("currentModelPill");
    let currentModelDescEl = document.getElementById("currentModelDesc");

    function ensureModelStatusBar() {
        if (currentModelPill && currentModelDescEl) return;

        const composer = document.querySelector(".composer");
        if (!composer) return;

        const wrap = document.createElement("div");
        wrap.className = "model-status";
        wrap.innerHTML = `
      <div class="model-status-left">現在のモデル</div>
      <div class="model-status-right">
        <span id="currentModelPill" class="model-pill">読み込み中…</span>
        <span id="currentModelDesc" class="model-desc"></span>
      </div>
    `;
        composer.parentNode.insertBefore(wrap, composer);

        currentModelPill = document.getElementById("currentModelPill");
        currentModelDescEl = document.getElementById("currentModelDesc");
    }

    function updateModelUI() {
        modelBtnText.textContent = modelLabel(currentModel);
        ensureModelStatusBar();
        if (currentModelPill) currentModelPill.textContent = modelLabel(currentModel);
        if (currentModelDescEl) currentModelDescEl.textContent = modelDesc(currentModel);
    }

    // ----------------------------
    // Optional UI: notice modal (auto create)
    // ----------------------------
    let noticeModal = document.getElementById("noticeModal");
    let noticeBody = document.getElementById("noticeBody");
    let noticeOkBtn = document.getElementById("noticeOkBtn");

    function ensureNoticeModal() {
        if (noticeModal && noticeBody && noticeOkBtn) return;

        const modal = document.createElement("div");
        modal.id = "noticeModal";
        modal.className = "modal";
        modal.hidden = true;
        modal.innerHTML = `
      <div class="modal-backdrop"></div>
      <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="noticeTitle">
        <div class="modal-title" id="noticeTitle">更新履歴 / 注意事項</div>
        <div id="noticeBody" class="modal-body"></div>
        <div class="modal-actions">
          <button id="noticeOkBtn" type="button" class="modal-ok">上記を了解した！</button>
        </div>
      </div>
    `;
        document.body.appendChild(modal);

        noticeModal = document.getElementById("noticeModal");
        noticeBody = document.getElementById("noticeBody");
        noticeOkBtn = document.getElementById("noticeOkBtn");
    }

    function showNoticeModal(text) {
        ensureNoticeModal();
        if (!noticeModal || !noticeBody || !noticeOkBtn) return;
        noticeBody.textContent = text || "";
        noticeModal.hidden = false;
        document.body.style.overflow = "hidden";
    }

    function hideNoticeModal() {
        if (!noticeModal) return;
        noticeModal.hidden = true;
        document.body.style.overflow = "";
    }

    // ----------------------------
    // localStorage thread per user
    // ----------------------------
    const activeThreadKey = () => `activeThread:${userId || "anon"}`;
    const setActiveThread = (tid) => {
        activeThreadId = (tid || "").trim() || null;
        localStorage.setItem(activeThreadKey(), activeThreadId || "");
    };
    const loadActiveThread = () => (localStorage.getItem(activeThreadKey()) || "").trim() || null;

    function newThreadId() {
        if (window.crypto?.randomUUID) return crypto.randomUUID().replaceAll("-", "");
        return (Date.now().toString(16) + Math.random().toString(16).slice(2) + Math.random().toString(16).slice(2)).slice(0, 32);
    }

    // ----------------------------
    // API fetch (401 -> login)
    // ----------------------------
    async function apiFetch(url, opts) {
        const res = await fetch(url, opts);

        if (res.status === 401) {
            location.href = "/login";
            throw new Error("unauthorized");
        }

        const ct = (res.headers.get("content-type") || "").toLowerCase();
        if (ct.includes("text/html")) {
            location.href = "/login";
            throw new Error("not json");
        }
        return res;
    }

    // ----------------------------
    // Notice fetch (毎回表示)
    // ----------------------------
    async function showNoticeEveryTime() {
        try {
            const res = await apiFetch("/api/notice");
            const data = await res.json().catch(() => ({}));
            if (!res.ok) return;

            const content = String(data.content || "");
            showNoticeModal(content);

            ensureNoticeModal();
            noticeOkBtn.onclick = () => hideNoticeModal();
        } catch {
            // ignore
        }
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
    // chat UI (normal messages)
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
        scrollToBottom(true);
        return { body, bubble, row };
    }

    function renderEmptyChat() {
        chat.innerHTML = "";
        addMsg({
            role: "bot",
            text: "ぼくはChuっとGPTです💋 学習データの中から回答します。質問前に「現在のモデル」を確認してね！",
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
        updateModelUI();
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
        updateModelUI();
        showToast(`現在：${modelLabel(currentModel)}`);

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
                showTime: role === "bot",
            });
        }

        if (items.length === 0) renderEmptyChat();
        scrollToBottom(true);
    }

    // ----------------------------
    // flash highlight for export
    // ----------------------------
    function flashThread(threadId) {
        const el = convList.querySelector(`.conv-item[data-thread-id="${CSS.escape(threadId)}"]`);
        if (!el) return;
        el.classList.add("flash");
        clearTimeout(el._flashT);
        el._flashT = setTimeout(() => el.classList.remove("flash"), 650);
    }

    // ----------------------------
    // CSV export (thread)
    // ----------------------------
    async function exportThreadCsv(threadId, threadName) {
        flashThread(threadId);

        const url = new URL("/api/export", location.origin);
        url.searchParams.set("thread_id", threadId);

        const res = await apiFetch(url.toString());
        if (!res.ok) {
            const t = await res.text();
            throw new Error(t);
        }

        const blob = await res.blob();
        const a = document.createElement("a");
        const objUrl = URL.createObjectURL(blob);

        const safeTitle = sanitizeFilePart(threadName).slice(0, 60) || "chat";
        const fname = `ChuっとGPT_${userId}_${safeTitle}_${nowStampForFile()}.csv`;

        a.download = fname;
        a.href = objUrl;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(() => URL.revokeObjectURL(objUrl), 1500);
    }

    // ----------------------------
    // sidebar list & menu
    // ----------------------------
    function closeAnyMenu() {
        const m = document.querySelector(".menu-pop");
        if (m) m.remove();
    }

    function openMenuFor(anchorEl, it) {
        closeAnyMenu();
        const pop = document.createElement("div");
        pop.className = "menu-pop";

        const btnExport = document.createElement("button");
        btnExport.type = "button";
        btnExport.textContent = "会話を保存";

        const btnRename = document.createElement("button");
        btnRename.type = "button";
        btnRename.textContent = "名前を変更する";

        const btnDelete = document.createElement("button");
        btnDelete.type = "button";
        btnDelete.textContent = "削除する";
        btnDelete.className = "danger";

        btnExport.addEventListener("click", async () => {
            closeAnyMenu();
            try {
                await exportThreadCsv(it.thread_id, threadDisplayName(it));
                showToast("CSVを保存しました");
            } catch {
                showToast("保存に失敗しました");
            }
        });

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

        pop.appendChild(btnExport);
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
            row.dataset.threadId = it.thread_id;
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
            more.textContent = "…";

            left.addEventListener("click", async () => {
                setActiveThread(it.thread_id);
                await loadHistory();
                await loadThreads();
                scrollToBottom(true);
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
    // New Chat
    // ----------------------------
    newChatBtn.addEventListener("click", async () => {
        setActiveThread(newThreadId());
        renderEmptyChat();
        showToast("新しいチャットを開始しました");
        await loadThreads();
        input.focus();
        scrollToBottom(true);
    });

    // ----------------------------
    // Composer placeholder: "ちゅっと考え中・・・"
    // ----------------------------
    const defaultPlaceholder = input.getAttribute("placeholder") || "メッセージを入力…";

    function lockComposerThinking() {
        send.disabled = true;
        input.disabled = true;
        input.value = "";
        input.setAttribute("placeholder", "ちゅっと考え中・・・");
    }

    function unlockComposer() {
        send.disabled = false;
        input.disabled = false;
        input.setAttribute("placeholder", defaultPlaceholder);
        input.focus();
    }

    // ----------------------------
    // SSE parsing
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

    // ----------------------------
    // Thinking row (GIF only, no bubble, no model tag)
    // ----------------------------
    function addThinkingGifOnlyRow() {
        const row = document.createElement("div");
        row.className = "msg bot gif-only";

        const img = document.createElement("img");
        img.className = "thinking-gif";
        img.src = THINKING_GIF_SRC;
        img.alt = "thinking";

        row.appendChild(img);
        chat.appendChild(row);

        // 追加直後もスクロール
        scrollToBottom(true);
        requestAnimationFrame(() => scrollToBottom(true));
        setTimeout(() => scrollToBottom(true), 0);

        // ★ここが重要：画像の高さ確定後に必ずもう一度スクロール
        const forceScrollAfterLoad = () => {
            scrollToBottom(true);
            requestAnimationFrame(() => scrollToBottom(true));
            setTimeout(() => scrollToBottom(true), 0);
        };

        // decode() が使えるブラウザは decode 完了後が最も確実
        if (img.decode) {
            img.decode().then(forceScrollAfterLoad).catch(() => {
                // decode失敗時はonloadにフォールバック
            });
        }

        img.addEventListener("load", forceScrollAfterLoad, { once: true });
        img.addEventListener("error", () => {
            // 画像が無い/壊れててもUIが止まらないように
            forceScrollAfterLoad();
        }, { once: true });

        return row;
    }

    async function streamChat(message) {
        if (!activeThreadId) setActiveThread(newThreadId());
        stickToBottom = true;

        // User msg
        addMsg({ role: "user", text: message, modelKey: "", timeISO: "", showModelTag: false, showTime: false });

        // Thinking GIF only (NO bubble)
        const thinkingRow = addThinkingGifOnlyRow();

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

                if (eventName === "meta") {
                    if (ev.thread_id) setActiveThread(ev.thread_id);
                    continue;
                }

                if (eventName === "delta") {
                    if (!cleared) {
                        thinkingRow.remove(); // ★GIF行を消す
                        full = "";
                        cleared = true;
                    }

                    // 返答の吹き出し（通常表示：モデルタグは付ける）
                    // 初回delta時に作る
                    if (cleared && full === "") {
                        // ここではまだbubbleが無いので1回だけ作る
                    }

                    full += (ev.text || "");

                    // まだ返答bubbleを作ってないなら作成
                    // すでに最後のbot bubbleがあるならそこを更新
                    let lastBotBubbleText = chat.querySelector(".msg.bot:last-child .bubble-text");
                    let lastBotMsg = chat.querySelector(".msg.bot:last-child");

                    // lastがgif-onlyだったら（念のため）作り直し
                    if (!lastBotMsg || lastBotMsg.classList.contains("gif-only")) {
                        const created = addMsg({
                            role: "bot",
                            text: full,
                            modelKey: currentModel,
                            timeISO: "",
                            showModelTag: true,
                            showTime: false
                        });
                        lastBotBubbleText = created.body;
                    } else {
                        // 既存のbot bubble-textに追記反映
                        if (!lastBotBubbleText) {
                            const created = addMsg({
                                role: "bot",
                                text: full,
                                modelKey: currentModel,
                                timeISO: "",
                                showModelTag: true,
                                showTime: false
                            });
                            lastBotBubbleText = created.body;
                        } else {
                            lastBotBubbleText.textContent = full;
                        }
                    }

                    scrollToBottom();

                } else if (eventName === "replace") {
                    if (!cleared) {
                        thinkingRow.remove();
                        cleared = true;
                    }
                    full = ev.text || "";

                    // bot bubbleを必ず用意して置換
                    let lastBotBubbleText = chat.querySelector(".msg.bot:last-child .bubble-text");
                    let lastBotMsg = chat.querySelector(".msg.bot:last-child");

                    if (!lastBotMsg || lastBotMsg.classList.contains("gif-only")) {
                        const created = addMsg({
                            role: "bot",
                            text: full,
                            modelKey: currentModel,
                            timeISO: "",
                            showModelTag: true,
                            showTime: false
                        });
                        lastBotBubbleText = created.body;
                    } else if (lastBotBubbleText) {
                        lastBotBubbleText.textContent = full;
                    } else {
                        const created = addMsg({
                            role: "bot",
                            text: full,
                            modelKey: currentModel,
                            timeISO: "",
                            showModelTag: true,
                            showTime: false
                        });
                        lastBotBubbleText = created.body;
                    }

                    scrollToBottom();

                } else if (eventName === "done") {
                    gotDone = true;
                    if (ev.thread_id) setActiveThread(ev.thread_id);

                    await loadThreads();
                    await loadHistory();
                    scrollToBottom(true);
                    return;

                } else if (eventName === "error") {
                    throw new Error(stringifyErrPayload(ev));
                }
            }
        }

        if (!gotDone) {
            await loadThreads();
            if (activeThreadId) {
                await loadHistory();
                scrollToBottom(true);
            }
        }
    }

    // ----------------------------
    // submit
    // ----------------------------
    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        const message = input.value.trim();
        if (!message) return;

        lockComposerThinking();

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
            scrollToBottom(true);
        } finally {
            unlockComposer();
        }
    });

    // ----------------------------
    // init
    // ----------------------------
    (async () => {
        try {
            await loadModels();
            await showNoticeEveryTime();

            activeThreadId = loadActiveThread();
            await loadThreads();
            await loadHistory();
            if (!activeThreadId) renderEmptyChat();
            input.focus();
            scrollToBottom(true);
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
            scrollToBottom(true);
        }
    })();
})();