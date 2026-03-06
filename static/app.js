(() => {
    const chat = document.getElementById("chat");
    const form = document.getElementById("form");
    const input = document.getElementById("input");
    const send = document.getElementById("send");
    const cancelBtn = document.getElementById("cancel");
    const newChatBtn = document.getElementById("newChatBtn");
    const convList = document.getElementById("convList");
    const toast = document.getElementById("toast");
    const modelBtn = document.getElementById("modelBtn");
    const modelBtnText = document.getElementById("modelBtnText");
    const modelMenu = document.getElementById("modelMenu");

    const menuToggle = document.getElementById("menuToggle");
    const sidebarOverlay = document.getElementById("sidebarOverlay");
    const sidebar = document.querySelector(".sidebar");

    if (!chat || !form || !input || !send || !cancelBtn || !newChatBtn || !convList || !toast || !modelBtn || !modelBtnText || !modelMenu) {
        return;
    }

    let userId = null;
    let currentModel = null;
    let activeThreadId = null;

    const THINKING_GIF_SRC = "/static/thinking.gif";

    const MODEL_INFO = {
        seisan: { label: "生産モデル 1.07", desc: "現場の知識を、最短で引き出す。/ 現場会議議事録 / 能率管理表 / 品質過去トラ / 停止時間データ / 日報データ / 不良品データ / 変化点データ" },
        hozen: { label: "保全モデル 1.04", desc: "巧の知識をヒントに。 / 現場会議議事録 / TMSS予防保全・突発事後・調査解析" },
        sefety: { label: "安全・健康モデル 1.02", desc: "トミ鍛安全内規 / 鋳鍛設備課内規" },
        ems: { label: "環境/EMSモデル 1.02", desc: "環境EMS / 潤滑油使用量" },
        genka: { label: "原価・経営モデル 1.03", desc: "伝発注意事項 / 特調運用マニュアル / 特調運用マニュアル_BCP" },
        jinji: { label: "人事制度モデル 1.03", desc: "コロナ発生時の対応_第33版 / 60歳以降の再雇用制度 退職手続きマニュアル / 期間従業員運用マニュアル" },
        iatf: { label: "IATFモデル 1.04", desc: "IATF文書 / 現場会議議事録 / 品質過去トラ / 歯車基礎テキスト" },
        security: { label: "情報セキュリティーモデル 1.02", desc: "鋳鍛造部 端末管理・セキュリティー管理 ルール / '25_社給スマートフォン更新手順" },
        miyoshi_try: { label: "三好工場トライモデル 1.00", desc: "三好工場・明知工場製造技術部工場支援室 実証トライ" },
    };

    const modelLabel = (k) => (MODEL_INFO[k]?.label || k || "モデル");
    const modelDesc = (k) => (MODEL_INFO[k]?.desc || "");

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

    function isAscii1ByteLike(ch) {
        const cp = ch.codePointAt(0);
        return cp <= 0x7f;
    }

    function truncateByUnits(str, maxUnits) {
        const s = String(str || "");
        let units = 0;
        let out = "";
        for (let i = 0; i < s.length; i++) {
            const ch = s[i];
            const u = isAscii1ByteLike(ch) ? 1 : 2;
            if (units + u > maxUnits) return out + "…";
            out += ch;
            units += u;
        }
        return out;
    }

    function threadPreviewText(it) {
        const raw = (it?.name || "").trim() || (it?.preview || "").trim() || "（プレビューなし）";
        return truncateByUnits(raw, 28);
    }

    let stickToBottom = true;

    function scrollToBottom(force = false) {
        if (force || stickToBottom) chat.scrollTop = chat.scrollHeight;
    }

    chat.addEventListener("scroll", () => {
        const nearBottom = (chat.scrollHeight - (chat.scrollTop + chat.clientHeight)) < 40;
        stickToBottom = nearBottom;
    });

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
        }
    }

    function closeModelMenu() {
        modelMenu.hidden = true;
        modelBtn.classList.remove("open");
        modelBtn.setAttribute("aria-expanded", "false");
    }
    function openModelMenu() {
        modelMenu.hidden = false;
        modelBtn.classList.add("open");
        modelBtn.setAttribute("aria-expanded", "true");
    }
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

    async function sendFeedback({ kind, modelKey, threadId, question, answer, botTs }) {
        const res = await apiFetch("/api/feedback", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                kind,
                model_key: modelKey,
                thread_id: threadId,
                question,
                answer,
                bot_ts: botTs,
            })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "feedback error");
        return data;
    }

    async function loadFeedbackStateMap({ threadId, modelKey }) {
        if (!threadId) return new Map();
        try {
            const url = new URL("/api/feedback/state", location.origin);
            url.searchParams.set("thread_id", threadId);
            if (modelKey) url.searchParams.set("model_key", modelKey);

            const res = await apiFetch(url.toString());
            const data = await res.json().catch(() => ({}));
            if (!res.ok) return new Map();

            const m = new Map();
            for (const it of (data.items || [])) {
                const bt = String(it.bot_ts || "").trim();
                const kd = String(it.kind || "").trim().toLowerCase();
                if (!bt) continue;
                if (kd !== "good" && kd !== "bad") continue;
                m.set(bt, kd);
            }
            return m;
        } catch {
            return new Map();
        }
    }

    function attachFeedbackUI({ bubble, modelKey, threadId, question, answer, botTs, initialKind }) {
        const bar = document.createElement("div");
        bar.className = "feedback-bar";

        const up = document.createElement("button");
        up.type = "button";
        up.className = "feedback-btn";
        up.textContent = "👍";

        const down = document.createElement("button");
        down.type = "button";
        down.className = "feedback-btn";
        down.textContent = "👎";

        let state = (initialKind === "good" || initialKind === "bad") ? initialKind : "none";

        const render = () => {
            up.classList.toggle("picked", state === "good");
            down.classList.toggle("picked", state === "bad");
        };

        const commit = async (next) => {
            up.disabled = true;
            down.disabled = true;
            try {
                await sendFeedback({
                    kind: next,
                    modelKey,
                    threadId,
                    question,
                    answer,
                    botTs
                });
                state = next;
                render();
                if (next === "good") showToast("👍 を記録しました");
                else if (next === "bad") showToast("👎 を記録しました");
                else showToast("評価を取り消しました");
            } catch {
                showToast("記録に失敗しました");
            } finally {
                up.disabled = false;
                down.disabled = false;
            }
        };

        up.addEventListener("click", async () => {
            const next = (state === "good") ? "none" : "good";
            await commit(next);
        });

        down.addEventListener("click", async () => {
            const next = (state === "bad") ? "none" : "bad";
            await commit(next);
        });

        render();
        bar.appendChild(up);
        bar.appendChild(down);
        bubble.appendChild(bar);
    }

    function addMsg({ role, text, modelKey, timeISO, showModelTag, showTime, feedback }) {
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

        let ts = null;
        if (showTime) {
            ts = document.createElement("div");
            ts.className = "msg-time";
            ts.textContent = fmtDateTime(timeISO || "");
            bubble.appendChild(ts);
        }

        if (role === "bot" && feedback && feedback.threadId && feedback.question && feedback.answer && feedback.botTs) {
            attachFeedbackUI({
                bubble,
                modelKey: feedback.modelKey || modelKey || currentModel,
                threadId: feedback.threadId,
                question: feedback.question,
                answer: feedback.answer,
                botTs: feedback.botTs,
                initialKind: feedback.initialKind || "none"
            });
        }

        row.appendChild(bubble);
        chat.appendChild(row);
        scrollToBottom(true);
        return { body, bubble, row, tsEl: ts };
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
            feedback: null
        });
    }

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

        const feedbackMap = await loadFeedbackStateMap({ threadId: activeThreadId, modelKey: currentModel });

        const url = new URL("/api/history", location.origin);
        url.searchParams.set("thread_id", activeThreadId);

        const res = await apiFetch(url.toString());
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "history error");

        const items = data.items || [];
        chat.innerHTML = "";

        let lastUserText = "";

        for (const m of items) {
            const role = m.role === "user" ? "user" : "bot";
            if (role === "user") {
                lastUserText = m.content || "";
                addMsg({
                    role,
                    text: m.content,
                    modelKey: m.model_key,
                    timeISO: m.created_at,
                    showModelTag: false,
                    showTime: false,
                    feedback: null
                });
                continue;
            }

            const botText = m.content || "";
            const qText = lastUserText || "";
            const botTs = m.created_at || "";
            const initialKind = feedbackMap.get(botTs) || "none";

            addMsg({
                role: "bot",
                text: botText,
                modelKey: m.model_key,
                timeISO: botTs,
                showModelTag: true,
                showTime: true,
                feedback: {
                    threadId: m.thread_id,
                    modelKey: m.model_key,
                    question: qText,
                    answer: botText,
                    botTs: botTs,
                    initialKind: initialKind
                }
            });
        }

        if (items.length === 0) renderEmptyChat();
        scrollToBottom(true);
    }

    function flashThread(threadId) {
        const el = convList.querySelector(`.conv-item[data-thread-id="${CSS.escape(threadId)}"]`);
        if (!el) return;
        el.classList.add("flash");
        clearTimeout(el._flashT);
        el._flashT = setTimeout(() => el.classList.remove("flash"), 650);
    }

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
            closeSidebarIfMobile();
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
            preview.textContent = threadPreviewText(it);

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
                closeSidebarIfMobile();
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

    newChatBtn.addEventListener("click", async () => {
        setActiveThread(newThreadId());
        renderEmptyChat();
        showToast("新しいチャットを開始しました");
        await loadThreads();
        input.focus();
        scrollToBottom(true);
        closeSidebarIfMobile();
    });

    const defaultPlaceholder = input.getAttribute("placeholder") || "メッセージを入力… (Shift+Enterで改行)";

    let currentAbortController = null;

    function lockComposerThinking() {
        send.disabled = true;
        input.disabled = true;
        cancelBtn.hidden = false;
        input.value = "";
        input.setAttribute("placeholder", "ちゅっと考え中・・・");
        resizeInputToContent();
    }

    function unlockComposer() {
        send.disabled = false;
        input.disabled = false;
        cancelBtn.hidden = true;
        currentAbortController = null;
        input.setAttribute("placeholder", defaultPlaceholder);
        input.focus();
        resizeInputToContent();
    }

    cancelBtn.addEventListener("click", () => {
        if (currentAbortController) {
            currentAbortController.abort();
            showToast("取消しました");
        }
    });

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

    function addThinkingGifOnlyRow() {
        const row = document.createElement("div");
        row.className = "msg bot gif-only";

        const img = document.createElement("img");
        img.className = "thinking-gif";
        img.src = THINKING_GIF_SRC;
        img.alt = "thinking";

        row.appendChild(img);
        chat.appendChild(row);

        scrollToBottom(true);
        requestAnimationFrame(() => scrollToBottom(true));
        setTimeout(() => scrollToBottom(true), 0);

        const forceScrollAfterLoad = () => {
            scrollToBottom(true);
            requestAnimationFrame(() => scrollToBottom(true));
            setTimeout(() => scrollToBottom(true), 0);
        };

        if (img.decode) {
            img.decode().then(forceScrollAfterLoad).catch(() => { });
        }

        img.addEventListener("load", forceScrollAfterLoad, { once: true });
        img.addEventListener("error", () => forceScrollAfterLoad(), { once: true });

        return row;
    }

    // --------- P1-4: done後にloadHistoryしないための状態 ---------
    let streamingBot = null; // { row, bubble, body, tsEl, question, modelKey, threadId, answerAcc }

    function ensureStreamingBotBubble(questionText, modelKey, threadId) {
        if (streamingBot && streamingBot.body) return streamingBot;
        const created = addMsg({
            role: "bot",
            text: "",
            modelKey: modelKey || currentModel,
            timeISO: "",
            showModelTag: true,
            showTime: false,
            feedback: null
        });
        streamingBot = {
            row: created.row,
            bubble: created.bubble,
            body: created.body,
            tsEl: created.tsEl,
            question: questionText || "",
            modelKey: modelKey || currentModel,
            threadId: threadId || activeThreadId,
            answerAcc: ""
        };
        return streamingBot;
    }

    function finalizeStreamingBot({ botTs, answer, modelKey, threadId, question }) {
        if (!streamingBot || !streamingBot.body) return;

        streamingBot.body.textContent = answer || streamingBot.answerAcc || "";

        const timeEl = document.createElement("div");
        timeEl.className = "msg-time";
        timeEl.textContent = fmtDateTime(botTs || "");
        streamingBot.bubble.appendChild(timeEl);

        attachFeedbackUI({
            bubble: streamingBot.bubble,
            modelKey: modelKey || streamingBot.modelKey || currentModel,
            threadId: threadId || streamingBot.threadId || activeThreadId,
            question: question || streamingBot.question || "",
            answer: answer || streamingBot.answerAcc || "",
            botTs: botTs || "",
            initialKind: "none"
        });

        streamingBot = null;
    }

    async function streamChat(message) {
        if (!activeThreadId) setActiveThread(newThreadId());
        stickToBottom = true;

        addMsg({ role: "user", text: message, modelKey: "", timeISO: "", showModelTag: false, showTime: false, feedback: null });

        const thinkingRow = addThinkingGifOnlyRow();
        let res = null;

        try {
            currentAbortController = new AbortController();

            res = await apiFetch("/api/chat/stream", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ message, thread_id: activeThreadId }),
                signal: currentAbortController.signal
            });

            if (!res.ok) {
                const t = await res.text();
                throw new Error(t);
            }

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
                            thinkingRow.remove();
                            cleared = true;
                            ensureStreamingBotBubble(message, currentModel, activeThreadId);
                        }

                        const delta = (ev.text || "");
                        streamingBot.answerAcc += delta;
                        streamingBot.body.textContent = streamingBot.answerAcc;

                        scrollToBottom();

                    } else if (eventName === "replace") {
                        if (!cleared) {
                            thinkingRow.remove();
                            cleared = true;
                            ensureStreamingBotBubble(message, currentModel, activeThreadId);
                        }

                        streamingBot.answerAcc = (ev.text || "");
                        streamingBot.body.textContent = streamingBot.answerAcc;

                        scrollToBottom();

                    } else if (eventName === "done") {
                        gotDone = true;
                        if (ev.thread_id) setActiveThread(ev.thread_id);

                        // P1-4: ここでloadHistoryしない
                        finalizeStreamingBot({
                            botTs: ev.ts || "",
                            answer: ev.answer || (streamingBot ? streamingBot.answerAcc : ""),
                            modelKey: ev.model || currentModel,
                            threadId: ev.thread_id || activeThreadId,
                            question: message
                        });

                        // threadsだけ更新（一覧更新）
                        await loadThreads();
                        scrollToBottom(true);
                        return;

                    } else if (eventName === "error") {
                        throw new Error(stringifyErrPayload(ev));
                    }
                }
            }

            if (!gotDone) {
                await loadThreads();
                if (!cleared) {
                    try { thinkingRow.remove(); } catch { }
                }
                streamingBot = null;
            }
        } catch (err) {
            if (err && (err.name === "AbortError" || String(err).includes("AbortError"))) {
                try { thinkingRow.remove(); } catch { }
                streamingBot = null;
                return;
            }

            try { thinkingRow.remove(); } catch { }
            streamingBot = null;

            addMsg({
                role: "bot",
                text: "エラー: " + (err?.message || String(err)),
                modelKey: currentModel,
                timeISO: new Date().toISOString().slice(0, 19),
                showModelTag: true,
                showTime: true,
                feedback: null
            });
            scrollToBottom(true);
        }
    }

    function getMaxInputPx() {
        const cs = window.getComputedStyle(input);
        const lineH = parseFloat(cs.lineHeight) || 20;
        const padTop = parseFloat(cs.paddingTop) || 0;
        const padBottom = parseFloat(cs.paddingBottom) || 0;
        const borderTop = parseFloat(cs.borderTopWidth) || 0;
        const borderBottom = parseFloat(cs.borderBottomWidth) || 0;
        return (lineH * 5) + padTop + padBottom + borderTop + borderBottom;
    }

    function resizeInputToContent() {
        input.style.height = "auto";
        const maxPx = getMaxInputPx();
        const next = Math.min(input.scrollHeight, maxPx);
        input.style.height = `${next}px`;
        input.style.overflowY = (input.scrollHeight > maxPx) ? "auto" : "hidden";
    }

    input.addEventListener("input", () => resizeInputToContent());

    input.addEventListener("keydown", (e) => {
        if (e.key !== "Enter") return;
        if (e.shiftKey) return;
        e.preventDefault();
        if (send.disabled) return;
        if (typeof form.requestSubmit === "function") form.requestSubmit();
        else form.dispatchEvent(new Event("submit", { cancelable: true, bubbles: true }));
    });

    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        const message = input.value.trim();
        if (!message) return;

        lockComposerThinking();

        try {
            await streamChat(message);
            input.value = "";
            resizeInputToContent();
        } finally {
            unlockComposer();
        }
    });

    function setSidebarOpen(open) {
        document.body.classList.toggle("sidebar-open", !!open);
        if (menuToggle) menuToggle.setAttribute("aria-expanded", open ? "true" : "false");
        if (sidebarOverlay) sidebarOverlay.hidden = !open;
    }

    function isMobile() {
        return window.matchMedia && window.matchMedia("(max-width: 768px)").matches;
    }

    function closeSidebarIfMobile() {
        if (isMobile()) setSidebarOpen(false);
    }

    if (menuToggle && sidebar && sidebarOverlay) {
        menuToggle.addEventListener("click", () => {
            const open = !document.body.classList.contains("sidebar-open");
            setSidebarOpen(open);
        });

        sidebarOverlay.addEventListener("click", () => setSidebarOpen(false));
    }

    window.addEventListener("resize", () => {
        if (!isMobile()) setSidebarOpen(false);
    });

    (async () => {
        try {
            await loadModels();
            await showNoticeEveryTime();

            activeThreadId = loadActiveThread();
            await loadThreads();
            await loadHistory();
            if (!activeThreadId) renderEmptyChat();
            input.focus();
            resizeInputToContent();
            scrollToBottom(true);
        } catch (e) {
            chat.innerHTML = "";
            addMsg({
                role: "bot",
                text: "初期化エラー: " + (e?.message || String(e)),
                modelKey: currentModel,
                timeISO: "",
                showModelTag: true,
                showTime: false,
                feedback: null
            });
            scrollToBottom(true);
        }
    })();
})();