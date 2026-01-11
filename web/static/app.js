/* global EventSource */

function $(id) {
  return document.getElementById(id);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function clampInt(x) {
  if (x === "" || x === null || x === undefined) return null;
  const n = Number(x);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function formatNumber(n) {
  if (n === null || n === undefined) return "—";
  return new Intl.NumberFormat().format(n);
}

function setCookie(name, value, days) {
  const expires = days ? `; Max-Age=${days * 24 * 60 * 60}` : "";
  document.cookie = `${name}=${encodeURIComponent(value)}${expires}; Path=/; SameSite=Lax`;
}

function getCookie(name) {
  const parts = document.cookie.split(";").map((p) => p.trim());
  for (const p of parts) {
    if (p.startsWith(`${name}=`)) return decodeURIComponent(p.slice(name.length + 1));
  }
  return null;
}

function setJsonCookie(name, obj, days) {
  setCookie(name, JSON.stringify(obj), days);
}

function getJsonCookie(name, fallback) {
  const raw = getCookie(name);
  if (!raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function boxArtUrl(url, w, h) {
  if (!url) return "";
  return url.replace("{width}", String(w)).replace("{height}", String(h));
}

function streamThumbUrl(url, w, h) {
  if (!url) return "";
  return url.replace("{width}", String(w)).replace("{height}", String(h));
}

function twitchChannelUrl(userName) {
  if (!userName) return null;
  // Twitch channel URLs use the login; user_name is usually safe here (no spaces), but encode just in case.
  return `https://www.twitch.tv/${encodeURIComponent(String(userName))}`;
}

function parseCsvParam(v) {
  if (!v) return [];
  return String(v)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function unique(arr) {
  return Array.from(new Set(arr));
}

const ICONS = {
  magnifyingGlass: `
    <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z" />
    </svg>
  `,
  plusCircle: `
    <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v6m3-3H9m12 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
    </svg>
  `,
  checkCircle: `
    <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75 11.25 15 15 9.75m6 2.25a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
    </svg>
  `,
  minusCircle: `
    <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 12H9m12 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z" />
    </svg>
  `,
  noSymbol: `
    <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="m18.364 5.636-12.728 12.728M12 21a9 9 0 1 0-9-9 9 9 0 0 0 9 9ZM6.343 6.343l11.314 11.314" />
    </svg>
  `,
  xMark: `
    <svg class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
      <path stroke-linecap="round" stroke-linejoin="round" d="M6 18 18 6M6 6l12 12" />
    </svg>
  `,
};

const MASONRY_COL_WIDTH = 320; // px (keep in sync with columns-[320px])
const MASONRY_COL_GAP = 16; // px (1rem)

let _masonryBound = false;
let _masonryResizeTimer = null;

function updateMasonryColumns(cardCount) {
  const root = $("followedGames");
  if (!root) return;
  Ratelimit-Reset 
  const count = Math.max(0, Number(cardCount) || 0);
  const w = root.clientWidth || 0;
  const fit = Math.max(1, Math.floor((w + MASONRY_COL_GAP) / (MASONRY_COL_WIDTH + MASONRY_COL_GAP)));
  const cols = Math.max(1, Math.min(fit, Math.max(1, count)));

  root.style.columnCount = String(cols);
}

function clampMasonryToUsedColumns() {
  const root = $("followedGames");
  if (!root) return;

  // Count distinct x-positions of cards; this corresponds to how many columns are actually used.
  const children = Array.from(root.children).filter((el) => el.nodeType === 1);
  if (!children.length) return;

  const lefts = new Set(children.map((el) => el.offsetLeft));
  const used = Math.max(1, lefts.size);
  const current = Number.parseInt(root.style.columnCount || "0", 10) || 0;
  if (current && used < current) {
    root.style.columnCount = String(used);
  }
}

function bindMasonryResize() {
  if (_masonryBound) return;
  _masonryBound = true;

  window.addEventListener("resize", () => {
    if (_masonryResizeTimer) clearTimeout(_masonryResizeTimer);
    _masonryResizeTimer = setTimeout(() => {
      _masonryResizeTimer = null;
      const payload = state.lastStreamsPayload;
      const count = payload && payload.games ? payload.games.length : 0;
      updateMasonryColumns(count);
      requestAnimationFrame(() => clampMasonryToUsedColumns());
    }, 100);
  });
}

function hideAllCardActions(exceptEl) {
  document.querySelectorAll("[data-stream-actions],[data-game-actions]").forEach((el) => {
    if (exceptEl && el === exceptEl) return;
    el.classList.remove("show-actions");
  });
}

document.addEventListener("pointerdown", (e) => {
  const el =
    (e.target && e.target.closest && e.target.closest("[data-stream-actions]")) ||
    (e.target && e.target.closest && e.target.closest("[data-game-actions]")) ||
    null;
  if (!el) hideAllCardActions(null);
});

function normalizeIgnored(raw) {
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const item of raw) {
    if (typeof item === "string" || typeof item === "number") {
      out.push({ id: String(item), name: null });
      continue;
    }
    if (item && typeof item === "object") {
      const id = item.id ?? item.user_id;
      const name = item.name ?? item.user_name ?? null;
      if (id !== undefined && id !== null && String(id).trim()) {
        out.push({ id: String(id), name: name ? String(name) : null });
      }
    }
  }
  const m = new Map();
  for (const u of out) {
    if (!m.has(u.id)) m.set(u.id, { id: u.id, name: u.name });
    else if (!m.get(u.id).name && u.name) m.get(u.id).name = u.name;
  }
  return Array.from(m.values());
}

let _confirmResolve = null;
let _confirmCloseTimer = null;

function _openConfirmModal(title, body) {
  const modal = $("confirmModal");
  const overlay = $("confirmOverlay");
  const panel = $("confirmPanel");
  const titleEl = $("confirmTitle");
  const bodyEl = $("confirmBody");

  if (!modal || !overlay || !panel || !titleEl || !bodyEl) return;

  if (_confirmCloseTimer) {
    clearTimeout(_confirmCloseTimer);
    _confirmCloseTimer = null;
  }

  titleEl.textContent = title || "Confirm";
  bodyEl.textContent = body || "";

  modal.classList.remove("hidden");

  // Start from closed visual state
  overlay.classList.add("opacity-0");
  overlay.classList.remove("opacity-100");
  panel.classList.add("opacity-0", "translate-y-2", "scale-95");
  panel.classList.remove("opacity-100", "translate-y-0", "scale-100");

  // Force layout so the open transition isn't skipped
  void panel.offsetHeight;

  requestAnimationFrame(() => {
    overlay.classList.remove("opacity-0");
    overlay.classList.add("opacity-100");
    panel.classList.remove("opacity-0", "translate-y-2", "scale-95");
    panel.classList.add("opacity-100", "translate-y-0", "scale-100");
  });

  setTimeout(() => $("confirmOkBtn")?.focus(), 50);
}

function _closeConfirmModal(resultBool) {
  const modal = $("confirmModal");
  const overlay = $("confirmOverlay");
  const panel = $("confirmPanel");

  if (!modal || !overlay || !panel) {
    if (_confirmResolve) _confirmResolve(Boolean(resultBool));
    _confirmResolve = null;
    return;
  }

  overlay.classList.remove("opacity-100");
  overlay.classList.add("opacity-0");
  panel.classList.remove("opacity-100", "translate-y-0", "scale-100");
  panel.classList.add("opacity-0", "translate-y-2", "scale-95");

  if (_confirmCloseTimer) clearTimeout(_confirmCloseTimer);
  _confirmCloseTimer = setTimeout(() => {
    modal.classList.add("hidden");
    _confirmCloseTimer = null;
    if (_confirmResolve) _confirmResolve(Boolean(resultBool));
    _confirmResolve = null;
  }, 200);
}

function confirmDialog({ title, message }) {
  if (_confirmResolve) _closeConfirmModal(false);
  return new Promise((resolve) => {
    _confirmResolve = resolve;
    _openConfirmModal(title, message);
  });
}

function ignoredIds() {
  return state.ignoredUsers.map((x) => String(x.id));
}

function upsertIgnoredUser(id, name) {
  const sid = String(id);
  const sname = name ? String(name) : null;
  const existing = state.ignoredUsers.find((x) => String(x.id) === sid);
  if (existing) {
    if (!existing.name && sname) existing.name = sname;
    return;
  }
  state.ignoredUsers.push({ id: sid, name: sname });
}

function syncIgnoredCookie() {
  setJsonCookie("ignored_streamers", state.ignoredUsers, 365);
}

const state = {
  followedGameIds: [],
  ignoredUsers: [],
  filters: {
    verifiedOnly: false,
    minViewers: null,
    maxViewers: null,
    minFollowers: null,
    maxFollowers: null,
  },
  lastStreamsPayload: null,
  es: null,
  refreshInFlight: false,
  refreshQueued: false,
};

function readUrlParams() {
  const u = new URL(window.location.href);
  return {
    games: parseCsvParam(u.searchParams.get("games")),
    ignored: parseCsvParam(u.searchParams.get("ignored")),
  };
}

function boot() {
  const bootData = window.__BOOT__ || {};
  const url = readUrlParams();

  const cookieGames = getJsonCookie("followed_games", []);
  const cookieIgnored = getJsonCookie("ignored_streamers", []);
  const cookieFilters = getJsonCookie("stream_filters", null);

  state.followedGameIds = unique((url.games.length ? url.games : cookieGames).map(String));
  state.ignoredUsers = normalizeIgnored(url.ignored.length ? url.ignored : cookieIgnored);

  // If URL had session data, persist it
  if (url.games.length) setJsonCookie("followed_games", state.followedGameIds, 365);
  if (url.ignored.length) syncIgnoredCookie();

  // Filters: persist in cookies (not shared by default via URL).
  state.filters = normalizeFilters(cookieFilters);

  renderIgnored();
  bindUi();
  bindMasonryResize();
  syncFilterControlsFromState();
  connectSse();
  refreshStreams();

  // Touch tracked games so the backend fetcher knows what to pull.
  touchTrackedGames().catch(() => {});

  if (bootData.warning) {
    console.warn(bootData.warning);
  }
}

function openSettings() {
  const modal = $("settingsModal");
  if (!modal) return;
  const overlay = $("settingsOverlay");
  const panel = $("settingsPanel");

  if (_settingsCloseTimer) {
    clearTimeout(_settingsCloseTimer);
    _settingsCloseTimer = null;
  }

  modal.classList.remove("hidden");
  syncFilterControlsFromState();

  // Ensure we start from the "closed" visual state, then transition to "open".
  overlay?.classList.add("opacity-0");
  overlay?.classList.remove("opacity-100");

  panel?.classList.add("opacity-0", "translate-y-2", "scale-95");
  panel?.classList.remove("opacity-100", "translate-y-0", "scale-100");

  // Force a layout so the browser actually commits the initial state after display:none -> block.
  // This prevents the open animation from being skipped.
  if (panel) void panel.offsetHeight;

  // Trigger transition on the next frame.
  requestAnimationFrame(() => {
    overlay?.classList.remove("opacity-0");
    overlay?.classList.add("opacity-100");

    panel?.classList.remove("opacity-0", "translate-y-2", "scale-95");
    panel?.classList.add("opacity-100", "translate-y-0", "scale-100");
  });

  // Focus after the panel starts animating in.
  setTimeout(() => $("searchInput")?.focus(), 50);
}

let _settingsCloseTimer = null;
function closeSettings() {
  const modal = $("settingsModal");
  if (!modal) return;
  const overlay = $("settingsOverlay");
  const panel = $("settingsPanel");

  overlay?.classList.remove("opacity-100");
  overlay?.classList.add("opacity-0");

  panel?.classList.remove("opacity-100", "translate-y-0", "scale-100");
  panel?.classList.add("opacity-0", "translate-y-2", "scale-95");

  if (_settingsCloseTimer) clearTimeout(_settingsCloseTimer);
  _settingsCloseTimer = setTimeout(() => {
    modal.classList.add("hidden");
    _settingsCloseTimer = null;
  }, 200);
}

function bindUi() {
  $("settingsBtn")?.addEventListener("click", () => openSettings());
  $("closeSettingsBtn")?.addEventListener("click", () => closeSettings());
  $("settingsOverlay")?.addEventListener("click", () => closeSettings());
  $("settingsModal")?.addEventListener("pointerdown", (e) => {
    // Close when clicking/tapping anywhere outside the panel.
    if (e.target && e.target.closest && e.target.closest("#settingsPanel")) return;
    closeSettings();
  });

  $("confirmOkBtn")?.addEventListener("click", () => _closeConfirmModal(true));
  $("confirmCancelBtn")?.addEventListener("click", () => _closeConfirmModal(false));
  $("confirmModal")?.addEventListener("pointerdown", (e) => {
    if (e.target && e.target.closest && e.target.closest("#confirmPanel")) return;
    _closeConfirmModal(false);
  });

  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && _confirmResolve) {
      _closeConfirmModal(false);
      return;
    }
    if (e.key === "Escape") closeSettings();
  });

  $("searchBtn").addEventListener("click", () => doSearch());
  $("searchInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") doSearch();
  });

  bindFilterHandlers();

  // clearFollowedBtn removed (no "clear all followed games" action)

  $("clearIgnoredBtn").addEventListener("click", () => {
    state.ignoredUsers = [];
    syncIgnoredCookie();
    renderIgnored();
    refreshStreams();
  });

  $("shareBtn").addEventListener("click", async () => {
    const url = new URL(window.location.href);
    if (state.followedGameIds.length) url.searchParams.set("games", state.followedGameIds.join(","));
    else url.searchParams.delete("games");

    const ids = ignoredIds();
    if (ids.length) url.searchParams.set("ignored", ids.join(","));
    else url.searchParams.delete("ignored");

    // (Optional) include filters in share URL if user has set them
    url.searchParams.delete("verifiedOnly");
    url.searchParams.delete("minViewers");
    url.searchParams.delete("maxViewers");
    url.searchParams.delete("minFollowers");
    url.searchParams.delete("maxFollowers");

    try {
      await navigator.clipboard.writeText(url.toString());
      const badge = $("shareBtnBadge");
      const btn = $("shareBtn");
      badge?.classList.remove("hidden");
      btn?.classList.add("ring-1", "ring-emerald-500/30");
      await sleep(800);
      badge?.classList.add("hidden");
      btn?.classList.remove("ring-1", "ring-emerald-500/30");
    } catch {
      prompt("Copy this URL:", url.toString());
    }
  });
}

function normalizeFilters(raw) {
  const base = {
    verifiedOnly: false,
    minViewers: null,
    maxViewers: null,
    minFollowers: null,
    maxFollowers: null,
  };
  if (!raw || typeof raw !== "object") return base;
  return {
    verifiedOnly: Boolean(raw.verifiedOnly),
    minViewers: clampInt(raw.minViewers),
    maxViewers: clampInt(raw.maxViewers),
    minFollowers: clampInt(raw.minFollowers),
    maxFollowers: clampInt(raw.maxFollowers),
  };
}

function saveFilters() {
  setJsonCookie("stream_filters", state.filters, 365);
}

function syncFilterControlsFromState() {
  if ($("verifiedOnly")) $("verifiedOnly").checked = Boolean(state.filters.verifiedOnly);
  if ($("minViewers")) $("minViewers").value = state.filters.minViewers ?? "";
  if ($("maxViewers")) $("maxViewers").value = state.filters.maxViewers ?? "";
  if ($("minFollowers")) $("minFollowers").value = state.filters.minFollowers ?? "";
  if ($("maxFollowers")) $("maxFollowers").value = state.filters.maxFollowers ?? "";
}

let _filtersDebounce = null;
function scheduleFilterRefresh() {
  if (_filtersDebounce) clearTimeout(_filtersDebounce);
  _filtersDebounce = setTimeout(() => {
    _filtersDebounce = null;
    refreshStreams();
  }, 250);
}

function bindFilterHandlers() {
  $("verifiedOnly")?.addEventListener("change", () => {
    state.filters.verifiedOnly = $("verifiedOnly").checked;
    saveFilters();
    refreshStreams();
  });

  const bindNumber = (id, key) => {
    $(id)?.addEventListener("input", () => {
      state.filters[key] = clampInt($(id).value);
      saveFilters();
      scheduleFilterRefresh();
    });
  };

  bindNumber("minViewers", "minViewers");
  bindNumber("maxViewers", "maxViewers");
  bindNumber("minFollowers", "minFollowers");
  bindNumber("maxFollowers", "maxFollowers");
}

async function doSearch() {
  const q = $("searchInput").value.trim();
  if (!q) return;
  $("searchBtn").disabled = true;
  $("searchBtn").classList.add("opacity-60");
  try {
    const resp = await fetch(`/api/search_games?q=${encodeURIComponent(q)}`);
    const data = await resp.json();
    renderSearchResults(data.games || []);
  } finally {
    $("searchBtn").disabled = false;
    $("searchBtn").classList.remove("opacity-60");
  }
}

function renderSearchResults(games) {
  const root = $("searchResults");
  root.innerHTML = "";
  if (!games.length) {
    root.innerHTML = `<div class="text-sm text-slate-400">No matches.</div>`;
    return;
  }
  for (const g of games) {
    const followed = state.followedGameIds.includes(String(g.id));
    const div = document.createElement("div");
    div.className = "flex items-center gap-3 rounded-lg border border-slate-800 bg-slate-950/40 p-2";
    const img = boxArtUrl(g.box_art_url, 52, 70);
    div.innerHTML = `
      <img class="h-[70px] w-[52px] rounded-md object-cover" src="${img}" alt="" />
      <div class="min-w-0 flex-1">
        <div class="truncate text-sm font-semibold text-slate-200">${escapeHtml(g.name)}</div>
      </div>
      <button
        class="inline-flex h-10 w-10 items-center justify-center rounded-md ${followed ? "bg-slate-800 text-slate-300" : "bg-indigo-600 text-white hover:bg-indigo-500"}"
        aria-label="${followed ? "Following" : "Follow"} ${escapeHtml(g.name)}"
        title="${followed ? "Following" : "Follow"}"
      >
        ${followed ? ICONS.checkCircle : ICONS.plusCircle}
      </button>
    `;
    const btn = div.querySelector("button");
    btn.addEventListener("click", () => {
      if (state.followedGameIds.includes(String(g.id))) return;
      state.followedGameIds = unique([...state.followedGameIds, String(g.id)]);
      setJsonCookie("followed_games", state.followedGameIds, 365);
      connectSse();
      renderFollowedGames();
      refreshStreams();
      touchTrackedGames().catch(() => {});
      renderSearchResults(games);
    });
    root.appendChild(div);
  }
}

function renderIgnored() {
  const root = $("ignoredList");
  root.innerHTML = "";
  if (!state.ignoredUsers.length) {
    root.innerHTML = `<div class="text-sm text-slate-400">None</div>`;
    return;
  }
  for (let i = 0; i < state.ignoredUsers.length; i++) {
    const u = state.ignoredUsers[i];
    const div = document.createElement("div");
    div.className = "flex items-center justify-between gap-2 rounded-md border border-slate-800 bg-slate-950/40 px-2 py-1.5";
    const label = u.name ? u.name : "Ignored channel (name unknown yet)";
    div.innerHTML = `
      <div class="truncate">${escapeHtml(label)}</div>
      <button
        class="inline-flex h-8 w-8 items-center justify-center rounded-md text-slate-400 hover:bg-slate-800 hover:text-slate-200"
        aria-label="Remove ignored channel"
        title="Remove ignored channel"
      >
        ${ICONS.xMark}
      </button>
    `;
    div.querySelector("button").addEventListener("click", () => {
      state.ignoredUsers = state.ignoredUsers.filter((x) => String(x.id) !== String(u.id));
      syncIgnoredCookie();
      renderIgnored();
      refreshStreams();
    });
    root.appendChild(div);
  }
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function connectSse() {
  if (state.es) {
    try {
      state.es.close();
    } catch {
      // ignore
    }
    state.es = null;
  }

  if (!state.followedGameIds.length) return;

  const url = `/api/sse?game_ids=${encodeURIComponent(state.followedGameIds.join(","))}`;
  const es = new EventSource(url);
  state.es = es;

  es.addEventListener("game_updated", () => {
    // Debounce into one refresh (multiple games can update in a short burst)
    refreshStreams();
  });

  es.addEventListener("error", () => {
    // EventSource will retry automatically, but if it hard-fails, recreate it.
    // (This is a simple safeguard.)
  });
}

async function touchTrackedGames() {
  if (!state.followedGameIds.length) return;
  await fetch("/api/touch_tracked", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ game_ids: state.followedGameIds }),
  });
}

async function refreshStreams() {
  if (state.refreshInFlight) {
    state.refreshQueued = true;
    return;
  }
  state.refreshInFlight = true;
  try {
    await _refreshStreamsOnce();
  } finally {
    state.refreshInFlight = false;
    if (state.refreshQueued) {
      state.refreshQueued = false;
      refreshStreams();
    }
  }
}

function requireStatus() {
  if (state.filters.verifiedOnly) return "verified";
  return "any";
}

async function _refreshStreamsOnce() {
  renderFollowedGamesLoading();
  if (!state.followedGameIds.length) {
    $("followedGames").innerHTML = `<div class="text-sm text-slate-400">Follow a game to see streams.</div>`;
    updateMasonryColumns(1);
    return;
  }

  const params = new URLSearchParams();
  params.set("game_ids", state.followedGameIds.join(","));
  params.set("status", requireStatus());
  if (state.filters.minViewers !== null) params.set("min_viewers", String(state.filters.minViewers));
  if (state.filters.maxViewers !== null) params.set("max_viewers", String(state.filters.maxViewers));
  if (state.filters.minFollowers !== null) params.set("min_followers", String(state.filters.minFollowers));
  if (state.filters.maxFollowers !== null) params.set("max_followers", String(state.filters.maxFollowers));
  const ids = ignoredIds();
  if (ids.length) params.set("ignored", ids.join(","));

  const resp = await fetch(`/api/streams?${params.toString()}`);
  const data = await resp.json();
  state.lastStreamsPayload = data;
  backfillIgnoredNamesFromPayload(data);
  renderFollowedGames();
}

function backfillIgnoredNamesFromPayload(payload) {
  if (!payload || !payload.games || !state.ignoredUsers.length) return;
  let changed = false;
  const m = new Map(state.ignoredUsers.map((u) => [String(u.id), u]));
  for (const g of payload.games) {
    for (const s of g.streams || []) {
      const id = String(s.user_id);
      const u = m.get(id);
      if (u && !u.name && s.user_name) {
        u.name = String(s.user_name);
        changed = true;
      }
    }
  }
  if (changed) {
    state.ignoredUsers = Array.from(m.values());
    syncIgnoredCookie();
    renderIgnored();
  }
}
function renderFollowedGamesLoading() {
  const root = $("followedGames");
  if (!root.childElementCount) {
    root.innerHTML = `<div class="text-sm text-slate-400">Loading…</div>`;
  }
}

function liveChannelsLabel(n) {
  if (n === 0) return "No live channels";
  if (n === 1) return "1 live channel";
  return `${n} live channels`;
}

function badgeHtml(broadcasterType) {
  if (broadcasterType === "partner") {
    return `<span class="rounded bg-purple-600/30 px-2 py-0.5 text-xs text-purple-200 border border-purple-500/30">Partner</span>`;
  }
  if (broadcasterType === "affiliate") {
    return `<span class="rounded bg-sky-600/30 px-2 py-0.5 text-xs text-sky-200 border border-sky-500/30">Affiliate</span>`;
  }
  return `<span class="rounded bg-slate-800 px-2 py-0.5 text-xs text-slate-300 border border-slate-700">Unverified</span>`;
}

function animateEnter(el) {
  el.classList.add("transition-all", "duration-250", "ease-out");
  el.classList.add("opacity-0", "-translate-y-2", "scale-95");
  // Force layout so the initial state is committed before we transition.
  void el.offsetHeight;
  requestAnimationFrame(() => {
    el.classList.remove("opacity-0", "-translate-y-2", "scale-95");
  });
}

function animateExitAndRemove(el) {
  if (el.dataset.exiting === "1") return;
  el.dataset.exiting = "1";
  el.classList.add("transition-all", "duration-250", "ease-in");
  // Ensure the transition class is applied before changing opacity/transform.
  void el.offsetHeight;
  requestAnimationFrame(() => {
    el.classList.add("opacity-0", "-translate-y-2", "scale-95");
  });
  setTimeout(() => el.remove(), 280);
}

function ensureStreamRow(existingRow, stream) {
  const row = existingRow || document.createElement("div");
  if (!existingRow) {
    row.className = "p-3";
    row.dataset.streamId = String(stream.id);

    row.innerHTML = `
      <div class="stream-hover group relative flex gap-3" data-stream-actions="1">
        <button
          class="stream-ignore-btn absolute right-0 top-0 inline-flex h-9 w-9 items-center justify-center rounded-md bg-red-500/20 text-red-200 shadow-lg ring-1 ring-red-500/30 backdrop-blur
                 opacity-0 pointer-events-none transition-opacity duration-150
                 group-hover:opacity-100 group-hover:pointer-events-auto
                 group-focus-within:opacity-100 group-focus-within:pointer-events-auto
                 group-[.show-actions]:opacity-100 group-[.show-actions]:pointer-events-auto
                 hover:bg-red-500/30"
          aria-label="Ignore channel"
          title="Ignore channel"
          type="button"
        >
          ${ICONS.noSymbol}
        </button>

        <a data-stream-link-thumb target="_blank" rel="noreferrer" class="shrink-0">
          <img data-stream-thumb class="h-[63px] w-[112px] rounded-md object-cover border border-slate-800" alt="" />
        </a>
        <div class="min-w-0 flex-1">
          <div class="flex flex-wrap items-center gap-2">
            <a data-stream-link-name target="_blank" rel="noreferrer" class="text-sm font-semibold text-slate-100 truncate hover:underline" title="Open stream"></a>
            <span data-stream-badge></span>
          </div>
          <div class="mt-1 flex flex-wrap items-center gap-3 text-xs text-slate-400">
            <div data-stream-viewers></div>
            <div data-stream-followers></div>
          </div>
          <div data-stream-title class="mt-1 truncate pr-10 text-sm text-slate-200"></div>
        </div>
      </div>
    `;

    const hoverBox = row.querySelector("[data-stream-actions]");
    const ignoreBtn = row.querySelector(".stream-ignore-btn");

    ignoreBtn.addEventListener("pointerdown", (e) => e.stopPropagation());
    ignoreBtn.addEventListener("click", async () => {
      const s = row._streamData;
      if (!s) return;
      const uid = String(s.user_id);
      if (ignoredIds().includes(uid)) return;
      const name = s.user_name || "this channel";
      const ok = await confirmDialog({ title: "Ignore channel", message: `Ignore ${name}?` });
      if (!ok) return;
      upsertIgnoredUser(uid, s.user_name || null);
      syncIgnoredCookie();
      renderIgnored();
      refreshStreams();
    });

    hoverBox.addEventListener("pointerdown", (e) => {
      if (e.pointerType !== "touch") return;
      if (e.target && e.target.closest && e.target.closest(".stream-ignore-btn")) return;
      if (e.target && e.target.closest && e.target.closest("a")) return;
      const isOpen = hoverBox.classList.contains("show-actions");
      hideAllCardActions(isOpen ? null : hoverBox);
      hoverBox.classList.toggle("show-actions", !isOpen);
    });
  }

  // Store latest stream payload for handlers.
  row._streamData = stream;

  // Update content
  const channelUrl = twitchChannelUrl(stream.user_name);
  const thumb = streamThumbUrl(stream.thumbnail_url, 320, 180);

  const linkThumb = row.querySelector("[data-stream-link-thumb]");
  const linkName = row.querySelector("[data-stream-link-name]");
  const img = row.querySelector("[data-stream-thumb]");

  if (channelUrl) {
    linkThumb.setAttribute("href", channelUrl);
    linkName.setAttribute("href", channelUrl);
  } else {
    linkThumb.removeAttribute("href");
    linkName.removeAttribute("href");
  }

  img.setAttribute("src", thumb);
  linkName.textContent = stream.user_name || "Unknown streamer";
  row.querySelector("[data-stream-badge]").innerHTML = badgeHtml(stream.broadcaster_type);
  row.querySelector("[data-stream-viewers]").textContent = `Viewers: ${formatNumber(stream.viewer_count)}`;
  row.querySelector("[data-stream-followers]").textContent = `Followers: ${formatNumber(stream.follower_count)}`;
  row.querySelector("[data-stream-title]").textContent = stream.title || "";

  return row;
}

function ensureGameCard(existingCard, gameObj) {
  const gameId = String(gameObj.game.id);
  const card = existingCard || document.createElement("div");
  const isNew = !existingCard;

  if (!existingCard) {
    card.dataset.gameId = gameId;
    card.className =
      "mb-4 w-full break-inside-avoid relative rounded-xl border border-slate-800 bg-slate-950/30 overflow-hidden";

    const header = document.createElement("div");
    header.className = "group relative flex gap-3 p-3";
    header.setAttribute("data-game-actions", "1");
    header.innerHTML = `
      <button
        class="game-unfollow-btn absolute right-2 top-2 inline-flex h-9 w-9 items-center justify-center rounded-md bg-red-500/15 text-red-200 shadow-lg ring-1 ring-red-500/30
               opacity-0 pointer-events-none transition-opacity duration-150
               group-hover:opacity-100 group-hover:pointer-events-auto
               group-focus-within:opacity-100 group-focus-within:pointer-events-auto
               group-[.show-actions]:opacity-100 group-[.show-actions]:pointer-events-auto
               hover:bg-red-500/25"
        aria-label="Unfollow game"
        title="Unfollow"
        type="button"
      >
        ${ICONS.minusCircle}
      </button>

      <img data-game-art class="h-[96px] w-[72px] rounded-lg object-cover" alt="" />
      <div class="min-w-0 flex-1">
        <div class="flex items-start justify-between gap-2">
          <div class="min-w-0">
            <div data-game-name class="truncate text-base font-semibold text-slate-100"></div>
          </div>
        </div>
        <div data-game-count class="mt-2 text-xs text-slate-400"></div>
      </div>
    `;

    const body = document.createElement("div");
    body.className = "border-t border-slate-800";
    body.innerHTML = `
      <div data-stream-empty class="hidden p-3 text-sm text-slate-400">No matching live channels right now.</div>
      <div data-stream-list class="divide-y divide-slate-800"></div>
    `;

    card.appendChild(header);
    card.appendChild(body);

    const unfollowBtn = header.querySelector(".game-unfollow-btn");
    unfollowBtn.addEventListener("pointerdown", (e) => e.stopPropagation());
    unfollowBtn.addEventListener("click", async () => {
      const name = header.querySelector("[data-game-name]")?.textContent || "this game";
      const ok = await confirmDialog({ title: "Unfollow game", message: `Unfollow ${name}?` });
      if (!ok) return;
      state.followedGameIds = state.followedGameIds.filter((x) => x !== gameId);
      setJsonCookie("followed_games", state.followedGameIds, 365);
      connectSse();
      refreshStreams();
      touchTrackedGames().catch(() => {});
    });

    header.addEventListener("pointerdown", (e) => {
      if (e.pointerType !== "touch") return;
      if (e.target && e.target.closest && e.target.closest(".game-unfollow-btn")) return;
      const isOpen = header.classList.contains("show-actions");
      hideAllCardActions(isOpen ? null : header);
      header.classList.toggle("show-actions", !isOpen);
    });
  }

  // Update header content
  card.querySelector("[data-game-art]").setAttribute("src", boxArtUrl(gameObj.game.box_art_url, 188, 250));
  card.querySelector("[data-game-name]").textContent = gameObj.game.name || gameObj.game.id;
  card.querySelector("[data-game-count]").textContent = liveChannelsLabel((gameObj.streams || []).length);

  if (isNew) animateEnter(card);
  return card;
}

function updateGameStreams(card, streams) {
  const list = card.querySelector("[data-stream-list]");
  const empty = card.querySelector("[data-stream-empty]");
  const arr = streams || [];

  empty.classList.toggle("hidden", arr.length !== 0);

  // Versioning so we can safely schedule a second-pass reconcile after exit animations.
  card._streamsRenderVersion = (card._streamsRenderVersion || 0) + 1;
  const myVersion = card._streamsRenderVersion;

  const existing = new Map();
  list.querySelectorAll("[data-stream-id]").forEach((el) => existing.set(el.dataset.streamId, el));

  const desiredIds = new Set(arr.map((s) => String(s.id)));
  const removals = [];
  for (const [sid, row] of existing.entries()) {
    if (desiredIds.has(sid)) continue;
    if (row.dataset.exiting === "1") continue;
    removals.push(row);
  }

  const seen = new Set();
  for (const s of arr) {
    const sid = String(s.id);
    const prev = existing.get(sid);
    const row = ensureStreamRow(prev || null, s);
    seen.add(sid);
    if (!prev) {
      list.appendChild(row);
      animateEnter(row);
    }
  }

  // Start exit animations first, without reordering the remaining rows. This keeps the removed
  // stream animating "in place" rather than jumping due to reordering.
  for (const row of removals) {
    animateExitAndRemove(row);
  }

  // Second pass: after exit animations, reconcile order + remove any stale rows.
  if (removals.length) {
    setTimeout(() => {
      if (card._streamsRenderVersion !== myVersion) return;

      const latest = streams || [];
      const latestIds = new Set(latest.map((s) => String(s.id)));

      // Remove any non-exiting rows not present anymore.
      list.querySelectorAll("[data-stream-id]").forEach((el) => {
        if (el.dataset.exiting === "1") return;
        if (!latestIds.has(el.dataset.streamId)) el.remove();
      });

      // Rebuild list in desired order (excluding exiting nodes; they should be gone by now).
      const fragment = document.createDocumentFragment();
      for (const s of latest) {
        const sid = String(s.id);
        const prev = list.querySelector(`[data-stream-id="${CSS.escape(sid)}"]`);
        const row = ensureStreamRow(prev || null, s);
        fragment.appendChild(row);
      }
      list.appendChild(fragment);
    }, 320);
  } else {
    // No removals -> safe to reorder immediately.
    const fragment = document.createDocumentFragment();
    for (const s of arr) {
      const sid = String(s.id);
      const prev = existing.get(sid) || list.querySelector(`[data-stream-id="${CSS.escape(sid)}"]`);
      const row = ensureStreamRow(prev || null, s);
      fragment.appendChild(row);
    }
    list.appendChild(fragment);
  }
}

function renderFollowedGames() {
  const root = $("followedGames");
  const payload = state.lastStreamsPayload;
  if (!payload || !payload.games) {
    root.innerHTML = `<div class="text-sm text-slate-400">Loading…</div>`;
    updateMasonryColumns(1);
    return;
  }

  // Remove any placeholder content (e.g. "Loading…") once we have real data.
  Array.from(root.children).forEach((child) => {
    if (!(child instanceof HTMLElement)) return;
    if (!child.dataset.gameId) child.remove();
  });

  const existingCards = new Map();
  root.querySelectorAll("[data-game-id]").forEach((el) => existingCards.set(el.dataset.gameId, el));

  for (const g of payload.games) {
    const gid = String(g.game.id);
    const prev = existingCards.get(gid) || null;
    const card = ensureGameCard(prev, g);
    updateGameStreams(card, g.streams || []);
    root.appendChild(card); // keep order stable
    existingCards.delete(gid);
  }

  for (const card of existingCards.values()) {
    animateExitAndRemove(card);
  }

  // Prevent empty extra columns when there are fewer cards than available columns.
  updateMasonryColumns(payload.games.length);
  // Some browsers balance columns and can leave an empty column; clamp to actual used columns.
  requestAnimationFrame(() => {
    clampMasonryToUsedColumns();
    // One more frame for good measure after the first layout pass.
    requestAnimationFrame(() => clampMasonryToUsedColumns());
  });
}

boot();




