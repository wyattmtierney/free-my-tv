package io.freemytv;

import android.content.Context;
import android.content.SharedPreferences;
import android.os.Handler;
import android.os.Looper;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Live and next-game summaries for the teams on this TV's sports rail. */
final class SportsScores {
    interface Listener { void onScores(); }

    static final class Game {
        final String key, opponent, score, state, detail;
        final long start;

        Game(String key, String opponent, String score, String state, String detail, long start) {
            this.key = key;
            this.opponent = opponent;
            this.score = score;
            this.state = state;
            this.detail = detail;
            this.start = start;
        }

        String summary() {
            if (state.equals("LIVE")) return key + " " + score + " vs " + opponent + " (" + detail + ")";
            if (state.equals("FINAL")) return key + " " + score + " vs " + opponent + " (Final)";
            if (state.equals("UP NEXT")) return key + " vs " + opponent + " (" + detail + ")";
            return key + ": " + detail;
        }

        JSONObject json() {
            JSONObject o = new JSONObject();
            try {
                o.put("key", key); o.put("opponent", opponent); o.put("score", score);
                o.put("state", state); o.put("detail", detail); o.put("start", start);
            } catch (Exception ignored) {}
            return o;
        }
    }

    private static final String[] KEYS = {"Whitecaps", "BC Lions", "Raptors", "Canucks"};
    private static final long REFRESH_MS = 60_000L, RETRY_MS = 120_000L;
    private static final String CFL_URL = "https://cflscoreboard.cfl.ca/json/scoreboard/rounds.json";
    private static SportsScores instance;

    static synchronized SportsScores get(Context context) {
        if (instance == null) instance = new SportsScores(context.getApplicationContext());
        return instance;
    }

    volatile List<Game> games = Collections.emptyList();
    volatile long updatedAt;
    private final SharedPreferences cache;
    // One worker per feed; a slow league endpoint must not hold up the other three teams.
    private final ExecutorService io = Executors.newFixedThreadPool(5);
    private final Handler main = new Handler(Looper.getMainLooper());
    private final List<Listener> listeners = new ArrayList<>();
    private volatile boolean fetching;
    private volatile long failedAt;

    private SportsScores(Context context) {
        // v3 discards the first draft's unparseable schedule cache, which could leave every team at "No game".
        cache = context.getSharedPreferences("sports-scores-v3", Context.MODE_PRIVATE);
        updatedAt = cache.getLong("updatedAt", 0);
        try {
            JSONArray a = new JSONArray(cache.getString("games", "[]"));
            List<Game> loaded = new ArrayList<>();
            for (int i = 0; i < a.length(); i++) {
                JSONObject o = a.getJSONObject(i);
                loaded.add(new Game(o.optString("key"), o.optString("opponent"), o.optString("score"),
                        o.optString("state"), o.optString("detail"), o.optLong("start")));
            }
            if (!loaded.isEmpty()) games = loaded;
        } catch (Exception ignored) {}
    }

    synchronized void addListener(Listener listener) { if (!listeners.contains(listener)) listeners.add(listener); }
    synchronized void removeListener(Listener listener) { listeners.remove(listener); }

    void refreshIfStale() {
        long now = System.currentTimeMillis();
        if (fetching || now - updatedAt < REFRESH_MS || now - failedAt < RETRY_MS) return;
        fetching = true;
        io.execute(() -> {
            try {
                int seasonEndYear = seasonEndYear();
                Future<Game> whitecaps = io.submit(() -> fetchSafely("Whitecaps",
                        () -> fetchEspn("Whitecaps", "soccer", "usa.1", "9727", currentYear())));
                Future<Game> lions = io.submit(() -> fetchSafely("BC Lions", SportsScores::fetchCfl));
                Future<Game> raptors = io.submit(() -> fetchSafely("Raptors",
                        () -> fetchEspn("Raptors", "basketball", "nba", "28", seasonEndYear)));
                Future<Game> canucks = io.submit(() -> fetchSafely("Canucks",
                        () -> fetchEspn("Canucks", "hockey", "nhl", "22", seasonEndYear)));
                List<Game> next = new ArrayList<>();
                next.add(await(whitecaps, "Whitecaps"));
                next.add(await(lions, "BC Lions"));
                next.add(await(raptors, "Raptors"));
                next.add(await(canucks, "Canucks"));
                boolean hasError = false;
                for (Game game : next) if (game.state.equals("UNAVAILABLE")) hasError = true;
                games = next;
                updatedAt = System.currentTimeMillis();
                if (hasError) failedAt = updatedAt; else failedAt = 0;
                JSONArray a = new JSONArray();
                for (Game game : next) a.put(game.json());
                cache.edit().putString("games", a.toString()).putLong("updatedAt", updatedAt).apply();
                main.post(this::notifyListeners);
            } finally {
                fetching = false;
            }
        });
    }

    private interface GameFetch { Game get() throws Exception; }

    private static Game await(Future<Game> future, String key) {
        try {
            return future.get(12, TimeUnit.SECONDS);
        } catch (Exception e) {
            future.cancel(true);
            AppLog.w("scores " + key + " timed out: " + e.getClass().getSimpleName());
            return new Game(key, "", "—", "UNAVAILABLE", "Score feed timed out", 0);
        }
    }

    private static Game fetchSafely(String key, GameFetch fetch) {
        try {
            return fetch.get();
        } catch (Exception e) {
            AppLog.w("scores " + key + " unavailable: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return new Game(key, "", "—", "UNAVAILABLE", "Score feed unavailable", 0);
        }
    }

    private Game previous(String key) {
        for (Game g : games) if (g.key.equals(key)) return g;
        return null;
    }

    Game game(String key) {
        for (Game g : games) if (g.key.equalsIgnoreCase(key)) return g;
        return null;
    }

    String summary() {
        if (games.isEmpty()) return "Team scoreboard is loading";
        StringBuilder out = new StringBuilder("Team scores. ");
        for (int i = 0; i < games.size(); i++) {
            if (i > 0) out.append(". ");
            out.append(games.get(i).summary());
        }
        return out.toString();
    }

    private synchronized void notifyListeners() {
        for (Listener listener : new ArrayList<>(listeners)) listener.onScores();
    }

    private static Game fetchEspn(String key, String sport, String league, String teamId, int season) throws Exception {
        String url = "https://site.api.espn.com/apis/site/v2/sports/" + sport + "/" + league
                + "/teams/" + teamId + "/schedule?season=" + season;
        JSONObject root = new JSONObject(get(url));
        JSONArray events = root.optJSONArray("events");
        if (events == null || events.length() == 0) throw new Exception("empty team schedule");
        long now = System.currentTimeMillis();
        Game live = null, upcoming = null, finalGame = null;
        for (int i = 0; i < events.length(); i++) {
            JSONObject event = events.optJSONObject(i);
            if (event == null) continue;
            long start = parseDate(event.optString("date"));
            JSONObject competition = event.optJSONArray("competitions") == null ? null : event.optJSONArray("competitions").optJSONObject(0);
            if (competition == null) continue;
            JSONArray competitors = competition.optJSONArray("competitors");
            if (competitors == null) continue;
            JSONObject own = null, other = null;
            for (int j = 0; j < competitors.length(); j++) {
                JSONObject competitor = competitors.optJSONObject(j);
                JSONObject team = competitor == null ? null : competitor.optJSONObject("team");
                if (team == null) continue;
                if (team.optString("id").equals(teamId)) own = competitor;
                else other = competitor;
            }
            if (own == null || other == null) continue;
            JSONObject status = competition.optJSONObject("status");
            JSONObject type = status == null ? null : status.optJSONObject("type");
            String state = type == null ? "" : type.optString("state");
            String shortDetail = type == null ? "" : type.optString("shortDetail");
            String ownScore = scoreText(own.opt("score")), otherScore = scoreText(other.opt("score"));
            String score = ownScore.isEmpty() || otherScore.isEmpty() ? "—" : ownScore + " – " + otherScore;
            String opponent = other.optJSONObject("team").optString("displayName", "Opponent");
            if (state.equals("in")) {
                live = new Game(key, opponent, score, "LIVE", shortDetail.isEmpty() ? "In progress" : shortDetail, start);
                break;
            } else if (state.equals("pre") && start > now && (upcoming == null || start < upcoming.start)) {
                upcoming = new Game(key, opponent, "—", "UP NEXT", formatStart(start), start);
            } else if (state.equals("post") && start > 0 && start <= now && (finalGame == null || start > finalGame.start)) {
                finalGame = new Game(key, opponent, score, "FINAL", shortDetail.isEmpty() ? "Final" : shortDetail, start);
            }
        }
        return live != null ? live : upcoming != null ? upcoming : finalGame != null ? finalGame
                : new Game(key, "", "—", "NO GAME", "No game scheduled", 0);
    }

    private static Game fetchCfl() throws Exception {
        JSONArray rounds = new JSONArray(get(CFL_URL));
        long now = System.currentTimeMillis();
        Game live = null, upcoming = null, finalGame = null;
        for (int i = 0; i < rounds.length(); i++) {
            JSONObject round = rounds.optJSONObject(i);
            JSONArray tournaments = round == null ? null : round.optJSONArray("tournaments");
            if (tournaments == null) continue;
            for (int j = 0; j < tournaments.length(); j++) {
                JSONObject game = tournaments.optJSONObject(j);
                if (game == null) continue;
                JSONObject home = game.optJSONObject("homeSquad"), away = game.optJSONObject("awaySquad");
                if (home == null || away == null) continue;
                boolean isHome = home.optInt("id", -1) == 93775;
                if (!isHome && away.optInt("id", -1) != 93775) continue;
                JSONObject own = isHome ? home : away, other = isHome ? away : home;
                long start = parseDate(game.optString("date"));
                String status = game.optString("status").toLowerCase(Locale.ROOT);
                String ownScore = scoreText(own.opt("score")), otherScore = scoreText(other.opt("score"));
                String score = ownScore.isEmpty() || otherScore.isEmpty() ? "—" : ownScore + " – " + otherScore;
                String opponent = other.optString("name", "Opponent");
                Object period = game.opt("activePeriod"), clock = game.opt("clock");
                boolean completed = status.contains("complete") || status.equals("final");
                boolean inProgress = !completed && ((period != null && period != JSONObject.NULL)
                        || (clock != null && clock != JSONObject.NULL) || status.contains("progress") || status.equals("live"));
                if (inProgress) {
                    String detail = "LIVE";
                    if (period != null && period != JSONObject.NULL) detail = "Q" + String.valueOf(period);
                    if (clock instanceof String && !((String) clock).isEmpty()) detail += " · " + clock;
                    live = new Game("BC Lions", opponent, score, "LIVE", detail, start);
                    break;
                } else if (start > now && (upcoming == null || start < upcoming.start)) {
                    upcoming = new Game("BC Lions", opponent, "—", "UP NEXT", formatStart(start), start);
                } else if (completed && start > 0 && start <= now
                        && (finalGame == null || start > finalGame.start)) {
                    finalGame = new Game("BC Lions", opponent, score, "FINAL", "Final", start);
                }
            }
            if (live != null) break;
        }
        return live != null ? live : upcoming != null ? upcoming : finalGame != null ? finalGame
                : new Game("BC Lions", "", "—", "NO GAME", "No game scheduled", 0);
    }

    private static String scoreText(Object value) {
        if (value == null || value == JSONObject.NULL) return "";
        if (value instanceof JSONObject) return ((JSONObject) value).optString("displayValue", "");
        return String.valueOf(value);
    }

    private static long parseDate(String value) {
        try { return Instant.parse(value).toEpochMilli(); }
        catch (Exception ignored) {
            // ESPN often emits minute-precision ISO timestamps such as 2026-10-24T02:00Z.
            try { return java.time.OffsetDateTime.parse(value).toInstant().toEpochMilli(); }
            catch (Exception alsoIgnored) { return 0; }
        }
    }

    private static String formatStart(long at) {
        Calendar now = Calendar.getInstance();
        Calendar when = Calendar.getInstance();
        when.setTimeInMillis(at);
        String day = "";
        if (now.get(Calendar.YEAR) == when.get(Calendar.YEAR) && now.get(Calendar.DAY_OF_YEAR) == when.get(Calendar.DAY_OF_YEAR)) day = "TODAY · ";
        else {
            Calendar tomorrow = (Calendar) now.clone();
            tomorrow.add(Calendar.DAY_OF_YEAR, 1);
            if (tomorrow.get(Calendar.YEAR) == when.get(Calendar.YEAR) && tomorrow.get(Calendar.DAY_OF_YEAR) == when.get(Calendar.DAY_OF_YEAR)) day = "TOMORROW · ";
            else day = new SimpleDateFormat("EEE · ", Locale.US).format(new Date(at)).toUpperCase(Locale.ROOT);
        }
        return day + new SimpleDateFormat("h:mm a", Locale.US).format(new Date(at));
    }

    private static int currentYear() { return Calendar.getInstance().get(Calendar.YEAR); }

    /** NBA and NHL season IDs use the ending year; the new season begins in the summer/autumn. */
    private static int seasonEndYear() {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        int year = c.get(Calendar.YEAR);
        return c.get(Calendar.MONTH) >= Calendar.JULY ? year + 1 : year;
    }

    private static String get(String url) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setConnectTimeout(7000);
        c.setReadTimeout(7000);
        c.setRequestProperty("User-Agent", "FreeMyTV/1.0 (Android TV)");
        c.setRequestProperty("Accept", "application/json");
        c.setRequestProperty("Accept-Encoding", "gzip");
        try {
            int code = c.getResponseCode();
            if (code != 200) throw new Exception("HTTP " + code + " from " + c.getURL().getHost());
            InputStream raw = c.getInputStream();
            if ("gzip".equalsIgnoreCase(c.getContentEncoding())) raw = new java.util.zip.GZIPInputStream(raw);
            try (InputStream in = raw; ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[8192];
                int n;
                while ((n = in.read(buffer)) > 0) out.write(buffer, 0, n);
                return out.toString(StandardCharsets.UTF_8.name());
            }
        } finally { c.disconnect(); }
    }
}
