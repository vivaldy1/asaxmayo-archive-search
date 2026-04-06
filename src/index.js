/**
 * Cloudflare Workers: D1 API Bridge
 *
 * 役割: ブラウザ等のクライアントと D1 データベースの中継。
 * セキュリティ: クライアントから SQL を受け取る代わりに、
 *              アクション名 + パラメータのみを受け取り、
 *              SQL はすべてこのファイル内に閉じ込めている。
 *
 * リクエスト形式: { "action": "アクション名", "params": { ... } }
 */
export default {
  async fetch(request, env) {
    // 1. CORS ヘッダーの設定
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    // 2. プリフライトリクエスト (OPTIONS) への即時応答
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // 3. リクエストボディからアクション名とパラメータを抽出
      const { action, params = {} } = await request.json();
      if (!action) throw new Error("action is required.");

      // 4. アクションを実行して結果を返却
      const results = await handleAction(env.yt_data, action, params);
      return new Response(JSON.stringify(results), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }
  },
};

// ---------------------------------------------------------------------------
// アクションハンドラー
// ---------------------------------------------------------------------------
async function handleAction(db, action, params) {
  switch (action) {

    // -----------------------------------------------------------------------
    // モデレーター/オーナー一覧を取得
    // -----------------------------------------------------------------------
    case "get_mod_users": {
      const { results } = await db.prepare(
        `SELECT DISTINCT role, author
         FROM live_chats
         WHERE role IN ('OWNER', 'MODERATOR')
         ORDER BY CASE role WHEN 'OWNER' THEN 0 ELSE 1 END, author`
      ).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // 字幕を全文検索
    // params: { keywords[], mode, dateFrom, dateTo, order, limit }
    // -----------------------------------------------------------------------
    case "search_subtitles": {
      const { keywords = [], keywordGroups, mode = "AND", dateFrom, dateTo, order = "DESC", limit = 200 } = params;
      const { whereSQL, bindParams } = buildSearchWhere(
        keywords, mode, "s.text", dateFrom, dateTo, keywordGroups
      );
      const safeOrder = order === "ASC" ? "ASC" : "DESC";
      const safeLimit = Math.min(Number(limit) || 200, 1000);
      bindParams.push(safeLimit);

      const { results } = await db.prepare(
        `SELECT v.published_at AS date, s.video_id, v.title,
                s.timestamp, s.seconds, s.text
         FROM subtitles s
         JOIN videos v ON s.video_id = v.id
         ${whereSQL}
         ORDER BY v.published_at ${safeOrder}, s.seconds
         LIMIT ?`
      ).bind(...bindParams).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // コメントを全文検索
    // params: { keywords[], mode, dateFrom, dateTo, order, limit }
    // -----------------------------------------------------------------------
    case "search_comments": {
      const { keywords = [], keywordGroups, mode = "AND", dateFrom, dateTo, order = "DESC", limit = 200 } = params;
      const { whereSQL, bindParams } = buildSearchWhere(
        keywords, mode, "c.comment", dateFrom, dateTo, keywordGroups
      );
      const safeOrder = order === "ASC" ? "ASC" : "DESC";
      const safeLimit = Math.min(Number(limit) || 200, 1000);
      bindParams.push(safeLimit);

      const { results } = await db.prepare(
        `SELECT v.published_at AS date, c.video_id, v.title,
                c.author, c.comment
         FROM comments c
         JOIN videos v ON c.video_id = v.id
         ${whereSQL}
         ORDER BY v.published_at ${safeOrder}
         LIMIT ?`
      ).bind(...bindParams).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // LiveChat を全文検索（ユーザー絞り込みにも対応）
    // params: { keywords[], mode, dateFrom, dateTo, order, limit, userId? }
    // -----------------------------------------------------------------------
    case "search_live_chats": {
      const { keywords = [], keywordGroups, mode = "AND", dateFrom, dateTo, order = "DESC", limit = 200, userId } = params;
      const { whereSQL, bindParams } = buildSearchWhere(
        keywords, mode, "lc.message", dateFrom, dateTo, keywordGroups
      );

      // userId 絞り込みを WHERE 句の末尾に追加
      let finalWhereSQL = whereSQL;
      if (userId) {
        finalWhereSQL = whereSQL
          ? whereSQL + " AND lc.author = ?"
          : "WHERE lc.author = ?";
        bindParams.push(userId);
      }

      const safeOrder = order === "ASC" ? "ASC" : "DESC";
      const safeLimit = Math.min(Number(limit) || 200, 1000);
      bindParams.push(safeLimit);

      const { results } = await db.prepare(
        `SELECT v.published_at AS date, lc.video_id, v.title,
                lc.timestamp, lc.role, lc.author, lc.message, lc.type
         FROM live_chats lc
         JOIN videos v ON lc.video_id = v.id
         ${finalWhereSQL}
         ORDER BY v.published_at ${safeOrder}, lc.timestamp
         LIMIT ?`
      ).bind(...bindParams).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // 全テーブルのレコード件数を一括取得
    // -----------------------------------------------------------------------
    case "count_all": {
      const [videoRes, commentRes, chatRes, subtitleRes] = await Promise.all([
        db.prepare(`SELECT COUNT(*) AS cnt FROM videos WHERE title NOT LIKE '[%]'`).all(),
        db.prepare(`SELECT COUNT(*) AS cnt FROM comments`).all(),
        db.prepare(`SELECT COUNT(*) AS cnt FROM live_chats`).all(),
        db.prepare(`SELECT COUNT(*) AS cnt FROM subtitles`).all(),
      ]);
      return {
        videos:     videoRes.results[0]?.cnt    || 0,
        comments:   commentRes.results[0]?.cnt  || 0,
        live_chats: chatRes.results[0]?.cnt     || 0,
        subtitles:  subtitleRes.results[0]?.cnt || 0,
      };
    }

    // -----------------------------------------------------------------------
    // 動画一覧（各テーブルのカウント付き）
    // -----------------------------------------------------------------------
    case "get_video_list": {
      const { results } = await db.prepare(
        `SELECT v.id, v.title, v.published_at,
                COALESCE(cc.cnt, 0) AS comment_count,
                COALESCE(lc.cnt, 0) AS chat_count,
                COALESCE(sc.cnt, 0) AS subtitle_count
         FROM videos v
         LEFT JOIN (SELECT video_id, COUNT(*) AS cnt FROM comments   GROUP BY video_id) cc ON cc.video_id = v.id
         LEFT JOIN (SELECT video_id, COUNT(*) AS cnt FROM live_chats GROUP BY video_id) lc ON lc.video_id = v.id
         LEFT JOIN (SELECT video_id, COUNT(*) AS cnt FROM subtitles  GROUP BY video_id) sc ON sc.video_id = v.id
         WHERE v.title NOT LIKE '[%]'
         ORDER BY v.published_at DESC`
      ).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // 特定動画の字幕を取得
    // params: { videoId }
    // -----------------------------------------------------------------------
    case "get_video_subtitles": {
      const { videoId } = params;
      if (!videoId) throw new Error("videoId is required.");
      const { results } = await db.prepare(
        `SELECT timestamp, text FROM subtitles WHERE video_id = ? ORDER BY seconds`
      ).bind(videoId).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // 特定動画のコメントを取得
    // params: { videoId }
    // -----------------------------------------------------------------------
    case "get_video_comments": {
      const { videoId } = params;
      if (!videoId) throw new Error("videoId is required.");
      const { results } = await db.prepare(
        `SELECT author, comment AS text FROM comments WHERE video_id = ? ORDER BY rowid`
      ).bind(videoId).all();
      return results;
    }

    // -----------------------------------------------------------------------
    // 特定動画の LiveChat を取得
    // params: { videoId }
    // -----------------------------------------------------------------------
    case "get_video_livechat": {
      const { videoId } = params;
      if (!videoId) throw new Error("videoId is required.");
      const { results } = await db.prepare(
        `SELECT timestamp, author, message AS text, role, type
         FROM live_chats WHERE video_id = ? ORDER BY timestamp`
      ).bind(videoId).all();
      return results;
    }

    default:
      throw new Error(`Unknown action: ${action}`);
  }
}

// ---------------------------------------------------------------------------
// ヘルパー: 検索用 WHERE 句を組み立てる
// keywordGroups: string[][] — 各グループは同一キーワードのかな表記バリアント（内部OR）
//                            グループ同士はmode(AND/OR)で結合
// ---------------------------------------------------------------------------
function buildSearchWhere(keywords, mode, column, dateFrom, dateTo, keywordGroups) {
  const conditions = ["v.title NOT LIKE '[%]'"];
  const bindParams = [];

  if (dateFrom) {
    conditions.push("v.published_at >= ?");
    bindParams.push(dateFrom);
  }
  if (dateTo) {
    conditions.push("v.published_at <= ?");
    bindParams.push(dateTo + " 23:59:59");
  }

  const groups = keywordGroups && keywordGroups.length > 0
    ? keywordGroups
    : keywords.length > 0 ? keywords.map(k => [k]) : [];

  if (groups.length > 0) {
    const outerOp = mode === "OR" ? " OR " : " AND ";
    const groupClauses = groups.map(variants => {
      const varClauses = variants.map(() => `${column} LIKE ?`);
      variants.forEach(v => bindParams.push(`%${v}%`));
      return variants.length === 1 ? varClauses[0] : `(${varClauses.join(" OR ")})`;
    });
    conditions.push(`(${groupClauses.join(outerOp)})`);
  }

  const whereSQL = `WHERE ${conditions.join(" AND ")}`;
  return { whereSQL, bindParams };
}
